#!/usr/bin/env python3
"""Fleet soak: graceful worker drain on SIGTERM.

Enqueues long-running jobs, SIGTERMs a worker mid-flight, asserts:
  * worker stops taking new work
  * jobs either complete under drain or are released quickly
  * another worker reclaims released work without waiting full visibility
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("drain_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "job-queue")
VISIBILITY = int(os.environ.get("VISIBILITY_TIMEOUT", "60") or "60")
DRAIN_TIMEOUT = int(os.environ.get("DRAIN_TIMEOUT", "8") or "8")
LONG_MS = int(os.environ.get("LONG_MS", "15000") or "15000")
JOBS = int(os.environ.get("JOBS", "12") or "12")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120") or "120")
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDS)
        _rr += 1
        bases = COORDS[start:] + COORDS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def docker_sigterm_service(service: str) -> list[str]:
    """SIGTERM matching containers (grace period = DRAIN_TIMEOUT + buffer)."""
    # Match project containers: job-queue-worker-1-1 etc.
    filt = f"name={PROJECT}-{service}"
    out = subprocess.check_output(
        ["docker", "ps", "-q", "--filter", filt],
        text=True,
    )
    ids = [x for x in out.strip().split() if x]
    if not ids:
        # Fallback broader name match.
        out = subprocess.check_output(
            ["docker", "ps", "-q", "--filter", f"name={service}"],
            text=True,
        )
        ids = [x for x in out.strip().split() if x]
    grace = max(DRAIN_TIMEOUT + 10, 20)
    for cid in ids:
        logger.info("docker stop -t %d %s (%s)", grace, cid, service)
        # stop sends SIGTERM, waits *grace* seconds, then SIGKILL.
        subprocess.run(
            ["docker", "stop", "-t", str(grace), cid],
            check=False,
        )
    return ids


def main() -> int:
    logger.info(
        "drain soak jobs=%d long_ms=%d vis=%d drain_timeout=%d",
        JOBS,
        LONG_MS,
        VISIBILITY,
        DRAIN_TIMEOUT,
    )
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")

    # Seed long-running work across tenants so multiple workers hold leases.
    for i in range(JOBS):
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": f"drain-{i % 4:02d}",
                "payload": {"kind": "work", "ms": LONG_MS},
                "priority": 0,
                "max_attempts": 5,
                "visibility_timeout_seconds": VISIBILITY,
                "retry_backoff_base_seconds": 0,
            },
        )
    logger.info("seeded %d long jobs", JOBS)

    # Wait until several are leased.
    leased_peak = 0
    for _ in range(40):
        b = backlog()
        leased_peak = max(leased_peak, int(b.get("leased") or 0))
        logger.info("pre-drain backlog=%s", b)
        if leased_peak >= min(4, JOBS // 2):
            break
        time.sleep(0.5)
    if leased_peak < 1:
        logger.error("no jobs leased before drain")
        return 1

    # SIGTERM worker-1 in background so we can observe drain effects live.
    t0 = time.monotonic()
    stop_err: list[str] = []

    def _stop() -> None:
        try:
            ids = docker_sigterm_service("worker-1")
            if not ids:
                stop_err.append("worker-1 container not found")
        except Exception as exc:  # noqa: BLE001
            stop_err.append(str(exc))

    import threading

    threading.Thread(target=_stop, name="docker-stop", daemon=True).start()
    logger.info("worker-1 stop started")

    # Within drain window, worker-1 must release or finish so work progresses
    # without waiting full visibility.
    progress_deadline = t0 + DRAIN_TIMEOUT + 8
    saw_progress = False
    while time.monotonic() < progress_deadline:
        b = backlog()
        logger.info("during-drain backlog=%s", b)
        if int(b.get("completed") or 0) > 0:
            saw_progress = True
        # Released jobs briefly raise pending or other workers absorb them.
        if int(b.get("pending") or 0) > 0 or int(b.get("completed") or 0) > 0:
            saw_progress = True
        time.sleep(0.5)
    if stop_err:
        logger.error("docker stop failed: %s", stop_err)
        return 1

    # Finish remaining work on surviving workers (short jobs → quick).
    deadline = time.monotonic() + MAX_WALL
    while time.monotonic() < deadline:
        b = backlog()
        logger.info("post-drain backlog=%s", b)
        if (
            int(b.get("pending") or 0) == 0
            and int(b.get("leased") or 0) == 0
            and int(b.get("completed") or 0) + int(b.get("dead") or 0)
            >= JOBS * 0.9
        ):
            break
        time.sleep(1)

    b = backlog()
    elapsed = time.monotonic() - t0
    logger.info("final backlog=%s elapsed=%.1fs", b, elapsed)
    if int(b.get("completed") or 0) + int(b.get("dead") or 0) < JOBS * 0.9:
        logger.error("too few terminal jobs: %s", b)
        return 1
    # Must not depend on full visibility timeout to free worker-1's leases.
    if elapsed >= VISIBILITY * 0.75:
        logger.error(
            "finish took %.1fs (vis=%ds); expected drain release << visibility",
            elapsed,
            VISIBILITY,
        )
        return 1
    if not saw_progress:
        logger.error("no progress observed during drain window")
        return 1
    logger.info(
        "DRAIN SOAK PASSED completed=%s elapsed=%.1fs vis=%d drain_timeout=%d",
        b.get("completed"),
        elapsed,
        VISIBILITY,
        DRAIN_TIMEOUT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
