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

    # Snapshot leased count right before SIGTERM so we can detect batch release.
    pre_b = backlog()
    leased_before = int(pre_b.get("leased") or 0)
    completed_before = int(pre_b.get("completed") or 0)
    pending_before = int(pre_b.get("pending") or 0)
    logger.info("pre-sigterm backlog=%s", pre_b)

    # SIGTERM two workers so batch leftovers must free quickly for survivors.
    t0 = time.monotonic()
    stop_err: list[str] = []

    def _stop() -> None:
        try:
            for svc in ("worker-1", "worker-2"):
                ids = docker_sigterm_service(svc)
                if not ids:
                    stop_err.append(f"{svc} container not found")
        except Exception as exc:  # noqa: BLE001
            stop_err.append(str(exc))

    import threading

    threading.Thread(target=_stop, name="docker-stop", daemon=True).start()
    logger.info("worker-1/2 stop started (batch drain under load)")

    # Phase A: within a few seconds of SIGTERM, unprocessed batch members from
    # drained workers must free (pending↑ and/or leased↓). Regression: only the
    # active job was released and the rest of the batch stayed LEASED until VT.
    # Phase B: by DRAIN_TIMEOUT+buffer, force-release of active jobs also lands.
    progress_deadline = t0 + DRAIN_TIMEOUT + 8
    saw_progress = False
    saw_quick_batch_release = False
    time_to_batch_release: float | None = None
    time_to_progress: float | None = None
    min_leased = leased_before
    max_pending = pending_before
    while time.monotonic() < progress_deadline:
        b = backlog()
        leased_now = int(b.get("leased") or 0)
        completed_now = int(b.get("completed") or 0)
        pending_now = int(b.get("pending") or 0)
        min_leased = min(min_leased, leased_now)
        max_pending = max(max_pending, pending_now)
        now = time.monotonic()
        logger.info("during-drain backlog=%s", b)
        # Batch leftovers free immediately → pending rises and/or leased drops
        # well before full drain timeout (and far before visibility).
        if not saw_quick_batch_release and (
            pending_now > pending_before or leased_now < leased_before
        ):
            saw_quick_batch_release = True
            time_to_batch_release = now - t0
            logger.info(
                "batch release observed in %.2fs (pending %d→%d leased %d→%d)",
                time_to_batch_release,
                pending_before,
                pending_now,
                leased_before,
                leased_now,
            )
        if completed_now > completed_before or pending_now > pending_before:
            if not saw_progress:
                time_to_progress = now - t0
            saw_progress = True
        time.sleep(0.3)
    if stop_err:
        logger.error("docker stop failed: %s", stop_err)
        return 1
    if leased_before >= 4 and not saw_quick_batch_release:
        logger.error(
            "no batch release after SIGTERM "
            "(leased_before=%d min_leased=%d pending_before=%d max_pending=%d); "
            "queued batch jobs may still be locked until VT",
            leased_before,
            min_leased,
            pending_before,
            max_pending,
        )
        return 1
    if (
        time_to_batch_release is not None
        and time_to_batch_release > DRAIN_TIMEOUT
    ):
        logger.error(
            "batch release took %.1fs > drain_timeout=%ds "
            "(should free non-active batch immediately)",
            time_to_batch_release,
            DRAIN_TIMEOUT,
        )
        return 1

    # Finish remaining work on surviving workers.
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
    logger.info(
        "final backlog=%s elapsed=%.1fs batch_release=%.2fs progress=%.2fs",
        b,
        elapsed,
        time_to_batch_release if time_to_batch_release is not None else -1,
        time_to_progress if time_to_progress is not None else -1,
    )
    if int(b.get("completed") or 0) + int(b.get("dead") or 0) < JOBS * 0.9:
        logger.error("too few terminal jobs: %s", b)
        return 1
    # Reclaim path must not wait full visibility. Total wall may include normal
    # job runtime on survivors; require it still finishes before a full VT so
    # stuck leases would have been impossible to hide.
    if elapsed >= VISIBILITY:
        logger.error(
            "finish took %.1fs >= vis=%ds; drain did not free leases for reclaim",
            elapsed,
            VISIBILITY,
        )
        return 1
    if not saw_progress:
        logger.error("no progress observed during drain window")
        return 1
    logger.info(
        "DRAIN SOAK PASSED completed=%s elapsed=%.1fs "
        "batch_release=%.2fs vis=%d drain_timeout=%d",
        b.get("completed"),
        elapsed,
        time_to_batch_release if time_to_batch_release is not None else -1,
        VISIBILITY,
        DRAIN_TIMEOUT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
