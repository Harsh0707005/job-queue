#!/usr/bin/env python3
"""Fleet soak: failing jobs hop to unused workers, then keep going.

  * Dual coordinators, four registered workers, several tenants.
  * Hop cap is *higher than* the worker count so every worker fails the
    poison job once, then retries must continue (no forever-pending).
  * A worker that just nacked is skipped only while a peer has not yet
    failed that job.
  * Healthy jobs on the same tenants still complete.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("hop_skip_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DEAD_TENANTS = int(os.environ.get("DEAD_TENANTS", "3") or "3")
REL_TENANTS = int(os.environ.get("REL_TENANTS", "3") or "3")
POISON_EACH = int(os.environ.get("POISON_EACH", "4") or "4")
HEALTHY_EACH = int(os.environ.get("HEALTHY_EACH", "8") or "8")
HOPS = int(os.environ.get("MAX_WORKER_HOPS", "6") or "6")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "4") or "4")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                payload = json.loads(raw.decode()) if raw else {}
                return int(resp.status), payload
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            code, body = http_json("GET", "/health")
            if code == 200 and body.get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def main() -> int:
    logger.info(
        "hop-skip soak dead_tenants=%d rel_tenants=%d hops=%d workers=%d",
        DEAD_TENANTS,
        REL_TENANTS,
        HOPS,
        WORKER_COUNT,
    )
    wait_coords()

    dead_ids = [f"hskip-d{i:02d}" for i in range(DEAD_TENANTS)]
    rel_ids = [f"hskip-r{i:02d}" for i in range(REL_TENANTS)]
    for tid in dead_ids:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/quarantine",
            {"max_worker_hops": HOPS, "action": "dead"},
        )
        if code != 200:
            raise RuntimeError(f"set quarantine {tid} {code} {body}")
    for tid in rel_ids:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/quarantine",
            {"max_worker_hops": HOPS, "action": "release"},
        )
        if code != 200:
            raise RuntimeError(f"set quarantine {tid} {code} {body}")

    expect_poison = (DEAD_TENANTS + REL_TENANTS) * POISON_EACH
    expect_healthy = (DEAD_TENANTS + REL_TENANTS) * HEALTHY_EACH
    for tid in dead_ids + rel_ids:
        for i in range(POISON_EACH):
            code, body = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "fail", "ms": 15, "error": "poison"},
                    "max_attempts": 20,
                    "retry_backoff_base_seconds": 0,
                },
            )
            if code != 201:
                raise RuntimeError(f"poison enqueue {tid} {code} {body}")
        for i in range(HEALTHY_EACH):
            code, body = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 10, "i": i},
                },
            )
            if code != 201:
                raise RuntimeError(f"healthy enqueue {tid} {code} {body}")
    logger.info("enqueued poison=%d healthy=%d", expect_poison, expect_healthy)

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        healthy_done = 0
        poison_dead = 0
        poison_parked = 0
        hopping = 0
        same_worker_retry = 0
        for tid in dead_ids + rel_ids:
            for j in list_jobs(tid):
                st = j.get("status")
                hops = list(j.get("failed_workers") or [])
                if st == "completed":
                    healthy_done += 1
                elif st == "dead":
                    poison_dead += 1
                    _assert_terminal_hops(tid, hops, kind="dead")
                elif st == "quarantined":
                    poison_parked += 1
                    _assert_terminal_hops(tid, hops, kind="parked")
                elif st in ("pending", "leased"):
                    hopping += 1
                    # Duplicate hops are only a bug *before* every worker
                    # has had a turn.
                    if hops and len(set(hops)) < WORKER_COUNT and len(hops) != len(
                        set(hops)
                    ):
                        same_worker_retry += 1
        now = time.time()
        if now - last >= 2:
            logger.info(
                "healthy=%d/%d dead=%d parked=%d hopping=%d same_retry=%d",
                healthy_done,
                expect_healthy,
                poison_dead,
                poison_parked,
                hopping,
                same_worker_retry,
            )
            last = now
        if same_worker_retry:
            raise RuntimeError(
                "a failed worker leased the same poison job again "
                f"while peers were live (count={same_worker_retry})"
            )
        if (
            healthy_done == expect_healthy
            and hopping == 0
            and poison_dead + poison_parked == expect_poison
        ):
            want_dead = DEAD_TENANTS * POISON_EACH
            want_park = REL_TENANTS * POISON_EACH
            if poison_dead != want_dead or poison_parked != want_park:
                raise RuntimeError(
                    f"actions mixed dead={poison_dead} want {want_dead} "
                    f"parked={poison_parked} want {want_park}"
                )
            logger.info(
                "hop-skip soak OK healthy=%d dead=%d parked=%d hops=%d workers=%d",
                healthy_done,
                poison_dead,
                poison_parked,
                HOPS,
                WORKER_COUNT,
            )
            return 0
        time.sleep(0.4)
    raise RuntimeError("hop-skip soak timed out")


def _assert_terminal_hops(tid: str, hops: list, *, kind: str) -> None:
    if len(hops) != HOPS:
        raise RuntimeError(f"{tid} {kind} hops={hops} want length {HOPS}")
    uniq = set(hops)
    if HOPS <= WORKER_COUNT:
        if len(uniq) != HOPS:
            raise RuntimeError(f"{tid} {kind} hops not distinct: {hops}")
    else:
        if len(uniq) < WORKER_COUNT:
            raise RuntimeError(
                f"{tid} {kind} never reached every worker: {hops}"
            )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("hop-skip soak failed")
        sys.exit(1)
