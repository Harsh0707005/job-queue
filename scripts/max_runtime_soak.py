#!/usr/bin/env python3
"""Distributed soak: max_runtime stops stuck heartbeating workers.

Enqueues jobs with short max_runtime and long sleep_ms so workers cannot
finish. Waits until jobs are leased, then asserts they never complete and
end up pending (retry) or dead after max attempts.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("max_runtime_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
N = int(os.environ.get("JOBS", "20"))
MAX_RUNTIME = int(os.environ.get("MAX_RUNTIME_SECONDS", "2"))
SLEEP_MS = int(os.environ.get("SLEEP_MS", "15000"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120"))
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
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
    last = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def backlog() -> dict[str, int]:
    bl = http_json("GET", "/v1/admin/backlog")
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in (
        "pending", "leased", "completed", "dead", "failed", "cancelled"
    )}


def main() -> int:
    logger.info(
        "max_runtime soak jobs=%d max_rt=%ds sleep_ms=%d",
        N,
        MAX_RUNTIME,
        SLEEP_MS,
    )
    for b in COORDS:
        for _ in range(60):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError("coord not ready")

    tenant = "maxrt"
    http_json("POST", f"/v1/tenants/{tenant}/pause", {})
    for i in range(N):
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tenant,
                "payload": {"kind": "stuck", "i": i, "sleep_ms": SLEEP_MS},
                "visibility_timeout_seconds": 30,
                "max_runtime_seconds": MAX_RUNTIME,
                "max_attempts": 2,
                "retry_backoff_base_seconds": 0,
            },
        )
    http_json("POST", f"/v1/tenants/{tenant}/resume", {})
    logger.info("enqueued %d stuck jobs", N)

    # Phase 1: wait until workers have leased some work.
    started = time.monotonic()
    saw_leased = False
    while time.monotonic() - started < 30:
        j = backlog()
        logger.info("wait-lease %s", j)
        if j["leased"] > 0:
            saw_leased = True
            break
        if j["completed"] > 0:
            logger.error("completed before max_runtime could apply")
            return 1
        time.sleep(0.3)
    if not saw_leased:
        logger.error("workers never leased jobs")
        return 1

    # Phase 2: jobs re-lease after force-release until max_attempts, then dead.
    # Success = zero completes ever, and all N eventually dead-lettered.
    while time.monotonic() - started < MAX_WALL:
        j = backlog()
        logger.info("drain %s", j)
        if j["completed"] > 0:
            logger.error("jobs completed despite max_runtime — fail")
            return 1
        if j["dead"] >= N and j["leased"] == 0 and j["pending"] == 0:
            time.sleep(1)
            j2 = backlog()
            if j2["completed"] > 0:
                logger.error("late complete: %s", j2)
                return 1
            if j2["dead"] < N:
                continue
            logger.info(
                "MAX RUNTIME SOAK PASSED dead=%d completed=0 (all stuck jobs stopped)",
                j2["dead"],
            )
            return 0
        time.sleep(1)

    logger.error("timeout")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
