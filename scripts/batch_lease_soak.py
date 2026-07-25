#!/usr/bin/env python3
"""Distributed batch-lease soak: multi-coordinator fleet, batch_size>1 workers.

Enqueues a known job set across tenants (with priority lanes), drains via
remote workers that pull *batches*, and verifies:

  * every job completed exactly once (no double lease/ack)
  * high-priority jobs complete before low-priority
  * fair share across tenants in the high lane
  * empty backlog
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
logger = logging.getLogger("batch_lease_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]

TENANTS = int(os.environ.get("TENANTS", "6"))
HIGH_PER = int(os.environ.get("HIGH_PER_TENANT", "20"))
LOW_PER = int(os.environ.get("LOW_PER_TENANT", "10"))
HIGH_P = int(os.environ.get("HIGH_PRIORITY", "100"))
LOW_P = int(os.environ.get("LOW_PRIORITY", "1"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300"))
POLL = float(os.environ.get("POLL_SECONDS", "1"))

_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    absolute: str | None = None,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        assert path is not None
        start = _rr % len(COORDINATOR_URLS)
        _rr += 1
        bases = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
        targets = [b + path for b in bases]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"{method} failed: {last}")


def wait_ready(timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = 0
        for b in COORDINATOR_URLS:
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    ok += 1
            except Exception:
                pass
        if ok >= 1:
            logger.info("ready %d/%d coordinators", ok, len(COORDINATOR_URLS))
            return
        time.sleep(1)
    raise RuntimeError("not ready")


def main() -> int:
    expected = TENANTS * (HIGH_PER + LOW_PER)
    expected_high = TENANTS * HIGH_PER
    logger.info(
        "batch lease soak: tenants=%d high=%d@%d low=%d@%d expected=%d",
        TENANTS,
        HIGH_PER,
        HIGH_P,
        LOW_PER,
        LOW_P,
        expected,
    )
    wait_ready()

    tenants = [f"batch-t{i:02d}" for i in range(TENANTS)]
    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/pause", {})

    for tid in tenants:
        for i in range(LOW_PER):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "priority": LOW_P,
                    "payload": {"lane": "low", "i": i},
                },
            )
        for i in range(HIGH_PER):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "priority": HIGH_P,
                    "payload": {"lane": "high", "i": i},
                },
            )
        logger.info("enqueued %s", tid)

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/resume", {})
    logger.info("resumed — batch workers draining")

    started = time.monotonic()
    last_log = 0.0
    while True:
        elapsed = time.monotonic() - started
        if elapsed > MAX_WALL:
            logger.error("timeout")
            return 1
        try:
            backlog = http_json("GET", "/v1/admin/backlog")
            prio = http_json("GET", f"/v1/admin/priority-stats?high_min={HIGH_P}")
            order = http_json(
                "GET", f"/v1/admin/completion-order?limit={expected + 20}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("poll: %s", exc)
            time.sleep(POLL)
            continue

        jobs = backlog.get("jobs") or {}
        high_done = int(prio.get("high_completed") or 0)
        low_done = int(prio.get("low_completed") or 0)
        high_open = int(prio.get("high_pending") or 0) + int(
            prio.get("high_leased") or 0
        )

        if low_done > 0 and (high_open > 0 or high_done < expected_high):
            logger.error(
                "low completed before high drained: high_done=%d low_done=%d open=%d",
                high_done,
                low_done,
                high_open,
            )
            return 1

        if int(jobs.get("dead") or 0) > 0:
            logger.error("dead jobs: %s", jobs)
            return 1

        if time.monotonic() - last_log >= 3:
            last_log = time.monotonic()
            logger.info(
                "t=%.0fs high=%d/%d low=%d backlog=%s",
                elapsed,
                high_done,
                expected_high,
                low_done,
                jobs,
            )

        if (
            high_done >= expected_high
            and low_done >= TENANTS * LOW_PER
            and int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(jobs.get("completed") or 0) == expected
        ):
            completed = order.get("completed") or []
            if len(completed) >= expected:
                prios = [int(c["priority"]) for c in completed[:expected]]
                first_low = next(
                    (i for i, p in enumerate(prios) if p < HIGH_P), None
                )
                if first_low is not None and first_low != expected_high:
                    logger.error(
                        "completion order wrong first_low=%d expected=%d",
                        first_low,
                        expected_high,
                    )
                    return 1
            by_t = prio.get("high_completed_by_tenant") or {}
            for tid in tenants:
                if int(by_t.get(tid, 0)) != HIGH_PER:
                    logger.error("tenant %s high share %s", tid, by_t)
                    return 1
            logger.info(
                "BATCH LEASE SOAK PASSED in %.1fs completed=%d",
                elapsed,
                expected,
            )
            return 0
        time.sleep(POLL)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
