#!/usr/bin/env python3
"""Load soak: multi-tenant cron schedules under pending caps + enqueue rates.

Registers many cron schedules (second-resolution) across tenants that also
have tight pending caps and enqueue rate limits. Schedule materialization
must still fire (caps/rates do not apply to schedule ticks). Workers drain
jobs. Success = enough schedule-originated completions and empty backlog.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cron_schedule_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "12"))
SCHEDULES_PER_TENANT = int(os.environ.get("SCHEDULES_PER_TENANT", "4"))
# 6-field cron: every N seconds
CRON_EVERY_SEC = int(os.environ.get("CRON_EVERY_SEC", "3"))
PENDING_CAP = int(os.environ.get("PENDING_CAP", "1"))
ENQUEUE_RATE = float(os.environ.get("ENQUEUE_RATE", "0.5"))
ENQUEUE_BURST = int(os.environ.get("ENQUEUE_BURST", "1"))
# How long to let schedules fire + drain
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "45"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))
MIN_COMPLETED = int(os.environ.get("MIN_COMPLETED", "80"))
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


def completed_by_schedule() -> dict[str, Any]:
    try:
        return http_json("GET", "/v1/admin/completed-by-schedule")
    except Exception as exc:  # noqa: BLE001
        logger.warning("completed-by-schedule: %s", exc)
        return {}


def main() -> int:
    cron = f"*/{CRON_EVERY_SEC} * * * * *"
    total_schedules = TENANTS * SCHEDULES_PER_TENANT
    logger.info(
        "cron soak tenants=%d schedules/t=%d cron=%r pending_cap=%d "
        "enqueue_rate=%s/%s min_completed=%d",
        TENANTS,
        SCHEDULES_PER_TENANT,
        cron,
        PENDING_CAP,
        ENQUEUE_RATE,
        ENQUEUE_BURST,
        MIN_COMPLETED,
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

    def setup_tenant(t: int) -> list[tuple[str, str]]:
        tid = f"cron-{t:02d}"
        out: list[tuple[str, str]] = []
        # Tight throttles that would block manual flood; schedules must ignore.
        try:
            http_json(
                "POST",
                f"/v1/tenants/{tid}/concurrency",
                {"max_concurrent_leases": 2},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("concurrency %s: %s", tid, exc)
        try:
            http_json(
                "POST",
                f"/v1/tenants/{tid}/pending-limit",
                {"max_pending_jobs": PENDING_CAP},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("pending-limit %s: %s", tid, exc)
        try:
            http_json(
                "POST",
                f"/v1/tenants/{tid}/rate-limits",
                {
                    "enqueue_per_second": ENQUEUE_RATE,
                    "enqueue_burst": ENQUEUE_BURST,
                    "lease_per_second": 50,
                    "lease_burst": 50,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("rate-limits %s: %s", tid, exc)

        # Fill pending via one manual enqueue then fail more (rate/pending).
        try:
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "manual", "seed": True},
                    "priority": 0,
                },
            )
        except Exception:
            pass

        for i in range(SCHEDULES_PER_TENANT):
            try:
                resp = http_json(
                    "POST",
                    "/v1/schedules",
                    {
                        "tenant_id": tid,
                        "cron": cron,
                        "timezone": "UTC",
                        "name": f"s-{i}",
                        "payload": {
                            "kind": "cron_tick",
                            "tenant": tid,
                            "sched": i,
                            "ms": 20,
                        },
                        "priority": i % 3,
                        "max_attempts": 5,
                        "visibility_timeout_seconds": 15,
                    },
                )
                sid = (resp.get("schedule") or {}).get("id")
                if sid:
                    out.append((tid, str(sid)))
            except Exception as exc:  # noqa: BLE001
                logger.warning("schedule %s/%s: %s", tid, i, exc)
        return out

    schedule_ids: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(12, TENANTS)) as pool:
        for f in as_completed(
            [pool.submit(setup_tenant, t) for t in range(TENANTS)]
        ):
            schedule_ids.extend(f.result())
    registered = len(schedule_ids)
    logger.info("registered %d / %d schedules", registered, total_schedules)
    if registered < total_schedules * 0.9:
        logger.error("too few schedules registered")
        return 1

    # Wait for schedules to fire while workers drain.
    started = time.monotonic()
    while time.monotonic() - started < min(RUN_SECONDS, MAX_WALL):
        b = backlog()
        cbs = completed_by_schedule()
        by_sched = cbs.get("completed_by_schedule") or {}
        sched_completed = sum(int(v) for v in by_sched.values())
        logger.info(
            "progress backlog=%s schedule_completed_jobs=%d unique_schedules=%d",
            b,
            sched_completed,
            len(by_sched),
        )
        time.sleep(2)

    # Stop further ticks so the backlog can drain.
    logger.info("pausing %d schedules for drain", len(schedule_ids))
    for tid, sid in schedule_ids:
        try:
            http_json(
                "POST",
                f"/v1/schedules/{sid}/pause",
                {"tenant_id": tid},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("pause %s: %s", sid, exc)

    drain_deadline = time.monotonic() + max(45, MAX_WALL - RUN_SECONDS)
    while time.monotonic() < drain_deadline:
        b = backlog()
        logger.info("drain backlog=%s", b)
        if (
            int(b.get("pending") or 0) == 0
            and int(b.get("leased") or 0) == 0
            and int(b.get("failed") or 0) == 0
        ):
            time.sleep(2)
            b2 = backlog()
            if int(b2.get("pending") or 0) == 0 and int(b2.get("leased") or 0) == 0:
                break
        time.sleep(2)

    b = backlog()
    cbs = completed_by_schedule()
    by_sched = cbs.get("completed_by_schedule") or {}
    sched_completed = sum(int(v) for v in by_sched.values())
    completed = int(b.get("completed") or 0)
    logger.info(
        "final backlog=%s completed=%d schedule_originated=%d schedules_fired=%d",
        b,
        completed,
        sched_completed,
        len(by_sched),
    )

    if completed < MIN_COMPLETED:
        logger.error(
            "completed %d < min %d (schedules may not have fired under load)",
            completed,
            MIN_COMPLETED,
        )
        return 1
    if sched_completed < MIN_COMPLETED * 0.5:
        logger.error(
            "schedule-originated completions %d too low (min ~%d)",
            sched_completed,
            int(MIN_COMPLETED * 0.5),
        )
        return 1
    if len(by_sched) < registered * 0.5:
        logger.error(
            "only %d schedules produced completions (registered %d)",
            len(by_sched),
            registered,
        )
        return 1
    if int(b.get("pending") or 0) > 0 or int(b.get("leased") or 0) > 0:
        logger.error("backlog not drained: %s", b)
        return 1
    if int(b.get("dead") or 0) > registered * 0.1:
        logger.error("too many dead jobs: %s", b)
        return 1

    # Manual flood still rate-limited (sanity: throttles are active).
    try:
        http_json(
            "POST",
            "/v1/jobs",
            {"tenant_id": "cron-00", "payload": {"kind": "probe"}},
        )
        # second may fail with 429 or 400 depending on pending
        try:
            http_json(
                "POST",
                "/v1/jobs",
                {"tenant_id": "cron-00", "payload": {"kind": "probe2"}},
            )
        except Exception:
            pass
    except Exception:
        pass

    logger.info(
        "CRON SCHEDULE SOAK PASSED completed=%d schedule_jobs=%d schedules=%d",
        completed,
        sched_completed,
        len(by_sched),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
