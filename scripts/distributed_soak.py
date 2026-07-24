#!/usr/bin/env python3
"""Drive the distributed docker-compose fleet to soak goals.

Registers 8 tenants with 25s schedules and job.completed webhooks, then waits
until each schedule has TARGET completed jobs, backlog is empty, and the
webhook sink reports no duplicate deliveries.
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
logger = logging.getLogger("distributed_soak")

COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://coordinator:8080").rstrip("/")
SINK_URL = os.environ.get("SINK_URL", "http://webhook-sink:8090").rstrip("/")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")

TENANTS = int(os.environ.get("TENANTS", "8"))
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "25"))
TARGET_COMPLETED = int(os.environ.get("TARGET_COMPLETED", "35"))
MAX_WALL_SECONDS = int(os.environ.get("MAX_WALL_SECONDS", "2400"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "3"))


def http_json(
    method: str, url: str, body: dict[str, Any] | None = None, timeout: float = 30.0
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))


def wait_url(url: str, *, timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            http_json("GET", url)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(1)
    raise RuntimeError(f"timeout waiting for {url}: {last}")


def setup() -> dict[str, str]:
    """Register tenants/schedules/webhooks. Returns tenant_id -> schedule_id."""
    mapping: dict[str, str] = {}
    for i in range(TENANTS):
        tenant_id = f"tenant-{i:02d}"
        http_json(
            "POST",
            f"{COORDINATOR_URL}/v1/webhooks",
            {
                "tenant_id": tenant_id,
                "url": WEBHOOK_URL,
                "events": ["job.completed"],
                "secret": f"secret-{tenant_id}",
                "max_attempts": 8,
                "backoff_base_seconds": 1,
                "backoff_max_seconds": 30,
            },
        )
        sched = http_json(
            "POST",
            f"{COORDINATOR_URL}/v1/schedules",
            {
                "tenant_id": tenant_id,
                "name": f"dist-soak-{tenant_id}",
                "interval_seconds": INTERVAL_SECONDS,
                "payload": {"tenant": tenant_id, "kind": "distributed-soak"},
                "visibility_timeout_seconds": 30,
                "max_attempts": 3,
            },
        )["schedule"]
        mapping[tenant_id] = sched["id"]
        logger.info("registered %s schedule=%s", tenant_id, sched["id"])
    return mapping


def pause_done(tenant_to_sid: dict[str, str], counts: dict[str, int], disabled: set[str]) -> None:
    for tenant_id, sid in tenant_to_sid.items():
        if sid in disabled:
            continue
        if counts.get(sid, 0) >= TARGET_COMPLETED:
            http_json(
                "POST",
                f"{COORDINATOR_URL}/v1/schedules/{sid}/pause",
                {"tenant_id": tenant_id},
            )
            disabled.add(sid)
            logger.info("paused schedule %s tenant=%s at %d", sid, tenant_id, counts[sid])


def main() -> int:
    logger.info(
        "distributed soak: tenants=%d interval=%ds target=%d max_wall=%ds",
        TENANTS,
        INTERVAL_SECONDS,
        TARGET_COMPLETED,
        MAX_WALL_SECONDS,
    )
    logger.info("coordinator=%s sink=%s webhook=%s", COORDINATOR_URL, SINK_URL, WEBHOOK_URL)

    wait_url(f"{COORDINATOR_URL}/health")
    wait_url(f"{SINK_URL}/health")

    tenant_to_sid = setup()
    sids = set(tenant_to_sid.values())
    disabled: set[str] = set()
    started = time.monotonic()
    last_log = 0.0

    while True:
        elapsed = time.monotonic() - started
        if elapsed > MAX_WALL_SECONDS:
            logger.error("wall clock exceeded")
            return 1

        try:
            completed_payload = http_json(
                "GET", f"{COORDINATOR_URL}/v1/admin/completed-by-schedule"
            )
            backlog = http_json("GET", f"{COORDINATOR_URL}/v1/admin/backlog")
            wh_stats = http_json("GET", f"{COORDINATOR_URL}/v1/admin/webhook-stats")
            sink = http_json("GET", f"{SINK_URL}/stats")
        except Exception as exc:  # noqa: BLE001
            logger.warning("poll error: %s", exc)
            time.sleep(POLL_SECONDS)
            continue

        counts = {
            sid: int(completed_payload.get("completed_by_schedule", {}).get(sid, 0))
            for sid in sids
        }
        pause_done(tenant_to_sid, counts, disabled)

        jobs = backlog.get("jobs") or {}
        webs = backlog.get("webhooks") or {}
        min_c = min(counts.values()) if counts else 0
        max_c = max(counts.values()) if counts else 0

        if time.monotonic() - last_log >= 10:
            last_log = time.monotonic()
            logger.info(
                "t=%.0fs completed[min=%d max=%d sum=%d] paused=%d/%d "
                "jobs=%s webhooks=%s sink=%s",
                elapsed,
                min_c,
                max_c,
                sum(counts.values()),
                len(disabled),
                len(sids),
                jobs,
                webs,
                {
                    "posts": sink.get("total_posts"),
                    "unique": sink.get("unique_deliveries"),
                    "dups": sink.get("duplicate_posts"),
                },
            )

        # Failure signals
        if int(jobs.get("dead") or 0) > 0 or int(webs.get("dead") or 0) > 0:
            logger.error("dead rows present: jobs=%s webhooks=%s", jobs, webs)
            return 1
        if sink.get("duplicate_posts", 0) > 0 or sink.get("deliveries_with_multiple_posts", 0) > 0:
            logger.error("duplicate webhook deliveries detected: %s", sink)
            return 1
        if wh_stats.get("duplicate_succeeded_job_events"):
            logger.error("duplicate succeeded job events: %s", wh_stats)
            return 1

        goal = (
            all(counts.get(sid, 0) >= TARGET_COMPLETED for sid in sids)
            and int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(webs.get("pending") or 0) == 0
            and int(webs.get("delivering") or 0) == 0
            and int(sink.get("duplicate_posts") or 0) == 0
            and int(sink.get("unique_deliveries") or 0) >= TARGET_COMPLETED * TENANTS
            and int(sink.get("total_posts") or 0) == int(sink.get("unique_deliveries") or 0)
        )
        if goal:
            logger.info(
                "GOAL MET in %.1fs: counts=%s backlog=%s sink=%s",
                elapsed,
                counts,
                backlog,
                sink,
            )
            # Extra assert: succeeded webhooks match completed jobs.
            expected = TARGET_COMPLETED * TENANTS
            if int(jobs.get("completed") or 0) < expected:
                logger.error("completed under target: %s", jobs)
                return 1
            if int(webs.get("succeeded") or 0) < expected:
                logger.error("webhook succeeded under target: %s", webs)
                return 1
            if int(sink.get("total_posts") or 0) != int(sink.get("unique_deliveries") or 0):
                logger.error("posts != unique deliveries: %s", sink)
                return 1
            logger.info("DISTRIBUTED SOAK PASSED")
            return 0

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
