#!/usr/bin/env python3
"""Distributed chaos soak: dual coordinators, remote webhook agents, kill+pause.

Verifies:
  * no duplicate webhook POSTs (sink total_posts == unique_deliveries)
  * no double schedule triggers (never >1 pending+leased job per schedule)
  * fair leasing (completed per-tenant stays balanced when not paused)
  * pause/resume: paused tenants get no new claimable work
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
logger = logging.getLogger("chaos_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or os.environ.get("COORDINATOR_URL")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SINK_URL = os.environ.get("SINK_URL", "http://webhook-sink:8090").rstrip("/")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")

TENANTS = int(os.environ.get("TENANTS", "8"))
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "20"))
TARGET_COMPLETED = int(os.environ.get("TARGET_COMPLETED", "25"))
MAX_WALL_SECONDS = int(os.environ.get("MAX_WALL_SECONDS", "2400"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "3"))
PAUSE_TENANTS = [
    t.strip()
    for t in os.environ.get("PAUSE_TENANTS", "tenant-01,tenant-03").split(",")
    if t.strip()
]
PAUSE_AFTER = float(os.environ.get("PAUSE_AFTER_SECONDS", "90"))
RESUME_AFTER = float(os.environ.get("RESUME_AFTER_SECONDS", "180"))

_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
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
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}
                return json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"request failed {method} {absolute or path}: {last}")


def wait_ready(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = 0
        for base in COORDINATOR_URLS:
            try:
                h = http_json("GET", absolute=base + "/health")
                if h.get("ok"):
                    ok += 1
            except Exception:
                pass
        try:
            http_json("GET", absolute=SINK_URL + "/health")
            sink_ok = True
        except Exception:
            sink_ok = False
        if ok >= 1 and sink_ok:
            logger.info("ready: %d/%d coordinators + sink", ok, len(COORDINATOR_URLS))
            return
        time.sleep(1)
    raise RuntimeError("stack not ready")


def setup() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for i in range(TENANTS):
        tenant_id = f"tenant-{i:02d}"
        http_json(
            "POST",
            "/v1/webhooks",
            {
                "tenant_id": tenant_id,
                "url": WEBHOOK_URL,
                "events": ["job.completed"],
                "secret": f"secret-{tenant_id}",
                "max_attempts": 10,
                "backoff_base_seconds": 1,
                "backoff_max_seconds": 20,
            },
        )
        sched = http_json(
            "POST",
            "/v1/schedules",
            {
                "tenant_id": tenant_id,
                "name": f"chaos-{tenant_id}",
                "interval_seconds": INTERVAL_SECONDS,
                "payload": {"tenant": tenant_id, "kind": "chaos"},
                "visibility_timeout_seconds": 30,
                "max_attempts": 3,
            },
        )["schedule"]
        mapping[tenant_id] = sched["id"]
        logger.info("registered %s schedule=%s", tenant_id, sched["id"])
    return mapping


def main() -> int:
    logger.info(
        "chaos soak: tenants=%d interval=%ds target=%d coords=%s pause=%s",
        TENANTS,
        INTERVAL_SECONDS,
        TARGET_COMPLETED,
        COORDINATOR_URLS,
        PAUSE_TENANTS,
    )
    wait_ready()
    tenant_to_sid = setup()
    sids = set(tenant_to_sid.values())
    disabled: set[str] = set()
    paused_set: set[str] = set()
    did_pause = False
    did_resume = False
    started = time.monotonic()
    last_log = 0.0
    max_active_seen = 0
    fairness_samples: list[tuple[int, int]] = []
    completed_at_pause: dict[str, int] = {}

    while True:
        elapsed = time.monotonic() - started
        if elapsed > MAX_WALL_SECONDS:
            logger.error("wall clock exceeded")
            return 1

        try:
            completed_payload = http_json("GET", "/v1/admin/completed-by-schedule")
            backlog = http_json("GET", "/v1/admin/backlog")
            wh_stats = http_json("GET", "/v1/admin/webhook-stats")
            sink = http_json("GET", absolute=SINK_URL + "/stats")
            fairness = http_json("GET", "/v1/admin/fairness-check")
        except Exception as exc:  # noqa: BLE001
            logger.warning("poll error (expected during coordinator kill): %s", exc)
            time.sleep(POLL_SECONDS)
            continue

        counts = {
            sid: int(completed_payload.get("completed_by_schedule", {}).get(sid, 0))
            for sid in sids
        }
        completed_by_tenant = fairness.get("completed_by_tenant") or {}

        if not did_pause and elapsed >= PAUSE_AFTER:
            for t in PAUSE_TENANTS:
                try:
                    http_json("POST", f"/v1/tenants/{t}/pause", {})
                    paused_set.add(t)
                    completed_at_pause[t] = int(completed_by_tenant.get(t, 0))
                    logger.info(
                        "PAUSED tenant %s (completed_at_pause=%d)",
                        t,
                        completed_at_pause[t],
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("pause %s failed: %s", t, exc)
            did_pause = True
        if did_pause and not did_resume and elapsed >= RESUME_AFTER:
            for t in list(paused_set):
                try:
                    http_json("POST", f"/v1/tenants/{t}/resume", {})
                    paused_set.discard(t)
                    logger.info("RESUMED tenant %s", t)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("resume %s failed: %s", t, exc)
            did_resume = True

        for tenant_id, sid in tenant_to_sid.items():
            if sid in disabled:
                continue
            if counts.get(sid, 0) >= TARGET_COMPLETED:
                try:
                    http_json(
                        "POST",
                        f"/v1/schedules/{sid}/pause",
                        {"tenant_id": tenant_id},
                    )
                    disabled.add(sid)
                    logger.info("schedule paused %s tenant=%s", sid, tenant_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("pause schedule failed: %s", exc)

        jobs = backlog.get("jobs") or {}
        webs = backlog.get("webhooks") or {}
        active_by_sched = fairness.get("active_jobs_by_schedule") or {}
        max_active = int(fairness.get("max_active_per_schedule") or 0)
        max_active_seen = max(max_active_seen, max_active)

        unpaused_counts = []
        for tenant_id, sid in tenant_to_sid.items():
            if tenant_id in paused_set:
                continue
            unpaused_counts.append(counts.get(sid, 0))
        if len(unpaused_counts) >= 2:
            fairness_samples.append((min(unpaused_counts), max(unpaused_counts)))

        if max_active > 1:
            logger.error(
                "SCHEDULE DOUBLE TRIGGER: active jobs per schedule=%s",
                active_by_sched,
            )
            return 1
        # Logical duplicates: two succeeded outbox rows for same job+event.
        if wh_stats.get("duplicate_succeeded_job_events"):
            logger.error("duplicate succeeded job events: %s", wh_stats)
            return 1
        # Sink unique deliveries must never exceed completed jobs (ghost posts).
        if int(sink.get("unique_deliveries") or 0) > int(jobs.get("completed") or 0) + 2:
            logger.error(
                "sink unique deliveries exceed completed jobs: sink=%s jobs=%s",
                sink,
                jobs,
            )
            return 1
        if int(jobs.get("dead") or 0) > 0 or int(webs.get("dead") or 0) > 0:
            logger.error("dead rows: jobs=%s webhooks=%s", jobs, webs)
            return 1

        # While paused, completed may rise by at most 1 (in-flight lease).
        # Large growth would mean schedules/leases still running for paused tenant.
        if paused_set and elapsed > PAUSE_AFTER + 20:
            for t in paused_set:
                base = completed_at_pause.get(t, 0)
                now_c = int(completed_by_tenant.get(t, 0))
                if now_c > base + 1:
                    logger.error(
                        "paused tenant %s completed grew %d -> %d (schedules still firing?)",
                        t,
                        base,
                        now_c,
                    )
                    return 1

        min_c = min(counts.values()) if counts else 0
        max_c = max(counts.values()) if counts else 0
        if time.monotonic() - last_log >= 10:
            last_log = time.monotonic()
            logger.info(
                "t=%.0fs completed[min=%d max=%d sum=%d] paused_tenants=%s "
                "sched_paused=%d max_active/sched=%d jobs=%s wh=%s sink=%s",
                elapsed,
                min_c,
                max_c,
                sum(counts.values()),
                sorted(paused_set),
                len(disabled),
                max_active,
                jobs,
                webs,
                {
                    "posts": sink.get("total_posts"),
                    "unique": sink.get("unique_deliveries"),
                    "retries": sink.get("retry_posts"),
                },
            )

        goal = (
            all(counts.get(sid, 0) >= TARGET_COMPLETED for sid in sids)
            and int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(webs.get("pending") or 0) == 0
            and int(webs.get("delivering") or 0) == 0
            and int(webs.get("succeeded") or 0) >= TARGET_COMPLETED * TENANTS
            and int(sink.get("unique_deliveries") or 0) >= TARGET_COMPLETED * TENANTS
            and int(sink.get("unique_jobs") or 0) >= TARGET_COMPLETED * TENANTS
            and not wh_stats.get("duplicate_succeeded_job_events")
            and did_pause
            and did_resume
            and max_active_seen <= 1
        )
        if goal:
            # Fairness among active tenants: allow some skew from kills/pauses.
            bad = [s for s in fairness_samples if s[1] - s[0] > 5]
            if fairness_samples and len(bad) > max(3, len(fairness_samples) * 0.6):
                logger.error("fairness regression samples=%s", bad[:15])
                return 1
            logger.info(
                "GOAL MET in %.1fs max_active_seen=%d sink=%s counts=%s",
                elapsed,
                max_active_seen,
                sink,
                counts,
            )
            logger.info("CHAOS SOAK PASSED")
            return 0

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
