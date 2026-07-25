#!/usr/bin/env python3
"""Distributed priority-lane soak against dual coordinators + remote workers.

Enqueues mixed high/low priority jobs across several tenants, then drains via
fair remote workers. Asserts:

  * all high-priority jobs complete before any low-priority job
  * within the high lane, no single tenant monopolizes (share of leases)
  * low-priority tenants still complete after the high lane drains
  * empty backlog at the end
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
logger = logging.getLogger("priority_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or os.environ.get("COORDINATOR_URL")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]

TENANTS = int(os.environ.get("TENANTS", "6"))
HIGH_PER_TENANT = int(os.environ.get("HIGH_PER_TENANT", "12"))
LOW_PER_TENANT = int(os.environ.get("LOW_PER_TENANT", "8"))
HIGH_PRIORITY = int(os.environ.get("HIGH_PRIORITY", "100"))
LOW_PRIORITY = int(os.environ.get("LOW_PRIORITY", "1"))
MAX_WALL_SECONDS = int(os.environ.get("MAX_WALL_SECONDS", "600"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "1"))

_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
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
                return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"{method} {absolute or path} failed: {last}")


def wait_ready(timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = 0
        for base in COORDINATOR_URLS:
            try:
                if http_json("GET", absolute=base + "/health").get("ok"):
                    ok += 1
            except Exception:
                pass
        if ok >= 1:
            logger.info("ready: %d/%d coordinators", ok, len(COORDINATOR_URLS))
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def enqueue_via_admin(tenant_id: str, priority: int, n: int, tag: str) -> list[str]:
    """Enqueue by calling coordinator lease APIs is not enough — need enqueue.

    Use a thin enqueue endpoint if present; else use direct JobQueue is not
    available. We add POST /v1/jobs for setup.
    """
    ids: list[str] = []
    for i in range(n):
        payload = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tenant_id,
                "payload": {"tag": tag, "i": i, "priority": priority},
                "priority": priority,
            },
        )
        ids.append(payload["job"]["id"])
    return ids


def main() -> int:
    expected_high = TENANTS * HIGH_PER_TENANT
    expected_low = TENANTS * LOW_PER_TENANT
    expected_total = expected_high + expected_low
    logger.info(
        "priority soak: tenants=%d high=%d@p%d low=%d@p%d coords=%s",
        TENANTS,
        HIGH_PER_TENANT,
        HIGH_PRIORITY,
        LOW_PER_TENANT,
        LOW_PRIORITY,
        COORDINATOR_URLS,
    )
    wait_ready()

    high_ids: set[str] = set()
    low_ids: set[str] = set()
    tenants = [f"prio-t{i:02d}" for i in range(TENANTS)]

    # Pause so remote workers cannot drain mid-setup (low was enqueued first).
    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/pause", {})
    logger.info("paused %d tenants for setup", len(tenants))

    for tid in tenants:
        # Low first intentionally — only priority lanes (not enqueue order) should win.
        low_ids.update(
            enqueue_via_admin(tid, LOW_PRIORITY, LOW_PER_TENANT, "low")
        )
        high_ids.update(
            enqueue_via_admin(tid, HIGH_PRIORITY, HIGH_PER_TENANT, "high")
        )
        logger.info(
            "enqueued tenant=%s high=%d low=%d",
            tid,
            HIGH_PER_TENANT,
            LOW_PER_TENANT,
        )

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/resume", {})
    logger.info("resumed tenants — workers may drain")

    # Snapshot completion order by polling completed job priorities via admin.
    started = time.monotonic()
    last_log = 0.0
    seen_low_while_high_pending = False

    while True:
        elapsed = time.monotonic() - started
        if elapsed > MAX_WALL_SECONDS:
            logger.error("timeout")
            return 1

        try:
            order = http_json(
                "GET", f"/v1/admin/completion-order?limit={expected_total + 50}"
            )
            backlog = http_json("GET", "/v1/admin/backlog")
            # Threshold between lanes: anything >= HIGH_PRIORITY is "high".
            fairness = http_json(
                "GET", f"/v1/admin/priority-stats?high_min={HIGH_PRIORITY}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("poll error: %s", exc)
            time.sleep(POLL_SECONDS)
            continue

        completed = order.get("completed") or []
        jobs = backlog.get("jobs") or {}
        high_done = int(fairness.get("high_completed") or 0)
        low_done = int(fairness.get("low_completed") or 0)
        high_pending = int(fairness.get("high_pending") or 0) + int(
            fairness.get("high_leased") or 0
        )

        # Invariant: while high work remains, no low completions yet
        # (allow equality only after high is fully done).
        if low_done > 0 and high_pending > 0:
            logger.error(
                "LOW completed while HIGH still outstanding: low_done=%d high_pending=%d",
                low_done,
                high_pending,
            )
            return 1
        if low_done > 0 and high_done < expected_high:
            # Strict: all high must finish before first low.
            logger.error(
                "LOW started before all HIGH finished: high_done=%d expected=%d",
                high_done,
                expected_high,
            )
            return 1

        # Within high lane, share of completed should not be one tenant > 70%
        # early on (monopoly). Check mid-drain.
        by_tenant_high = fairness.get("high_completed_by_tenant") or {}
        if high_done >= max(6, expected_high // 3) and high_done < expected_high:
            total_h = sum(int(v) for v in by_tenant_high.values()) or 1
            for t, n in by_tenant_high.items():
                share = int(n) / total_h
                if share > 0.55 and TENANTS >= 3:
                    logger.error(
                        "high-lane monopoly: tenant=%s share=%.2f stats=%s",
                        t,
                        share,
                        by_tenant_high,
                    )
                    return 1

        if time.monotonic() - last_log >= 5:
            last_log = time.monotonic()
            logger.info(
                "t=%.0fs high_done=%d/%d low_done=%d/%d backlog=%s high_by_tenant=%s",
                elapsed,
                high_done,
                expected_high,
                low_done,
                expected_low,
                jobs,
                by_tenant_high,
            )

        if (
            high_done >= expected_high
            and low_done >= expected_low
            and int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(jobs.get("completed") or 0) >= expected_total
        ):
            # Final order check: all high before any low in completion stream.
            priorities = [int(c["priority"]) for c in completed[:expected_total]]
            if len(priorities) < expected_total:
                # completion-order may be capped; use aggregate stats as primary.
                pass
            else:
                first_low = next(
                    (i for i, p in enumerate(priorities) if p == LOW_PRIORITY),
                    None,
                )
                if first_low is not None:
                    if any(p == HIGH_PRIORITY for p in priorities[first_low:]):
                        logger.error("high completed after low in order: %s", priorities[:40])
                        return 1
                    if first_low != expected_high:
                        logger.error(
                            "first low at index %d, expected %d",
                            first_low,
                            expected_high,
                        )
                        return 1

            # Fairness: each tenant got its high jobs.
            for i in range(TENANTS):
                tid = f"prio-t{i:02d}"
                got = int(by_tenant_high.get(tid, 0))
                if got != HIGH_PER_TENANT:
                    logger.error(
                        "tenant %s high completed=%d expected=%d",
                        tid,
                        got,
                        HIGH_PER_TENANT,
                    )
                    return 1

            logger.info(
                "PRIORITY SOAK PASSED in %.1fs high=%d low=%d",
                elapsed,
                high_done,
                low_done,
            )
            return 0

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
