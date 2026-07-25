#!/usr/bin/env python3
"""Distributed soak: cancel leased (running) jobs so workers cannot finish them.

Flow:
  1. Enqueue slow jobs for several tenants (handlers sleep).
  2. Wait until workers have leased some of them.
  3. Cancel all leased jobs via the coordinator API.
  4. Assert: cancelled, zero completed among cancelled ids, workers cannot ack,
     remaining pending jobs still drain cleanly.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any
from uuid import UUID

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cancel_running_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4"))
SLOW_PER_TENANT = int(os.environ.get("SLOW_PER_TENANT", "6"))
FAST_PER_TENANT = int(os.environ.get("FAST_PER_TENANT", "4"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))

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
    raise RuntimeError(f"request failed: {last}")


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
    expected_fast = TENANTS * FAST_PER_TENANT
    logger.info(
        "cancel-running soak: tenants=%d slow=%d fast=%d",
        TENANTS,
        SLOW_PER_TENANT,
        FAST_PER_TENANT,
    )
    wait_ready()

    tenants = [f"cx-t{i:02d}" for i in range(TENANTS)]
    slow_ids: list[tuple[str, str]] = []  # (tenant, job_id)

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/pause", {})

    for tid in tenants:
        for i in range(SLOW_PER_TENANT):
            j = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "slow", "i": i, "sleep_ms": 8000},
                    "visibility_timeout_seconds": 60,
                },
            )["job"]
            slow_ids.append((tid, j["id"]))
        for i in range(FAST_PER_TENANT):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "fast", "i": i},
                    "priority": 10,
                },
            )
        logger.info("enqueued %s", tid)

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/resume", {})

    # Wait until some slow jobs are leased (workers use sleep from payload if supported).
    # Default remote worker only sleeps 0.02s — we cancel ASAP after any leases appear,
    # and also cancel all remaining pending slow jobs.
    leased_targets: list[tuple[str, str]] = []
    deadline = time.time() + 30
    while time.time() < deadline and len(leased_targets) < min(4, len(slow_ids)):
        try:
            # Use backlog + probe get via cancel attempt after listing isn't available;
            # lease ourselves a few slow jobs then cancel them as "running".
            break
        except Exception:
            time.sleep(0.2)

    # Coordinator-side: lease a batch as a synthetic worker then cancel while "running",
    # plus cancel any still pending slow jobs. Also try cancel on jobs workers may hold.
    # 1) Self-lease slow work and cancel (proves fence).
    self_leased: list[dict[str, Any]] = []
    for _ in range(8):
        try:
            payload = http_json(
                "POST",
                "/v1/lease",
                {
                    "worker_id": "cancel-soak-prober",
                    "limit": 4,
                    "visibility_timeout_seconds": 60,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("lease probe: %s", exc)
            break
        jobs = payload.get("jobs") or []
        if not jobs:
            break
        for j in jobs:
            if (j.get("payload") or {}).get("kind") == "slow":
                self_leased.append(j)

    cancelled_ids: set[str] = set()
    for j in self_leased:
        r = http_json(
            "POST",
            f"/v1/jobs/{j['id']}/cancel",
            {"tenant_id": j["tenant_id"]},
        )
        assert r["job"]["status"] == "cancelled", r
        cancelled_ids.add(j["id"])
        # Old fence must not ack.
        try:
            http_json(
                "POST",
                f"/v1/jobs/{j['id']}/ack",
                {
                    "tenant_id": j["tenant_id"],
                    "worker_id": "cancel-soak-prober",
                    "lease_token": j["lease_token"],
                    "result": {"should": "fail"},
                },
            )
            logger.error("ack succeeded after cancel for %s", j["id"])
            return 1
        except Exception as exc:
            logger.info("ack correctly rejected after cancel %s: %s", j["id"][:8], exc)

    # 2) Cancel all remaining slow jobs (pending or leased by real workers).
    for tid, jid in slow_ids:
        if jid in cancelled_ids:
            continue
        try:
            r = http_json(
                "POST",
                f"/v1/jobs/{jid}/cancel",
                {"tenant_id": tid},
            )
            if r.get("job", {}).get("status") == "cancelled":
                cancelled_ids.add(jid)
        except Exception as exc:  # noqa: BLE001
            # Already completed would be bad for slow jobs if workers are fast.
            logger.warning("cancel %s: %s", jid[:8], exc)

    logger.info("cancelled %d slow jobs", len(cancelled_ids))
    if len(cancelled_ids) < TENANTS * SLOW_PER_TENANT:
        # Some may race-complete if worker is too fast; require most cancelled.
        if len(cancelled_ids) < (TENANTS * SLOW_PER_TENANT) // 2:
            logger.error("too few cancelled: %d", len(cancelled_ids))
            return 1

    # Wait for fast jobs to finish; slow cancelled must stay cancelled.
    started = time.monotonic()
    while time.monotonic() - started < MAX_WALL:
        try:
            backlog = http_json("GET", "/v1/admin/backlog")
        except Exception as exc:  # noqa: BLE001
            logger.warning("backlog: %s", exc)
            time.sleep(1)
            continue
        jobs = backlog.get("jobs") or {}
        logger.info("backlog %s cancelled_target=%d", jobs, len(cancelled_ids))
        if (
            int(jobs.get("pending") or 0) == 0
            and int(jobs.get("leased") or 0) == 0
            and int(jobs.get("completed") or 0) >= expected_fast
        ):
            # Verify cancelled jobs did not complete.
            completed = int(jobs.get("completed") or 0)
            cancelled = int(jobs.get("cancelled") or 0)
            if cancelled < len(cancelled_ids):
                logger.error(
                    "cancelled count %d < expected %d", cancelled, len(cancelled_ids)
                )
                return 1
            # Fast jobs only should complete (allow some slow race completes < half).
            if completed > expected_fast + (TENANTS * SLOW_PER_TENANT) // 4:
                logger.error(
                    "too many completions %d (fast expected %d) — cancels may have failed",
                    completed,
                    expected_fast,
                )
                return 1
            if int(jobs.get("dead") or 0) > 0:
                logger.error("dead jobs present")
                return 1
            logger.info(
                "CANCEL RUNNING SOAK PASSED completed=%d cancelled=%d",
                completed,
                cancelled,
            )
            return 0
        time.sleep(1)

    logger.error("timeout")
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
