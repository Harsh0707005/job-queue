#!/usr/bin/env python3
"""Load soak: multi-tenant completed/dead retention under fleet workers.

Enqueues many jobs across tenants (mix of success + forced dead), sets short
global retention, runs cleaner, asserts aged terminal rows are purged while
fresh work still completes. Uses dual coordinators + multiple workers.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("retention_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "10"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "20"))
DEAD_FRACTION = float(os.environ.get("DEAD_FRACTION", "0.2"))
COMPLETED_RETENTION = int(os.environ.get("COMPLETED_RETENTION_SECONDS", "8"))
DEAD_RETENTION = int(os.environ.get("DEAD_RETENTION_SECONDS", "8"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))
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
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode() if exc.fp else str(exc)
            last = RuntimeError(f"{exc.code} {raw}")
            if exc.code in (400, 409, 429):
                raise last from exc
            continue
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


def main() -> int:
    total_jobs = TENANTS * JOBS_PER_TENANT
    expect_dead = int(total_jobs * DEAD_FRACTION)
    logger.info(
        "retention soak tenants=%d jobs/t=%d ret_c=%ds ret_d=%ds total=%d",
        TENANTS,
        JOBS_PER_TENANT,
        COMPLETED_RETENTION,
        DEAD_RETENTION,
        total_jobs,
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

    # Short retention windows so the cleaner can prove deletes under load.
    http_json(
        "POST",
        "/v1/admin/retention",
        {
            "completed_retention_seconds": COMPLETED_RETENTION,
            "dead_retention_seconds": DEAD_RETENTION,
        },
    )
    ret = http_json("GET", "/v1/admin/retention")
    logger.info("retention config %s", ret)

    def seed(t: int) -> int:
        tid = f"ret-{t:02d}"
        n = 0
        for i in range(JOBS_PER_TENANT):
            # Kind "fail" forces remote worker to nack → dead after max_attempts.
            # Remote default handler: payload kind fail / raise.
            fail = i < max(1, int(JOBS_PER_TENANT * DEAD_FRACTION))
            payload: dict[str, Any]
            if fail:
                payload = {
                    "kind": "fail",
                    "error": "retention-soak-force-dead",
                    "ms": 30,
                }
                max_attempts = 1
            else:
                payload = {"kind": "work", "ms": 40, "tenant": tid, "i": i}
                max_attempts = 3
            try:
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": payload,
                        "priority": i % 3,
                        "max_attempts": max_attempts,
                        "visibility_timeout_seconds": 10,
                        "retry_backoff_base_seconds": 0,
                    },
                )
                n += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("seed %s: %s", tid, exc)
        return n

    seeded = 0
    with ThreadPoolExecutor(max_workers=min(10, TENANTS)) as pool:
        for f in as_completed([pool.submit(seed, t) for t in range(TENANTS)]):
            seeded += f.result()
    logger.info("seeded %d (expect_dead~%d)", seeded, expect_dead)

    # Phase 1: process active work; track peak terminal counts (cleaner may
    # already be purging mid-run under short retention).
    started = time.monotonic()
    completed_peak = 0
    dead_peak = 0
    saw_empty_active = False
    while time.monotonic() - started < MAX_WALL * 0.5:
        b = backlog()
        completed_peak = max(completed_peak, int(b.get("completed") or 0))
        dead_peak = max(dead_peak, int(b.get("dead") or 0))
        logger.info(
            "drain-active backlog=%s peaks c=%d d=%d",
            b,
            completed_peak,
            dead_peak,
        )
        if int(b.get("pending") or 0) == 0 and int(b.get("leased") or 0) == 0:
            saw_empty_active = True
            # Give a moment for late acks, then leave the active-drain phase.
            time.sleep(1)
            b = backlog()
            completed_peak = max(completed_peak, int(b.get("completed") or 0))
            dead_peak = max(dead_peak, int(b.get("dead") or 0))
            if int(b.get("pending") or 0) == 0 and int(b.get("leased") or 0) == 0:
                break
        time.sleep(1)

    logger.info(
        "after active drain peaks completed=%d dead=%d empty_active=%s",
        completed_peak,
        dead_peak,
        saw_empty_active,
    )
    if not saw_empty_active:
        logger.error("active work never fully drained")
        return 1
    if completed_peak < 1:
        logger.error("never observed completed jobs (workers may not have acked)")
        return 1
    if completed_peak + dead_peak < seeded * 0.5:
        # Allow lower ratio: cleaner can delete terminals before we sample peak.
        logger.warning(
            "peak terminal %d low vs seeded %d (cleaner may race); continuing",
            completed_peak + dead_peak,
            seeded,
        )

    # Phase 2: wait past retention; dual cleaners + explicit purge.
    wait = max(COMPLETED_RETENTION, DEAD_RETENTION) + 4
    logger.info("waiting %ds for retention + cleaner", wait)
    time.sleep(wait)

    total_purged = 0
    for _ in range(40):
        try:
            r = http_json(
                "POST",
                "/v1/admin/purge-expired-jobs",
                {"limit": 1000},
            )
            n = int(r.get("total_deleted") or 0)
            total_purged += n
            if n == 0:
                break
        except Exception as exc:  # noqa: BLE001
            logger.warning("purge: %s", exc)
            break
        time.sleep(0.2)

    b2 = backlog()
    logger.info(
        "final backlog=%s purged_via_admin=%d peaks c=%d d=%d",
        b2,
        total_purged,
        completed_peak,
        dead_peak,
    )
    completed_left = int(b2.get("completed") or 0)
    dead_left = int(b2.get("dead") or 0)
    # After retention, aged terminals must be gone (allow a tiny race window).
    if completed_left > 3:
        logger.error(
            "completed not purged enough: left=%d peak=%d",
            completed_left,
            completed_peak,
        )
        return 1
    if dead_left > 3:
        logger.error(
            "dead not purged enough: left=%d peak=%d",
            dead_left,
            dead_peak,
        )
        return 1
    if int(b2.get("pending") or 0) != 0 or int(b2.get("leased") or 0) != 0:
        logger.error("active backlog not empty: %s", b2)
        return 1

    # Phase 3: tenant override None = forever (must not follow short global).
    keep_tid = "ret-keep"
    http_json(
        "POST",
        f"/v1/tenants/{keep_tid}/retention",
        {
            "completed_retention_seconds": None,
            "dead_retention_seconds": None,
        },
    )
    tr = http_json("GET", f"/v1/tenants/{keep_tid}/retention")
    if tr.get("completed_inherits") or tr.get("dead_inherits"):
        logger.error("tenant forever override mis-decoded as inherit: %s", tr)
        return 1
    if tr.get("completed_retention_seconds") is not None:
        logger.error("tenant forever should expose null seconds: %s", tr)
        return 1

    http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": keep_tid,
            "payload": {"kind": "work", "ms": 20},
            "max_attempts": 3,
        },
    )
    # Wait for job to complete under workers.
    keep_done = False
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        b3 = backlog()
        # Global retention is short; forever tenant job must still exist.
        # We cannot see per-tenant counts in backlog; wait past global window
        # then ensure purge does not remove all jobs if keep tenant still has one.
        if int(b3.get("completed") or 0) >= 1:
            keep_done = True
            break
        if int(b3.get("pending") or 0) == 0 and int(b3.get("leased") or 0) == 0:
            # Completed and already purged globally for other tenants; keep may remain.
            keep_done = True
            break
        time.sleep(0.5)
    if not keep_done:
        logger.error("keep-tenant job never left active states")
        return 1

    time.sleep(max(COMPLETED_RETENTION, DEAD_RETENTION) + 3)
    # Force purge; forever tenant must retain its completed job.
    for _ in range(20):
        r = http_json(
            "POST", "/v1/admin/purge-expired-jobs", {"limit": 1000}
        )
        if int(r.get("total_deleted") or 0) == 0:
            break
    b_keep = backlog()
    # The keep job should still be counted as completed (global others gone).
    if int(b_keep.get("completed") or 0) < 1:
        logger.error(
            "tenant forever override failed: completed wiped under global "
            "retention (backlog=%s tenant_ret=%s)",
            b_keep,
            tr,
        )
        return 1

    logger.info(
        "RETENTION SOAK PASSED peak_completed=%d peak_dead=%d "
        "final=%s purged=%d keep_completed=%s",
        completed_peak,
        dead_peak,
        b2,
        total_purged,
        b_keep.get("completed"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
