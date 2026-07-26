#!/usr/bin/env python3
"""Hard chaos soak: many tenants/jobs/workers; audit must match final state.

Pushes dual coordinators + workers while chaos kills coordinators. At the end
every job's immutable audit stream must:
  * have contiguous seq starting at 1
  * end with to_status equal to the job row status
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
logger = logging.getLogger("audit_chaos_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "12"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "40"))
TARGET_COMPLETED_RATIO = float(os.environ.get("TARGET_COMPLETED_RATIO", "0.85"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "420"))
POLL = float(os.environ.get("POLL_SECONDS", "2"))
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


def wait_ready() -> None:
    for b in COORDS:
        for _ in range(120):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog err (chaos?): %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    target_done = int(total * TARGET_COMPLETED_RATIO)
    logger.info(
        "audit chaos soak tenants=%d jobs/tenant=%d total=%d target_completed>=%d",
        TENANTS,
        JOBS_PER_TENANT,
        total,
        target_done,
    )
    wait_ready()

    # Seed aggressively across tenants.
    def seed_tenant(t: int) -> int:
        tid = f"audit-{t:02d}"
        n = 0
        for i in range(JOBS_PER_TENANT):
            kind = "steps" if i % 3 else "plain"
            payload: dict[str, Any]
            if kind == "steps":
                payload = {
                    "kind": "steps",
                    "steps": 2 + (i % 4),
                    "step_ms": 50 + (i % 5) * 20,
                }
            else:
                payload = {"kind": "plain", "i": i, "sleep_ms": 30 + (i % 40)}
            # Occasional approval gate for diversity
            if i % 11 == 0:
                payload = {
                    "kind": "steps",
                    "steps": 4,
                    "step_ms": 40,
                    "approval_after_step": 2,
                    "approval_timeout_seconds": 8,
                }
            try:
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": payload,
                        "priority": i % 5,
                        "max_attempts": 6,
                        "visibility_timeout_seconds": 12,
                        "retry_backoff_base_seconds": 0,
                    },
                )
                n += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("enqueue fail %s: %s", tid, exc)
        return n

    seeded = 0
    with ThreadPoolExecutor(max_workers=min(8, TENANTS)) as pool:
        futs = [pool.submit(seed_tenant, t) for t in range(TENANTS)]
        for f in as_completed(futs):
            seeded += f.result()
    logger.info("seeded %d jobs", seeded)

    # Drive approvals in background while work runs.
    def approve_loop(stop_at: float) -> None:
        while time.time() < stop_at:
            for t in range(TENANTS):
                tid = f"audit-{t:02d}"
                try:
                    # Process timeouts and approve whatever is waiting (best-effort).
                    http_json(
                        "POST",
                        "/v1/approvals/process-timeouts",
                        {"tenant_id": tid, "limit": 50},
                    )
                    listing = http_json(
                        "GET", f"/v1/approvals?tenant_id={tid}&limit=20"
                    )
                    for j in listing.get("jobs") or []:
                        # Approve half, let half time out for richer audit.
                        jid = j["id"]
                        if hash(jid) % 2 == 0:
                            try:
                                http_json(
                                    "POST",
                                    f"/v1/jobs/{jid}/approve",
                                    {"tenant_id": tid},
                                )
                            except Exception:
                                pass
                except Exception:
                    pass
            time.sleep(1.0)

    import threading

    stop_at = time.time() + MAX_WALL - 30
    thr = threading.Thread(target=approve_loop, args=(stop_at,), daemon=True)
    thr.start()

    started = time.monotonic()
    while time.monotonic() - started < MAX_WALL:
        b = backlog()
        completed = int(b.get("completed") or 0)
        pending = int(b.get("pending") or 0)
        leased = int(b.get("leased") or 0)
        cancelled = int(b.get("cancelled") or 0)
        logger.info(
            "progress completed=%d pending=%d leased=%d cancelled=%d dead=%s",
            completed,
            pending,
            leased,
            cancelled,
            b.get("dead"),
        )
        if completed >= target_done and leased == 0 and pending < TENANTS:
            # Give a moment for stragglers / approvals.
            time.sleep(3)
            b2 = backlog()
            if int(b2.get("leased") or 0) == 0:
                break
        time.sleep(POLL)

    # Final approval timeout sweep.
    for t in range(TENANTS):
        try:
            http_json(
                "POST",
                "/v1/approvals/process-timeouts",
                {"tenant_id": f"audit-{t:02d}", "limit": 200},
            )
        except Exception:
            pass
    time.sleep(2)

    # Verify audit consistency for every tenant hard.
    mismatches = 0
    checked = 0
    for t in range(TENANTS):
        tid = f"audit-{t:02d}"
        for attempt in range(8):
            try:
                report = http_json(
                    "GET",
                    f"/v1/tenants/{tid}/audit/verify?limit=50000",
                    timeout=60,
                )
                break
            except Exception as exc:  # noqa: BLE001
                logger.warning("verify %s attempt %d: %s", tid, attempt, exc)
                time.sleep(1)
        else:
            logger.error("verify failed for tenant %s", tid)
            mismatches += 1
            continue
        checked += int(report.get("checked") or 0)
        bad = int(report.get("mismatches") or 0)
        if not report.get("ok"):
            logger.error(
                "tenant %s audit mismatches=%d sample=%s",
                tid,
                bad,
                (report.get("reports") or [])[:3],
            )
            mismatches += bad
        else:
            logger.info("tenant %s audit ok checked=%d", tid, report.get("checked"))

    b = backlog()
    logger.info("final backlog %s checked_jobs=%d mismatches=%d", b, checked, mismatches)
    if mismatches > 0:
        logger.error("AUDIT CHAOS SOAK FAILED mismatches=%d", mismatches)
        return 1
    if checked < seeded * 0.5:
        logger.error("too few jobs verified checked=%d seeded=%d", checked, seeded)
        return 1
    logger.info(
        "AUDIT CHAOS SOAK PASSED checked=%d seeded=%d completed=%s",
        checked,
        seeded,
        b.get("completed"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
