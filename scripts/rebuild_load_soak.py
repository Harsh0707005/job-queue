#!/usr/bin/env python3
"""Docker fleet load test: lose job rows, rebuild from audit at scale.

1. Seed many tenants/jobs; let workers run to mix of terminal states.
2. DELETE a large fraction of jobs rows (keep audit).
3. Discover orphans via API; rebuild in batches across coordinators.
4. Verify every rebuilt job: row exists, audit verifies, checkpoint/status ok.
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
from uuid import UUID

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rebuild_load_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get(
    "JOB_QUEUE_DSN",
    "postgresql://queue:queue@postgres:5432/queue",
)
TENANTS = int(os.environ.get("TENANTS", "10"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "30"))
DELETE_FRACTION = float(os.environ.get("DELETE_FRACTION", "0.6"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300"))
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
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
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def backlog() -> dict[str, int]:
    bl = http_json("GET", "/v1/admin/backlog")
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    logger.info(
        "rebuild load soak tenants=%d jobs/tenant=%d total=%d delete_frac=%.2f",
        TENANTS,
        JOBS_PER_TENANT,
        total,
        DELETE_FRACTION,
    )
    wait_ready()

    # Seed
    def seed(t: int) -> int:
        tid = f"rb-{t:02d}"
        n = 0
        for i in range(JOBS_PER_TENANT):
            payload: dict[str, Any]
            if i % 4 == 0:
                payload = {
                    "kind": "steps",
                    "steps": 3,
                    "step_ms": 40,
                    "approval_after_step": 1,
                    "approval_timeout_seconds": 5,
                }
            elif i % 2 == 0:
                payload = {"kind": "steps", "steps": 2 + i % 3, "step_ms": 30}
            else:
                payload = {"sleep_ms": 20 + i % 30, "i": i}
            try:
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": payload,
                        "priority": i % 4,
                        "max_attempts": 5,
                        "visibility_timeout_seconds": 15,
                        "retry_backoff_base_seconds": 0,
                    },
                )
                n += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("seed fail %s: %s", tid, exc)
        return n

    seeded = 0
    with ThreadPoolExecutor(max_workers=min(8, TENANTS)) as pool:
        for f in as_completed([pool.submit(seed, t) for t in range(TENANTS)]):
            seeded += f.result()
    logger.info("seeded %d", seeded)

    # Approve half of waiting jobs while work runs.
    started = time.monotonic()
    while time.monotonic() - started < min(90, MAX_WALL // 2):
        for t in range(TENANTS):
            tid = f"rb-{t:02d}"
            try:
                http_json(
                    "POST",
                    "/v1/approvals/process-timeouts",
                    {"tenant_id": tid},
                )
                listing = http_json("GET", f"/v1/approvals?tenant_id={tid}&limit=30")
                for j in listing.get("jobs") or []:
                    if hash(j["id"]) % 2 == 0:
                        try:
                            http_json(
                                "POST",
                                f"/v1/jobs/{j['id']}/approve",
                                {"tenant_id": tid},
                            )
                        except Exception:
                            pass
            except Exception:
                pass
        b = backlog()
        logger.info("drain %s", b)
        if int(b.get("pending") or 0) + int(b.get("leased") or 0) < max(5, seeded // 20):
            if int(b.get("leased") or 0) == 0:
                break
        time.sleep(1.5)

    # Final timeout sweep
    for t in range(TENANTS):
        try:
            http_json(
                "POST",
                "/v1/approvals/process-timeouts",
                {"tenant_id": f"rb-{t:02d}", "limit": 200},
            )
        except Exception:
            pass
    time.sleep(2)
    pre = backlog()
    logger.info("pre-delete backlog %s", pre)

    # Direct DB: delete a fraction of job rows (simulate bad migrate).
    try:
        import psycopg
    except ImportError:
        logger.error("psycopg not available in soak image")
        return 1

    deleted = 0
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, tenant_id FROM jobs
                ORDER BY created_at
                """
            )
            rows = cur.fetchall()
            victims = [r for i, r in enumerate(rows) if (i % 100) < int(DELETE_FRACTION * 100)]
            for jid, _tid in victims:
                cur.execute("DELETE FROM jobs WHERE id = %s", (jid,))
                deleted += 1
        conn.commit()
    logger.info("deleted %d job rows (audit kept)", deleted)

    # Discover orphans via API
    all_orphans: list[str] = []
    for t in range(TENANTS):
        tid = f"rb-{t:02d}"
        try:
            o = http_json("GET", f"/v1/audit/orphans?tenant_id={tid}&limit=5000")
            all_orphans.extend(o.get("job_ids") or [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("orphans %s: %s", tid, exc)
    # Also global orphans
    try:
        o = http_json("GET", "/v1/audit/orphans?limit=10000")
        for jid in o.get("job_ids") or []:
            if jid not in all_orphans:
                all_orphans.append(jid)
    except Exception as exc:  # noqa: BLE001
        logger.warning("global orphans: %s", exc)

    logger.info("orphans discovered=%d deleted=%d", len(all_orphans), deleted)
    if len(all_orphans) < deleted * 0.9:
        logger.error("orphan count too low vs deleted")
        return 1

    # Rebuild in parallel batches
    rebuilt = 0
    failed = 0

    def rebuild_one(jid: str) -> tuple[str, bool, str]:
        try:
            # tenant from last audit via rebuild without tenant first
            r = http_json(
                "POST",
                f"/v1/jobs/{jid}/rebuild",
                {},
                timeout=45,
            )
            job = r.get("job") or {}
            return jid, True, job.get("status") or ""
        except Exception as exc:  # noqa: BLE001
            return jid, False, str(exc)

    batch = 32
    for i in range(0, len(all_orphans), batch):
        chunk = all_orphans[i : i + batch]
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = [pool.submit(rebuild_one, jid) for jid in chunk]
            for f in as_completed(futs):
                jid, ok, info = f.result()
                if ok:
                    rebuilt += 1
                else:
                    failed += 1
                    logger.warning("rebuild fail %s: %s", jid, info[:200])
        logger.info("rebuild progress %d/%d failed=%d", rebuilt + failed, len(all_orphans), failed)

    # Verify no orphans left and audit matches
    remaining = http_json("GET", "/v1/audit/orphans?limit=100")
    rem_n = int(remaining.get("count") or 0)
    logger.info("orphans remaining=%d rebuilt=%d failed=%d", rem_n, rebuilt, failed)

    mismatches = 0
    checked = 0
    for t in range(TENANTS):
        tid = f"rb-{t:02d}"
        try:
            rep = http_json(
                "GET",
                f"/v1/tenants/{tid}/audit/verify?limit=20000",
                timeout=60,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("verify %s: %s", tid, exc)
            mismatches += 1
            continue
        checked += int(rep.get("checked") or 0)
        if not rep.get("ok"):
            mismatches += int(rep.get("mismatches") or 0)
            logger.error("tenant %s mismatches %s", tid, (rep.get("reports") or [])[:2])
        else:
            logger.info("tenant %s audit ok checked=%s", tid, rep.get("checked"))

    if failed > 0 or rem_n > 0 or mismatches > 0:
        logger.error(
            "REBUILD LOAD SOAK FAILED failed=%d orphans=%d mismatches=%d",
            failed,
            rem_n,
            mismatches,
        )
        return 1
    if rebuilt < deleted * 0.9:
        logger.error("rebuilt too few %d < %d", rebuilt, int(deleted * 0.9))
        return 1
    logger.info(
        "REBUILD LOAD SOAK PASSED rebuilt=%d checked=%d seeded=%d",
        rebuilt,
        checked,
        seeded,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
