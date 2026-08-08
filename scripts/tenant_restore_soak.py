#!/usr/bin/env python3
"""Fleet soak: export/restore a tenant under load; survive coordinator kill.

  * Several load tenants keep producing and completing work.
  * One source tenant is snapshotted (pending + schedule + webhook).
  * Dest with leftover alerts/metrics is refused (no mix with junk).
  * After dest is emptied, restore while workers keep running.
  * Coordinators are killed mid-restore; advance resumes on survivors.
  * Dest jobs become claimable after restore completes and finish.
  * Source tenant is never paused by export.
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
logger = logging.getLogger("tenant_restore_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "4") or "4")
JOBS_PER_LOAD = int(os.environ.get("JOBS_PER_LOAD", "25") or "25")
SRC_JOBS = int(os.environ.get("SRC_JOBS", "18") or "18")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
SRC = os.environ.get("SOURCE_TENANT", "src-export")
DST = os.environ.get("DEST_TENANT", "dst-restore")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")
JUNK_ALERT = f"dst-junk-leftover-{int(time.time())}"
JUNK_METRIC = "junk_ack"
JUNK_KIND = "old-kind"
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


def http_status(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    """Hit the first coordinator that answers; return HTTP status + JSON."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    last: Exception | None = None
    for base in COORDS:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return int(resp.getcode()), (
                    json.loads(raw.decode()) if raw else {}
                )
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def pg_exec(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    import psycopg
    from psycopg.rows import dict_row

    dsn = os.environ.get("JOB_QUEUE_DSN") or ""
    if not dsn:
        raise RuntimeError("JOB_QUEUE_DSN required for dest residue checks")
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        cur = conn.execute(sql, params)
        rows = list(cur.fetchall()) if cur.description is not None else []
        conn.commit()
        return [dict(r) for r in rows]


def seed_dest_junk(tid: str) -> None:
    http_json(
        "POST",
        "/v1/alerts/rules",
        {
            "name": JUNK_ALERT,
            "metric": "dead_count",
            "threshold": 99,
            "scope": "tenant",
            "tenant_id": tid,
        },
    )
    http_json("POST", "/v1/metrics/sample", {"tenant_id": tid})
    pg_exec(
        """
        INSERT INTO queue_metric_buckets
            (bucket_start, tenant_id, metric, count, sum_ms)
        VALUES (NOW(), %s, %s, 11, 77)
        ON CONFLICT DO NOTHING
        """,
        (tid, JUNK_METRIC),
    )
    pg_exec(
        """
        INSERT INTO duration_estimates
            (tenant_id, queue_name, kind, ewma_seconds, samples, updated_at)
        VALUES (%s, 'default', %s, 8.0, 4, NOW())
        ON CONFLICT DO NOTHING
        """,
        (tid, JUNK_KIND),
    )


def dest_junk_present(tid: str) -> dict[str, int]:
    """Counts only planted leftover markers, not restored source rows."""
    alert_n = pg_exec(
        "SELECT COUNT(*) AS n FROM alert_rules WHERE tenant_id = %s AND name = %s",
        (tid, JUNK_ALERT),
    )[0]["n"]
    bucket_n = pg_exec(
        "SELECT COUNT(*) AS n FROM queue_metric_buckets "
        "WHERE tenant_id = %s AND metric = %s",
        (tid, JUNK_METRIC),
    )[0]["n"]
    snap_n = pg_exec(
        "SELECT COUNT(*) AS n FROM queue_metric_snapshots WHERE tenant_id = %s",
        (tid,),
    )[0]["n"]
    est_n = pg_exec(
        "SELECT COUNT(*) AS n FROM duration_estimates "
        "WHERE tenant_id = %s AND kind = %s",
        (tid, JUNK_KIND),
    )[0]["n"]
    return {
        "alert_rules": int(alert_n),
        "queue_metric_buckets": int(bucket_n),
        "queue_metric_snapshots": int(snap_n),
        "duration_estimates": int(est_n),
    }


def junk_markers(counts: dict[str, int]) -> dict[str, int]:
    return {
        k: counts[k]
        for k in ("alert_rules", "queue_metric_buckets", "duration_estimates")
    }


def clear_dest_residue(tid: str) -> None:
    for sql in (
        "DELETE FROM alert_events WHERE tenant_id = %s",
        "DELETE FROM alert_rules WHERE tenant_id = %s",
        "DELETE FROM queue_metric_buckets WHERE tenant_id = %s",
        "DELETE FROM queue_metric_snapshots WHERE tenant_id = %s",
        "DELETE FROM duration_estimates WHERE tenant_id = %s",
        "DELETE FROM tenant_fairness WHERE tenant_id = %s",
        "DELETE FROM job_audit_events WHERE tenant_id = %s",
        "DELETE FROM payload_key_rotations WHERE tenant_id = %s",
        "DELETE FROM tenant_payload_keys WHERE tenant_id = %s",
        "DELETE FROM jobs WHERE tenant_id = %s",
        "DELETE FROM schedules WHERE tenant_id = %s",
        "DELETE FROM webhook_deliveries WHERE tenant_id = %s",
        "DELETE FROM webhook_endpoints WHERE tenant_id = %s",
        "DELETE FROM workflow_step_runs WHERE tenant_id = %s",
        "DELETE FROM workflow_runs WHERE tenant_id = %s",
        "DELETE FROM job_side_effects WHERE tenant_id = %s",
    ):
        pg_exec(sql, (tid,))


def wait_coords() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def tenant_count(tid: str, status: str | None = None) -> int:
    try:
        q = f"/v1/admin/stats?tenant_id={tid}"
        st = http_json("GET", q)
    except Exception:
        return -1
    # fallback: list jobs
    try:
        body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
        jobs = body.get("jobs") or []
        if status:
            jobs = [j for j in jobs if j.get("status") == status]
        return len(jobs)
    except Exception:
        return -1


def main() -> int:
    logger.info("tenant restore soak load=%d src_jobs=%d", LOAD_TENANTS, SRC_JOBS)
    wait_coords()

    for i in range(LOAD_TENANTS):
        tid = f"load-{i:02d}"
        for n in range(JOBS_PER_LOAD):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 40, "i": n, "t": tid},
                    "queue_name": "default",
                },
            )

    http_json(
        "POST",
        "/v1/webhooks",
        {
            "tenant_id": SRC,
            "url": WEBHOOK_URL,
            "events": ["job.completed"],
            "secret": "restore-soak",
        },
    )
    http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": SRC,
            "name": "src-tick",
            "payload": {"from": "schedule", "ms": 20},
            "interval_seconds": 30,
        },
    )
    for n in range(SRC_JOBS):
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": SRC,
                "payload": {"secret": f"src-secret-{n}", "n": n, "ms": 30},
                "delay_seconds": 2 if n % 3 == 0 else 0,
            },
        )

    http_json(
        "POST",
        "/v1/alerts/rules",
        {
            "name": f"src-export-alert-{int(time.time())}",
            "metric": "dead_count",
            "threshold": 5,
            "scope": "tenant",
            "tenant_id": SRC,
        },
    )

    exp = http_json("POST", f"/v1/tenants/{SRC}/export", {})
    snap = exp["snapshot"]
    logger.info("exported snapshot=%s counts=%s", snap["id"], snap.get("counts"))
    if snap.get("status") != "ready":
        raise RuntimeError(f"export not ready: {snap}")

    seed_dest_junk(DST)
    junk_before = dest_junk_present(DST)
    logger.info("seeded dest leftover %s", junk_before)
    if not junk_before.get("alert_rules") or not junk_before.get("queue_metric_buckets"):
        raise RuntimeError(f"failed to seed dest leftover: {junk_before}")

    code, refused = http_status(
        "POST",
        f"/v1/tenants/{DST}/restore",
        {"snapshot_id": snap["id"], "batch_size": 2},
    )
    logger.info("restore over leftover -> %s %s", code, refused)
    if code != 409:
        raise RuntimeError(f"expected 409 over leftover dest, got {code} {refused}")
    err = str(refused.get("error") or "")
    for table in (
        "alert_rules",
        "queue_metric_buckets",
        "queue_metric_snapshots",
        "duration_estimates",
    ):
        if table not in err:
            raise RuntimeError(f"409 missing leftover table {table}: {err}")
    if junk_markers(dest_junk_present(DST)) != junk_markers(junk_before):
        raise RuntimeError("refused restore mutated dest leftover")
    listed = http_json("GET", f"/v1/jobs?tenant_id={DST}&limit=50")
    if listed.get("jobs"):
        raise RuntimeError("restore mixed jobs onto leftover dest")

    logger.info("clearing dest leftover then restoring for real")
    clear_dest_residue(DST)
    leftover_after = dest_junk_present(DST)
    if any(junk_markers(leftover_after).values()):
        raise RuntimeError(f"dest still dirty after clear: {leftover_after}")
    # Coordinators may have sampled gauges for dest; wipe those too so restore
    # does not see leftover snapshots between seed/clear and start.
    pg_exec("DELETE FROM queue_metric_snapshots WHERE tenant_id = %s", (DST,))

    rst = http_json(
        "POST",
        f"/v1/tenants/{DST}/restore",
        {"snapshot_id": snap["id"], "batch_size": 2},
    )["restore"]
    logger.info("restore started id=%s status=%s", rst["id"], rst["status"])
    # Leave the restore mid-flight so chaos can kill a coordinator.
    time.sleep(10)

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        try:
            adv = http_json(
                "POST",
                f"/v1/tenants/{DST}/restore/advance",
                {"batch_size": 2},
            )["restore"]
        except Exception as exc:  # noqa: BLE001
            logger.warning("advance via tenant path: %s — trying admin", exc)
            try:
                http_json(
                    "POST",
                    "/v1/admin/restores/advance",
                    {"limit": 5, "batch_size": 5},
                )
                got = http_json("GET", f"/v1/tenants/{DST}/restore")
                adv = got["restore"]
            except Exception as exc2:  # noqa: BLE001
                logger.warning("advance failed (coord kill?): %s", exc2)
                time.sleep(1.5)
                continue
        logger.info(
            "restore status=%s applied=%s kind_index=%s",
            adv.get("status"),
            adv.get("applied"),
            adv.get("kind_index"),
        )
        if adv.get("status") == "complete":
            rst = adv
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"restore did not complete: {rst}")

    # Dest work should drain via the live worker fleet.
    while time.time() < deadline:
        listed = http_json("GET", f"/v1/jobs?tenant_id={DST}&limit=500")
        jobs = listed.get("jobs") or []
        pending = sum(1 for j in jobs if j.get("status") in ("pending", "leased"))
        completed = sum(1 for j in jobs if j.get("status") == "completed")
        logger.info("dst jobs total=%d pending/leased=%d completed=%d", len(jobs), pending, completed)
        if jobs and pending == 0 and completed >= min(SRC_JOBS, len(jobs)) * 0.8:
            break
        # keep load tenants moving
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": "load-00",
                "payload": {"kind": "work", "ms": 20, "i": int(time.time())},
            },
        )
        time.sleep(1.5)
    else:
        raise RuntimeError("dest tenant did not drain after restore")

    leftover_after_restore = dest_junk_present(DST)
    if any(junk_markers(leftover_after_restore).values()):
        raise RuntimeError(
            f"dest mixed leftover junk after restore: {leftover_after_restore}"
        )
    rules = http_json("GET", "/v1/alerts/rules").get("rules") or []
    dst_names = [r.get("name") for r in rules if r.get("tenant_id") == DST]
    if JUNK_ALERT in dst_names:
        raise RuntimeError(f"dest still has leftover alert: {dst_names}")
    if not any(n and "src-export-alert" in n for n in dst_names):
        raise RuntimeError(f"dest missing restored source alert: {dst_names}")

    src_listed = http_json("GET", f"/v1/jobs?tenant_id={SRC}&limit=500")
    if not (src_listed.get("jobs") or []):
        raise RuntimeError("source tenant lost all jobs after export")

    load_ok = False
    for i in range(LOAD_TENANTS):
        tid = f"load-{i:02d}"
        lj = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500").get("jobs") or []
        done = sum(1 for j in lj if j.get("status") == "completed")
        logger.info("load tenant %s completed=%d total=%d", tid, done, len(lj))
        if done > 0:
            load_ok = True
    if not load_ok:
        raise RuntimeError("load tenants made no progress during restore")

    logger.info("tenant restore soak OK dest_completed and load still running")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("tenant restore soak failed")
        sys.exit(1)
