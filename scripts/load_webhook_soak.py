#!/usr/bin/env python3
"""Webhook claim/finalize load: pre-queue thousands of deliveries, measure drain.

Seeds pending webhook_deliveries across TENANTS (via DSN), then waits for
webhook agents to claim+POST+finalize until pending+delivering == 0.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any
from uuid import uuid4

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("load_webhook_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DSN = (
    os.environ.get("JOB_QUEUE_DSN")
    or os.environ.get("DATABASE_URL")
    or "postgresql://queue:queue@postgres:5432/queue"
)
SINK_URL = os.environ.get("SINK_URL", "http://webhook-sink:8090").rstrip("/")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")

TENANTS = int(os.environ.get("TENANTS", "20"))
DELIVERIES_PER_TENANT = int(os.environ.get("DELIVERIES_PER_TENANT", "200"))  # 4000
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "600"))
POLL = float(os.environ.get("POLL_SECONDS", "0.5"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/results/webhook-baseline.json")
LABEL = os.environ.get("LOAD_LABEL", "webhook-baseline")
SEED_BATCH = int(os.environ.get("SEED_BATCH", "500"))

_rr = 0


def http_json(method: str, path: str | None = None, body: dict | None = None, *, absolute: str | None = None) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDINATOR_URLS)
        _rr += 1
        bases = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_ready() -> None:
    for _ in range(120):
        ok = 0
        for b in COORDINATOR_URLS:
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    ok += 1
            except Exception:
                pass
        try:
            http_json("GET", absolute=SINK_URL + "/health")
            sink = True
        except Exception:
            sink = False
        if ok >= 1 and sink:
            logger.info("ready coords=%d/%d sink=ok", ok, len(COORDINATOR_URLS))
            return
        time.sleep(1)
    raise RuntimeError("not ready")


def backlog() -> dict[str, Any]:
    return http_json("GET", "/v1/admin/backlog")


def seed_deliveries() -> int:
    """Register endpoints + bulk-insert pending deliveries via JobQueue/psycopg."""
    from job_queue import JobQueue
    from job_queue.db import transaction
    from psycopg.types.json import Jsonb

    total = TENANTS * DELIVERIES_PER_TENANT
    tenants = [f"wh-t{i:02d}" for i in range(TENANTS)]
    q = JobQueue.connect(DSN, min_size=1, max_size=4)
    try:
        # Clean leftover from prior runs (same tenant prefix).
        with transaction(q._pool) as conn:
            conn.execute(
                "DELETE FROM webhook_deliveries WHERE tenant_id LIKE 'wh-t%%'"
            )
            conn.execute(
                "DELETE FROM webhook_endpoints WHERE tenant_id LIKE 'wh-t%%'"
            )

        endpoint_ids: dict[str, Any] = {}
        for tid in tenants:
            ep = q.register_webhook(
                tenant_id=tid,
                url=WEBHOOK_URL,
                events=["job.completed"],
                secret=f"sec-{tid}",
                max_attempts=5,
            )
            endpoint_ids[tid] = ep.id
            logger.info("endpoint %s -> %s", tid, ep.id)

        now = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        )
        inserted = 0
        # Batch inserts for speed.
        rows_buf: list[tuple] = []
        for tid in tenants:
            eid = endpoint_ids[tid]
            for i in range(DELIVERIES_PER_TENANT):
                did = uuid4()
                jid = uuid4()
                payload = {
                    "event": "job.completed",
                    "tenant_id": tid,
                    "job_id": str(jid),
                    "delivery_id": str(did),
                    "i": i,
                }
                rows_buf.append(
                    (
                        did,
                        eid,
                        tid,
                        jid,
                        "job.completed",
                        Jsonb(payload),
                        "pending",
                        0,
                        5,
                        1,
                        60,
                        now,  # next_attempt_at due now
                        None,
                        None,
                        None,
                        now,
                        now,
                        None,
                    )
                )
                if len(rows_buf) >= SEED_BATCH:
                    _flush_seed(q, rows_buf)
                    inserted += len(rows_buf)
                    rows_buf.clear()
                    if inserted % 1000 == 0:
                        logger.info("seeded %d/%d", inserted, total)
        if rows_buf:
            _flush_seed(q, rows_buf)
            inserted += len(rows_buf)
        logger.info("seed complete %d deliveries", inserted)
        return inserted
    finally:
        q.close()


def _flush_seed(q: Any, rows: list[tuple]) -> None:
    from job_queue.db import transaction

    sql = """
            INSERT INTO webhook_deliveries (
              id, endpoint_id, tenant_id, job_id, event, payload,
              status, attempts, max_attempts, backoff_base_seconds, backoff_max_seconds,
              next_attempt_at, delivering_until, last_error, last_status_code,
              created_at, updated_at, completed_at
            ) VALUES (
              %s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,%s
            )
            """
    with transaction(q._pool) as conn:
        # psycopg3: executemany lives on the cursor, not Connection.
        with conn.cursor() as cur:
            cur.executemany(sql, rows)


def main() -> int:
    total = TENANTS * DELIVERIES_PER_TENANT
    logger.info(
        "WEBHOOK load label=%s tenants=%d per=%d total=%d coords=%s",
        LABEL,
        TENANTS,
        DELIVERIES_PER_TENANT,
        total,
        COORDINATOR_URLS,
    )
    wait_ready()
    t0 = time.perf_counter()
    seeded = seed_deliveries()
    t_seed = time.perf_counter() - t0
    bl = backlog()
    logger.info("pre-drain backlog %s seed_s=%.2f", bl, t_seed)

    t_drain0 = time.perf_counter()
    samples: list[dict[str, Any]] = []
    last_succ = 0
    first_success_s: float | None = None

    while True:
        elapsed = time.perf_counter() - t_drain0
        if elapsed > MAX_WALL:
            logger.error("timeout: %s", backlog())
            _write(
                {
                    "label": LABEL,
                    "ok": False,
                    "error": "timeout",
                    "total": total,
                    "seeded": seeded,
                    "seed_seconds": t_seed,
                    "drain_seconds": elapsed,
                    "samples": samples,
                    "final_backlog": backlog(),
                }
            )
            return 1
        try:
            bl = backlog()
        except Exception as exc:  # noqa: BLE001
            logger.warning("backlog: %s", exc)
            time.sleep(POLL)
            continue
        wh = bl.get("webhooks") or {}
        pending = int(wh.get("pending") or 0)
        delivering = int(wh.get("delivering") or 0)
        succeeded = int(wh.get("succeeded") or 0)
        dead = int(wh.get("dead") or 0)
        if first_success_s is None and succeeded > 0:
            first_success_s = elapsed
        delta = succeeded - last_succ
        last_succ = succeeded
        samples.append(
            {
                "t": round(elapsed, 3),
                "pending": pending,
                "delivering": delivering,
                "succeeded": succeeded,
                "dead": dead,
                "inst_succ_per_sec": round(delta / POLL, 2) if POLL else 0,
            }
        )
        logger.info(
            "drain t=%.1fs pending=%d delivering=%d succ=%d/%d dead=%d ~%.0f succ/s",
            elapsed,
            pending,
            delivering,
            succeeded,
            total,
            dead,
            succeeded / elapsed if elapsed > 0 else 0,
        )
        if pending == 0 and delivering == 0 and (succeeded + dead) >= total:
            time.sleep(0.5)
            bl2 = backlog()
            w2 = bl2.get("webhooks") or {}
            if int(w2.get("pending") or 0) == 0 and int(w2.get("delivering") or 0) == 0:
                break
        time.sleep(POLL)

    drain_s = time.perf_counter() - t_drain0
    thr = total / drain_s if drain_s > 0 else 0
    result = {
        "label": LABEL,
        "ok": True,
        "tenants": TENANTS,
        "deliveries_per_tenant": DELIVERIES_PER_TENANT,
        "total": total,
        "seeded": seeded,
        "seed_seconds": round(t_seed, 4),
        "drain_seconds": round(drain_s, 4),
        "drain_deliveries_per_sec": round(thr, 2),
        "time_to_first_success_seconds": (
            round(first_success_s, 4) if first_success_s is not None else None
        ),
        "final_backlog": backlog(),
        "samples": samples,
    }
    _write(result)
    logger.info(
        "WEBHOOK LOAD PASSED label=%s total=%d drain=%.2fs (%.0f/s) ttf=%.2fs",
        LABEL,
        total,
        drain_s,
        thr,
        first_success_s or -1,
    )
    print(
        "RESULT "
        + json.dumps(
            {
                "label": LABEL,
                "ok": True,
                "total": total,
                "drain_seconds": result["drain_seconds"],
                "drain_deliveries_per_sec": result["drain_deliveries_per_sec"],
                "time_to_first_success_seconds": result[
                    "time_to_first_success_seconds"
                ],
            }
        ),
        flush=True,
    )
    return 0


def _write(payload: dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
        with open(RESULTS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info("wrote %s", RESULTS_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.warning("write results: %s", exc)


if __name__ == "__main__":
    raise SystemExit(main())
