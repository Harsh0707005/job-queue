#!/usr/bin/env python3
"""Multi-tenant multi-worker fleet: spill large payloads off Postgres.

Stages
------
1. Enqueue a mix of large + tiny jobs while Postgres is starved
   (tiny shared_buffers). Workers drain. Assert:
     * every job completes
     * lease/get still see plaintext
     * large rows are blob pointers (small JSONB)
     * secrets never land in Postgres
2. Live DEK rotate + webhook delivery of a spilled payload.
3. Export / restore a spilled tenant; dest leases plaintext.
4. Crash leftovers (file without catalog, orphan catalog) GC cleanly.

  docker compose -f docker-compose.yml -f docker-compose.payload-blobs.yml \\
    up --build --abort-on-container-exit --exit-code-from payload-blobs-soak \\
    payload-blobs-soak
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
logger = logging.getLogger("payload_blob_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN") or ""
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "16") or "16")
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "49152") or "49152")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300") or "300")
SECRET_PREFIX = os.environ.get("SECRET_PREFIX", "blob-secret")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or ""
BLOB_DIR = os.environ.get("JOB_QUEUE_BLOB_DIR") or ""
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: int = 60,
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


def wait_coordinators() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def connect_queue():
    from job_queue import JobQueue

    if not DSN:
        raise RuntimeError("JOB_QUEUE_DSN is required")
    q = JobQueue.connect(DSN, min_size=1, max_size=4)
    return q


def postgres_secret_hits(secrets: list[str]) -> list[str]:
    if not DSN:
        return []
    import psycopg
    from psycopg.rows import dict_row

    hits: list[str] = []
    with psycopg.connect(DSN, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT payload::text AS t FROM jobs
            UNION ALL
            SELECT COALESCE(result::text, '') FROM jobs
            UNION ALL
            SELECT payload::text FROM webhook_deliveries
            UNION ALL
            SELECT payload::text FROM schedules
            """
        ).fetchall()
        blob = "\n".join(r["t"] or "" for r in rows)
    for s in secrets:
        if s in blob:
            hits.append(s)
    return hits


def blob_files_secret_hits(secrets: list[str]) -> list[str]:
    if not BLOB_DIR or not os.path.isdir(BLOB_DIR):
        return []
    hits: list[str] = []
    found: list[str] = []
    for root, _dirs, files in os.walk(BLOB_DIR):
        for name in files:
            if not name.endswith(".bin"):
                continue
            path = os.path.join(root, name)
            try:
                data = open(path, "rb").read()
            except OSError:
                continue
            text = data.decode("ascii", errors="ignore")
            found.append(text)
    blob = "\n".join(found)
    for s in secrets:
        if s in blob:
            hits.append(s)
    return hits


def stage1_enqueue_and_drain(q) -> tuple[list[str], int, list[str]]:
    from job_queue.crypto import is_blob_pointer

    pad = "X" * PAYLOAD_BYTES
    secrets: list[str] = []
    large_ids: list[str] = []
    expected = 0
    for i in range(TENANTS):
        tid = f"pblob-{i:02d}"
        for n in range(JOBS):
            secret = f"{SECRET_PREFIX}-{tid}-{n}"
            secrets.append(secret)
            large = n % 4 != 0
            if large:
                payload = {
                    "kind": "blob-soak",
                    "secret": secret,
                    "blob": pad,
                    "ack_result": {"echo": secret, "blob": pad},
                }
            else:
                payload = {
                    "kind": "blob-soak",
                    "secret": secret,
                    "tiny": True,
                    "ack_result": {"echo": secret},
                }
            created = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": payload,
                    "queue_name": "default",
                },
            )
            jid = (created.get("job") or created).get("id")
            if large and jid:
                large_ids.append(str(jid))
            expected += 1

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"stage1 postgres stored plaintext: {leaked[:3]}")

    # Stored large jobs must be pointers, and tiny JSONB.
    with q._pool.connection() as conn:
        for jid in large_ids:
            row = conn.execute(
                """
                SELECT payload, pg_column_size(payload) AS nbytes
                FROM jobs WHERE id = %s
                """,
                (jid,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"missing job {jid}")
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not is_blob_pointer(payload):
                raise RuntimeError(f"large job {jid} was not spilled: {payload}")
            if int(row["nbytes"] or 0) > 4096:
                raise RuntimeError(
                    f"spilled job {jid} still fat in postgres: {row['nbytes']}"
                )

    catalog = 0
    jobs_bytes = 0
    with q._pool.connection() as conn:
        catalog = int(
            conn.execute("SELECT COUNT(*) AS n FROM job_payload_blobs").fetchone()["n"]
        )
        jobs_bytes = int(
            conn.execute(
                "SELECT COALESCE(pg_total_relation_size('jobs'), 0) AS n"
            ).fetchone()["n"]
        )
    if catalog < len(large_ids):
        raise RuntimeError(
            f"catalog under-counted spills catalog={catalog} large={len(large_ids)}"
        )
    blob_bytes = 0
    if BLOB_DIR and os.path.isdir(BLOB_DIR):
        for root, _dirs, files in os.walk(BLOB_DIR):
            for name in files:
                if name.endswith(".bin"):
                    try:
                        blob_bytes += os.path.getsize(os.path.join(root, name))
                    except OSError:
                        pass
    # Inline 48KB * N jobs would roughly match blob_bytes; spilled jobs
    # must keep the heap far smaller than the ciphertext on disk.
    if large_ids and blob_bytes > 0 and jobs_bytes > blob_bytes:
        raise RuntimeError(
            f"postgres jobs table {jobs_bytes}b exceeds blob store {blob_bytes}b"
        )
    logger.info(
        "stage1 enqueued expected=%d large=%d catalog=%d jobs_table=%d blob_bytes=%d",
        expected,
        len(large_ids),
        catalog,
        jobs_bytes,
        blob_bytes,
    )

    deadline = time.time() + MAX_WALL
    completed = 0
    while time.time() < deadline:
        bl = backlog()
        completed = int(bl.get("completed") or 0)
        pending = int(bl.get("pending") or 0)
        leased = int(bl.get("leased") or 0)
        logger.info(
            "stage1 progress completed=%d pending=%d leased=%d expected=%d",
            completed,
            pending,
            leased,
            expected,
        )
        if completed >= expected and pending == 0 and leased == 0:
            break
        time.sleep(2)
    if completed < expected:
        raise RuntimeError(f"stage1 only completed {completed}/{expected}")

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"stage1 plaintext after drain: {leaked[:3]}")
    file_hits = blob_files_secret_hits(secrets)
    if file_hits:
        raise RuntimeError(f"stage1 plaintext in blob files: {file_hits[:3]}")

    # Workers only ever saw plaintext after lease: completed jobs decrypt.
    sample_tid = "pblob-00"
    jobs = q.list_jobs(tenant_id=sample_tid, status="completed", limit=JOBS)
    if len(jobs) < JOBS:
        raise RuntimeError(f"{sample_tid} completed {len(jobs)}/{JOBS}")
    for job in jobs:
        got = q.get(job.id, tenant_id=sample_tid)
        if got is None or not str(got.payload.get("secret") or "").startswith(
            SECRET_PREFIX
        ):
            raise RuntimeError(f"completed job {job.id} lost plaintext payload")
        echo = (got.result or {}).get("echo") if isinstance(got.result, dict) else None
        if echo != got.payload.get("secret"):
            raise RuntimeError(f"completed job {job.id} missing echoed result")
    return secrets, expected, large_ids


def stage2_rotate_and_webhooks(q, secrets: list[str]) -> None:
    for i in range(TENANTS):
        tid = f"pblob-{i:02d}"
        rot = http_json("POST", f"/v1/tenants/{tid}/payload-keys/rotate", {})
        status = (rot.get("rotation") or {}).get("status")
        logger.info("stage2 rotate %s status=%s", tid, status)
        if status != "reencrypting":
            raise RuntimeError(f"rotate {tid} unexpected {rot}")

    deadline = time.time() + min(120, MAX_WALL)
    while time.time() < deadline:
        try:
            http_json(
                "POST",
                "/v1/admin/payload-keys/advance",
                {"limit": TENANTS, "batch_size": 50},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("advance: %s", exc)
        done = 0
        for i in range(TENANTS):
            tid = f"pblob-{i:02d}"
            info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")
            rot = info.get("rotation") or {}
            if rot.get("status") in (None, "complete"):
                done += 1
        if done == TENANTS:
            break
        time.sleep(1)
    else:
        raise RuntimeError("stage2 rotations did not complete")

    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"stage2 plaintext after rotate: {leaked[:3]}")

    sample = q.get(
        q.list_jobs(tenant_id="pblob-00", status="completed", limit=1)[0].id,
        tenant_id="pblob-00",
    )
    if sample is None or SECRET_PREFIX not in str(sample.payload.get("secret")):
        raise RuntimeError("stage2 post-rotate decrypt failed")

    if not WEBHOOK_URL:
        logger.info("stage2 skip webhooks (no WEBHOOK_URL)")
        return
    from job_queue import WebhookEvent

    hook_tid = "pblob-wh"
    q.register_webhook(
        tenant_id=hook_tid,
        url=WEBHOOK_URL,
        events=[WebhookEvent.JOB_COMPLETED.value],
        secret="blob-hook",
    )
    secret = f"{SECRET_PREFIX}-webhook"
    secrets.append(secret)
    pad = "Y" * PAYLOAD_BYTES
    http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": hook_tid,
            "payload": {
                "kind": "blob-soak",
                "secret": secret,
                "blob": pad,
                "ack_result": {"echo": secret, "blob": pad},
            },
        },
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        bl = q.list_jobs(tenant_id=hook_tid, status="completed", limit=5)
        if bl:
            break
        time.sleep(1)
    else:
        raise RuntimeError("stage2 webhook job did not complete")
    # Drive outbox (coordinator also ticks; do one pass ourselves).
    for _ in range(20):
        q.process_webhook_deliveries(limit=20)
        delivered = q.list_webhook_deliveries(tenant_id=hook_tid, status="succeeded")
        if delivered:
            body = delivered[0].payload
            if body.get("payload", {}).get("secret") != secret:
                raise RuntimeError(f"webhook payload not plaintext: {body}")
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("stage2 webhook never succeeded")
    leaked = postgres_secret_hits([secret])
    if leaked:
        raise RuntimeError(f"stage2 webhook leaked plaintext: {leaked}")
    logger.info("stage2 rotate + webhook OK")


def stage3_export_restore(q, secrets: list[str]) -> None:
    src = "pblob-00"
    dst = "pblob-restore"
    snap = q.export_tenant(src)
    if snap.status != "ready":
        raise RuntimeError(f"export not ready: {snap}")
    if int(snap.counts.get("jobs") or 0) < 1:
        raise RuntimeError(f"export missed jobs: {snap.counts}")
    if int(snap.counts.get("payload_blobs") or 0) < 1:
        raise RuntimeError(f"export missed blobs: {snap.counts}")
    rst = q.restore_tenant(
        dst, snapshot_id=snap.id, batch_size=40, run_until_complete=True
    )
    if rst.status != "complete":
        raise RuntimeError(f"restore failed: {rst.status} {rst.error}")
    restored = q.list_jobs(tenant_id=dst, limit=JOBS + 5)
    if len(restored) < JOBS:
        raise RuntimeError(f"restore dest has {len(restored)} jobs, want {JOBS}")
    sample = q.get(restored[0].id, tenant_id=dst)
    if sample is None or not str(sample.payload.get("secret") or "").startswith(
        SECRET_PREFIX
    ):
        raise RuntimeError("restored job did not decrypt")
    from job_queue.crypto import is_blob_pointer

    with q._pool.connection() as conn:
        row = conn.execute(
            "SELECT payload FROM jobs WHERE id = %s",
            (restored[0].id,),
        ).fetchone()
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
    if sample.payload.get("tiny"):
        pass
    elif not is_blob_pointer(payload):
        # tiny jobs stay inline; large must still spill after reseal
        if "blob" in sample.payload and not is_blob_pointer(payload):
            raise RuntimeError("restored large job stayed inline")
    leaked = postgres_secret_hits(secrets)
    if leaked:
        raise RuntimeError(f"stage3 plaintext after restore: {leaked[:3]}")
    logger.info("stage3 export/restore OK dest_jobs=%d", len(restored))


def stage4_crash_gc(q) -> None:
    from job_queue.blob_store import FilesystemBlobStore

    if not BLOB_DIR:
        raise RuntimeError("JOB_QUEUE_BLOB_DIR required for crash GC")
    store = FilesystemBlobStore(BLOB_DIR)
    leftover = uuid4()
    store.write(leftover, b"ciphertext-not-plaintext-leftover")
    path = store.path_for(leftover)
    past = time.time() - 120
    os.utime(path, (past, past))
    gc = q.process_payload_blobs(limit=500)
    logger.info("stage4 gc %s", gc)
    if store.exists(leftover):
        raise RuntimeError("unreferenced leftover file survived GC")
    # Orphan catalog + file.
    planted = uuid4()
    store.write(planted, b"orphan-cipher")
    with q._pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO job_payload_blobs (
                id, tenant_id, job_id, kind, sha256, nbytes, key_id,
                status, created_at, committed_at
            ) VALUES (
                %s, 'pblob-gc', NULL, 'other', 'x', 16, NULL,
                'orphan', NOW(), NOW()
            )
            """,
            (planted,),
        )
        conn.commit()
    gc = q.process_payload_blobs(limit=500)
    if store.exists(planted):
        raise RuntimeError("orphan catalog file survived GC")
    with q._pool.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM job_payload_blobs WHERE id = %s",
            (planted,),
        ).fetchone()["n"]
    if int(n) != 0:
        raise RuntimeError("orphan catalog row survived GC")
    logger.info("stage4 crash GC OK deleted=%s unreferenced=%s", gc.get("deleted"), gc.get("unreferenced"))


def main() -> int:
    logger.info(
        "payload blob soak tenants=%d jobs=%d bytes=%d coords=%s",
        TENANTS,
        JOBS,
        PAYLOAD_BYTES,
        ",".join(COORDS),
    )
    wait_coordinators()
    q = connect_queue()
    try:
        secrets, expected, large_ids = stage1_enqueue_and_drain(q)
        logger.info(
            "stage1 OK expected=%d large=%d", expected, len(large_ids)
        )
        stage2_rotate_and_webhooks(q, secrets)
        stage3_export_restore(q, secrets)
        stage4_crash_gc(q)
    finally:
        q.close()
    logger.info("payload blob soak OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("payload blob soak failed")
        sys.exit(1)
