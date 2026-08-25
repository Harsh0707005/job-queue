#!/usr/bin/env python3
"""Fleet soak: restore spilled payloads onto a fresh blob volume under load.

  1. Load tenants keep completing small jobs.
  2. Source tenant enqueues large spilled jobs + a webhook.
  3. Export the source snapshot (ciphertext rides in the snapshot).
  4. Wipe the blob volume (fresh fleet) then restore into an empty dest.
  5. Dest leases, rotate, and webhooks must work — no missing-file loops.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("payload_blob_restore_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN") or ""
BLOB_DIR = os.environ.get("JOB_QUEUE_BLOB_DIR") or ""
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "3") or "3")
JOBS_PER_LOAD = int(os.environ.get("JOBS_PER_LOAD", "16") or "16")
SRC_JOBS = int(os.environ.get("SRC_JOBS", "12") or "12")
PAYLOAD_BYTES = int(os.environ.get("PAYLOAD_BYTES", "49152") or "49152")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300") or "300")
SRC = os.environ.get("SOURCE_TENANT", "pblob-src")
DST = os.environ.get("DEST_TENANT", "pblob-dst")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or ""
SECRET_PREFIX = os.environ.get("SECRET_PREFIX", "pbr-secret")
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
    return JobQueue.connect(DSN, min_size=1, max_size=4)


def wipe_blob_dir() -> int:
    if not BLOB_DIR or not os.path.isdir(BLOB_DIR):
        raise RuntimeError("JOB_QUEUE_BLOB_DIR required to simulate a fresh fleet")
    removed = 0
    for name in os.listdir(BLOB_DIR):
        path = os.path.join(BLOB_DIR, name)
        if os.path.isdir(path):
            shutil.rmtree(path)
            removed += 1
        elif name.endswith(".bin") or name.endswith(".tmp"):
            os.unlink(path)
            removed += 1
    return removed


def postgres_secret_hits(secrets: list[str]) -> list[str]:
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
            """
        ).fetchall()
        blob = "\n".join(r["t"] or "" for r in rows)
    for s in secrets:
        if s in blob:
            hits.append(s)
    return hits


def main() -> int:
    logger.info(
        "blob restore soak load=%d src_jobs=%d bytes=%d",
        LOAD_TENANTS,
        SRC_JOBS,
        PAYLOAD_BYTES,
    )
    wait_coordinators()
    q = connect_queue()
    pad = "Z" * PAYLOAD_BYTES
    secrets: list[str] = []
    try:
        for i in range(LOAD_TENANTS):
            tid = f"pbr-load-{i:02d}"
            for n in range(JOBS_PER_LOAD):
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": {"kind": "blob-soak", "ms": 20, "i": n},
                    },
                )

        if WEBHOOK_URL:
            from job_queue import WebhookEvent

            q.register_webhook(
                tenant_id=SRC,
                url=WEBHOOK_URL,
                events=[WebhookEvent.JOB_COMPLETED.value],
                secret="pbr-hook",
            )
        for n in range(SRC_JOBS):
            secret = f"{SECRET_PREFIX}-{n}"
            secrets.append(secret)
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": SRC,
                    "payload": {
                        "kind": "blob-soak",
                        "secret": secret,
                        "blob": pad,
                        "ack_result": {"echo": secret, "blob": pad},
                    },
                    "delay_seconds": 1 if n % 4 == 0 else 0,
                },
            )

        snap = q.export_tenant(SRC)
        if snap.status != "ready":
            raise RuntimeError(f"export not ready: {snap}")
        if int(snap.counts.get("jobs") or 0) < SRC_JOBS:
            raise RuntimeError(f"export missed jobs: {snap.counts}")
        if int(snap.counts.get("payload_blobs") or 0) < 1:
            raise RuntimeError(f"export missed blobs: {snap.counts}")
        logger.info("exported %s counts=%s", snap.id, snap.counts)

        wiped = wipe_blob_dir()
        logger.info("wiped blob dir entries=%d (fresh fleet)", wiped)

        rst = q.restore_tenant(
            DST, snapshot_id=snap.id, batch_size=20, run_until_complete=True
        )
        if rst.status != "complete":
            raise RuntimeError(f"restore failed: {rst.status} {rst.error}")

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext after restore: {leaked[:3]}")

        dest_jobs = q.list_jobs(tenant_id=DST, limit=SRC_JOBS + 5)
        if len(dest_jobs) < SRC_JOBS:
            raise RuntimeError(f"dest has {len(dest_jobs)} jobs want {SRC_JOBS}")
        live = [j for j in dest_jobs if j.status.value in ("pending", "leased")]
        dead = [j for j in dest_jobs if j.status.value == "dead"]
        if dead:
            raise RuntimeError(
                f"dest fail-closed {len(dead)} jobs; blobs not rebuilt: "
                f"{dead[0].last_error}"
            )

        rot = q.rotate_tenant_payload_key(DST)
        for _ in range(20):
            rot = q.advance_payload_key_rotation(DST, batch_size=50)
            if rot.status == "complete":
                break
        if rot.status != "complete":
            raise RuntimeError(f"dest rotate did not complete: {rot.status}")

        sample = q.get(dest_jobs[0].id, tenant_id=DST)
        if sample is None or not str(sample.payload.get("secret") or "").startswith(
            SECRET_PREFIX
        ):
            raise RuntimeError("dest job did not decrypt after restore/rotate")

        from job_queue.crypto import is_blob_pointer

        stored = None
        with q._pool.connection() as conn:
            stored = conn.execute(
                "SELECT payload FROM jobs WHERE id = %s",
                (sample.id,),
            ).fetchone()["payload"]
        if isinstance(stored, str):
            stored = json.loads(stored)
        if "blob" in sample.payload and not is_blob_pointer(stored):
            raise RuntimeError("dest large job was not re-spilled")
        if is_blob_pointer(stored) and not q._blob_store.exists(stored["blob_id"]):
            raise RuntimeError("dest pointer has no file after rebuild")

        from job_queue import JobStatus

        deadline = time.time() + MAX_WALL
        while time.time() < deadline:
            pending = q.count(tenant_id=DST, status=JobStatus.PENDING)
            leased = q.count(tenant_id=DST, status=JobStatus.LEASED)
            completed = q.count(tenant_id=DST, status=JobStatus.COMPLETED)
            logger.info(
                "dst pending=%d leased=%d completed=%d",
                pending,
                leased,
                completed,
            )
            if pending == 0 and leased == 0 and completed >= SRC_JOBS * 0.8:
                break
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": "pbr-load-00",
                    "payload": {"kind": "blob-soak", "ms": 15, "i": int(time.time())},
                },
            )
            time.sleep(1.5)
        else:
            raise RuntimeError("dest did not drain after restore")

        if WEBHOOK_URL:
            for _ in range(15):
                q.process_webhook_deliveries(limit=40)
                delivered = q.list_webhook_deliveries(
                    tenant_id=DST, status="succeeded"
                )
                if delivered:
                    body = delivered[0].payload
                    if not str(
                        (body.get("payload") or {}).get("secret") or ""
                    ).startswith(SECRET_PREFIX):
                        raise RuntimeError(f"dest webhook not plaintext: {body}")
                    break
                time.sleep(0.5)

        load_ok = False
        for i in range(LOAD_TENANTS):
            tid = f"pbr-load-{i:02d}"
            done = q.count(tenant_id=tid, status=JobStatus.COMPLETED)
            logger.info("load %s completed=%d", tid, done)
            if done > 0:
                load_ok = True
        if not load_ok:
            raise RuntimeError("load tenants made no progress")

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext at end: {leaked[:3]}")
        logger.info("payload blob restore soak OK dest_completed=%d", completed)
    finally:
        q.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("payload blob restore soak failed")
        sys.exit(1)
