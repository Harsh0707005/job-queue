#!/usr/bin/env python3
"""Fleet soak: live DEK rotation of spilled payloads under load.

Pending + leased jobs, webhooks, and export run while some rows still
name the old key and others the new one. After rotate completes every
blob pointer must still open; list APIs stay redacted; no plaintext in PG.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("payload_blob_rotate_soak")

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
SECRET_PREFIX = os.environ.get("SECRET_PREFIX", "prot-secret")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL") or ""
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


def postgres_secret_hits(secrets: list[str]) -> list[str]:
    import psycopg
    from psycopg.rows import dict_row

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
    return [s for s in secrets if s in blob]


def assert_pointers_openable(q, tenant_id: str) -> None:
    from job_queue.crypto import is_blob_pointer

    with q._pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT id, payload, result FROM jobs WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
        dels = conn.execute(
            """
            SELECT id, payload FROM webhook_deliveries WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
    for row in rows:
        for col in ("payload", "result"):
            env = row.get(col)
            if not is_blob_pointer(env):
                continue
            q._assert_envelope_readable(env)
            got = q.get(row["id"], tenant_id=tenant_id)
            if got is None:
                raise RuntimeError(f"{tenant_id} job {row['id']} vanished")
    for row in dels:
        if is_blob_pointer(row.get("payload")):
            q._assert_envelope_readable(row["payload"])


def main() -> int:
    logger.info(
        "blob rotate soak tenants=%d jobs=%d bytes=%d",
        TENANTS,
        JOBS,
        PAYLOAD_BYTES,
    )
    wait_coordinators()
    q = connect_queue()
    pad = "R" * PAYLOAD_BYTES
    secrets: list[str] = []
    expected = 0
    try:
        if WEBHOOK_URL:
            from job_queue import WebhookEvent

            for i in range(TENANTS):
                q.register_webhook(
                    tenant_id=f"prot-{i:02d}",
                    url=WEBHOOK_URL,
                    events=[WebhookEvent.JOB_COMPLETED.value],
                    secret="prot-hook",
                )
        for i in range(TENANTS):
            tid = f"prot-{i:02d}"
            for n in range(JOBS):
                secret = f"{SECRET_PREFIX}-{tid}-{n}"
                secrets.append(secret)
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": {
                            "kind": "blob-soak",
                            "secret": secret,
                            "blob": pad,
                            "ack_result": {"echo": secret, "blob": pad},
                        },
                    },
                )
                expected += 1

        sample = http_json("GET", "/v1/jobs?tenant_id=prot-00&limit=8")
        for row in sample.get("jobs") or []:
            payload = row.get("payload") or {}
            if payload.get("secret"):
                raise RuntimeError(f"list leaked plaintext: {payload}")
            if payload != {"_encrypted": True}:
                raise RuntimeError(f"list not redacted: {payload}")

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext before rotate: {leaked[:3]}")

        # Let workers take some leases so rotation overlaps inflight work.
        time.sleep(1.2)

        for i in range(TENANTS):
            tid = f"prot-{i:02d}"
            rot = http_json("POST", f"/v1/tenants/{tid}/payload-keys/rotate", {})
            status = (rot.get("rotation") or {}).get("status")
            logger.info("rotate %s status=%s", tid, status)
            if status != "reencrypting":
                raise RuntimeError(f"rotate {tid} unexpected {rot}")

        # Export one tenant mid-rotation (mixed old/new keys).
        snap = q.export_tenant("prot-00")
        if snap.status != "ready":
            raise RuntimeError(f"mid-rotate export failed: {snap}")
        logger.info("exported prot-00 mid-rotate blobs=%s", snap.counts.get("payload_blobs"))

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext after rotate start: {leaked[:3]}")

        deadline = time.time() + MAX_WALL
        while time.time() < deadline:
            try:
                http_json(
                    "POST",
                    "/v1/admin/payload-keys/advance",
                    {"limit": TENANTS, "batch_size": 40},
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("advance: %s", exc)
            bl = http_json("GET", "/v1/admin/backlog").get("jobs") or {}
            completed = int(bl.get("completed") or 0)
            pending = int(bl.get("pending") or 0)
            leased = int(bl.get("leased") or 0)
            logger.info(
                "progress completed=%d pending=%d leased=%d expected=%d",
                completed,
                pending,
                leased,
                expected,
            )
            done_rots = 0
            for i in range(TENANTS):
                info = http_json("GET", f"/v1/tenants/prot-{i:02d}/payload-keys")
                if (info.get("rotation") or {}).get("status") in (None, "complete"):
                    done_rots += 1
            if completed >= expected and pending == 0 and leased == 0 and done_rots == TENANTS:
                break
            time.sleep(1.5)
        else:
            raise RuntimeError("drain/rotate did not finish")

        for i in range(TENANTS):
            tid = f"prot-{i:02d}"
            info = http_json("GET", f"/v1/tenants/{tid}/payload-keys")
            rot = info.get("rotation") or {}
            if rot.get("status") not in (None, "complete"):
                raise RuntimeError(f"{tid} rotation stuck: {rot}")
            decrypt_only = [k for k in info["keys"] if k["status"] == "decrypt_only"]
            if decrypt_only:
                raise RuntimeError(f"{tid} leftover decrypt_only: {decrypt_only}")
            assert_pointers_openable(q, tid)
            q.process_payload_blobs(limit=200)
            assert_pointers_openable(q, tid)

        leaked = postgres_secret_hits(secrets)
        if leaked:
            raise RuntimeError(f"plaintext after complete: {leaked[:3]}")

        rst = q.restore_tenant(
            "prot-restore",
            snapshot_id=snap.id,
            batch_size=40,
            run_until_complete=True,
        )
        if rst.status != "complete":
            raise RuntimeError(f"restore after rotate failed: {rst.status}")
        assert_pointers_openable(q, "prot-restore")
        sample_job = q.list_jobs(tenant_id="prot-restore", limit=1)
        if not sample_job or not str(
            sample_job[0].payload.get("secret") or ""
        ).startswith(SECRET_PREFIX):
            raise RuntimeError("restored job did not decrypt")

        if WEBHOOK_URL:
            q.process_webhook_deliveries(limit=80)

        logger.info("payload blob rotate soak OK completed=%d", expected)
    finally:
        q.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("payload blob rotate soak failed")
        sys.exit(1)
