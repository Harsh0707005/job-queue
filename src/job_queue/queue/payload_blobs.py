"""Catalog + GC for spilled payload/result ciphertext blobs."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from job_queue.blob_store import BlobStoreError, FilesystemBlobStore
from job_queue.crypto import is_blob_pointer
from job_queue.db import transaction
from job_queue.queue._common import _utcnow


BLOB_KIND_PAYLOAD = "payload"
BLOB_KIND_RESULT = "result"
BLOB_KIND_DELIVERY = "delivery"
BLOB_KIND_OTHER = "other"

_PENDING_GC_SECONDS = 60


class PayloadBlobsMixin:
    _blob_store: FilesystemBlobStore | None
    payload_blob_threshold_bytes: int

    def process_payload_blobs(self, *, limit: int = 200) -> dict[str, int]:
        """Delete orphan / abandoned-pending blobs. Safe under load.

        Also removes leftover files with no catalog row (crash after write,
        before the Postgres insert committed). Those files are ignored until
        they are older than the pending-GC window so an in-flight spill
        cannot lose its ciphertext.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        cutoff = now - timedelta(seconds=_PENDING_GC_SECONDS)
        deleted = 0
        with transaction(self._pool) as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM job_payload_blobs
                WHERE status = 'orphan'
                   OR (status = 'pending' AND created_at < %s)
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (cutoff, int(limit)),
            ).fetchall()
            store = getattr(self, "_blob_store", None)
            for row in rows:
                bid = row["id"]
                if store is not None:
                    store.delete(bid)
                conn.execute("DELETE FROM job_payload_blobs WHERE id = %s", (bid,))
                deleted += 1
        unreferenced = self._gc_unreferenced_files(
            limit=max(0, int(limit) - deleted)
        )
        return {"deleted": deleted, "unreferenced": unreferenced}

    def _gc_unreferenced_files(self, *, limit: int) -> int:
        if limit < 1:
            return 0
        store = getattr(self, "_blob_store", None)
        if store is None or not hasattr(store, "iter_entries"):
            return 0
        now_ts = _utcnow().timestamp()
        removed = 0
        candidates = [
            bid
            for bid, mtime in store.iter_entries()
            if (now_ts - float(mtime)) >= _PENDING_GC_SECONDS
        ]
        if not candidates:
            return 0
        with transaction(self._pool) as conn:
            known = conn.execute(
                """
                SELECT id
                FROM job_payload_blobs
                WHERE id = ANY(%s)
                """,
                (candidates,),
            ).fetchall()
            keep = {row["id"] for row in known}
            for bid in candidates:
                if bid in keep:
                    continue
                store.delete(bid)
                removed += 1
                if removed >= limit:
                    break
        return removed

    def _orphan_blobs_for_tenant(self, conn: Any, tenant_id: str) -> None:
        if not tenant_id:
            return
        conn.execute(
            """
            UPDATE job_payload_blobs
            SET status = 'orphan'
            WHERE tenant_id = %s AND status <> 'orphan'
            """,
            (tenant_id,),
        )

    def _purge_tenant_blobs(self, conn: Any, tenant_id: str) -> int:
        """Delete catalog rows + files for a tenant (offboard wipe)."""
        if not tenant_id:
            return 0
        rows = conn.execute(
            "SELECT id FROM job_payload_blobs WHERE tenant_id = %s",
            (tenant_id,),
        ).fetchall()
        store = getattr(self, "_blob_store", None)
        n = 0
        for row in rows:
            bid = row["id"]
            if store is not None:
                store.delete(bid)
            conn.execute("DELETE FROM job_payload_blobs WHERE id = %s", (bid,))
            n += 1
        return n

    def _blob_id_of(self, payload: Any) -> UUID | None:
        if not is_blob_pointer(payload):
            return None
        try:
            return UUID(str(payload.get("blob_id")))
        except Exception:
            return None

    def _should_spill_envelope(self, env: dict[str, Any]) -> bool:
        store = getattr(self, "_blob_store", None)
        if store is None:
            return False
        threshold = int(getattr(self, "payload_blob_threshold_bytes", 0) or 0)
        if threshold < 1:
            return False
        raw = str(env.get("c") or "")
        return len(raw) >= threshold

    def _spill_envelope(
        self,
        conn: Any,
        env: dict[str, Any],
        *,
        tenant_id: str,
        job_id: UUID | None,
        kind: str,
    ) -> dict[str, Any]:
        store = getattr(self, "_blob_store", None)
        if store is None:
            return env
        cipher_b64 = str(env.get("c") or "")
        data = cipher_b64.encode("ascii")
        blob_id = uuid4()
        digest = store.write(blob_id, data)
        conn.execute(
            """
            INSERT INTO job_payload_blobs (
                id, tenant_id, job_id, kind, sha256, nbytes, key_id,
                status, created_at, committed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                'committed', NOW(), NOW()
            )
            """,
            (
                blob_id,
                tenant_id,
                job_id,
                kind,
                digest,
                len(data),
                env.get("kid"),
            ),
        )
        return {
            "_jq_enc": env.get("_jq_enc"),
            "kid": env.get("kid"),
            "n": env.get("n"),
            "blob_id": str(blob_id),
            "sha256": digest,
            "bytes": len(data),
        }

    def _hydrate_envelope(
        self, env: dict[str, Any], *, conn: Any | None = None
    ) -> dict[str, Any]:
        if not is_blob_pointer(env):
            return env
        store = getattr(self, "_blob_store", None)
        if store is None:
            raise BlobStoreError("payload blob store is not configured")
        blob_id = UUID(str(env["blob_id"]))
        data = store.read(blob_id)
        want = str(env.get("sha256") or "")
        if want:
            got = hashlib.sha256(data).hexdigest()
            if got != want:
                raise BlobStoreError(
                    f"payload blob {blob_id} checksum mismatch"
                )
        out = dict(env)
        out["c"] = data.decode("ascii")
        return out

    def _orphan_blob(self, conn: Any, blob_id: UUID | None) -> None:
        if blob_id is None:
            return
        conn.execute(
            """
            UPDATE job_payload_blobs
            SET status = 'orphan'
            WHERE id = %s AND status <> 'orphan'
            """,
            (blob_id,),
        )

    def _orphan_blobs_for_jobs(self, conn: Any, job_ids: list[UUID]) -> None:
        if not job_ids:
            return
        conn.execute(
            """
            UPDATE job_payload_blobs
            SET status = 'orphan'
            WHERE job_id = ANY(%s) AND status <> 'orphan'
            """,
            (job_ids,),
        )
