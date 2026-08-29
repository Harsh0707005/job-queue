"""Catalog + GC for spilled payload/result ciphertext blobs."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from dataclasses import replace

from job_queue.blob_store import BlobStoreError, FilesystemBlobStore
from job_queue.crypto import REDACTED_PAYLOAD, is_blob_pointer
from job_queue.db import transaction
from job_queue.models import JobStatus, WebhookEvent
from job_queue.queue._common import _JOB_COLUMNS, UnreadablePayloadError, _utcnow


BLOB_KIND_PAYLOAD = "payload"
BLOB_KIND_RESULT = "result"
BLOB_KIND_DELIVERY = "delivery"
BLOB_KIND_OTHER = "other"
BLOB_KIND_ARCHIVE = "archive"

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
                if self._blob_is_referenced(conn, bid):
                    continue
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

    def _purge_unreferenced_tenant_blobs(self, conn: Any, tenant_id: str) -> int:
        """Drop tenant blobs no live job or delivery pointer still names."""
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
            if self._blob_is_referenced(conn, bid):
                continue
            if store is not None:
                store.delete(bid)
            conn.execute("DELETE FROM job_payload_blobs WHERE id = %s", (bid,))
            n += 1
        return n

    def _count_tenant_blobs(self, conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*)::int AS n FROM job_payload_blobs WHERE tenant_id = %s",
            (tenant_id,),
        ).fetchone()
        return int(row["n"]) if row else 0

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

    def _assert_envelope_readable(self, env: Any) -> None:
        """Raise if a spilled pointer has no readable file."""
        if not is_blob_pointer(env):
            return
        self._hydrate_envelope(env)

    def _blob_is_referenced(self, conn: Any, blob_id: UUID) -> bool:
        bid = str(blob_id)
        row = conn.execute(
            """
            SELECT 1
            FROM jobs
            WHERE payload->>'blob_id' = %s
               OR result->>'blob_id' = %s
               OR archive_blob_id::text = %s
            LIMIT 1
            """,
            (bid, bid, bid),
        ).fetchone()
        if row is not None:
            return True
        row = conn.execute(
            """
            SELECT 1
            FROM webhook_deliveries
            WHERE payload->>'blob_id' = %s
            LIMIT 1
            """,
            (bid,),
        ).fetchone()
        return row is not None

    def _materialize_envelope(
        self, env: Any, *, conn: Any | None = None
    ) -> Any:
        """Return an openable envelope: inline ``c`` or hydrate a pointer."""
        if isinstance(env, str):
            import json

            try:
                env = json.loads(env)
            except json.JSONDecodeError:
                return env
        if not isinstance(env, dict):
            return env
        if isinstance(env.get("c"), str):
            return env
        if is_blob_pointer(env):
            return self._hydrate_envelope(env, conn=conn)
        return env

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

    def _dead_letter_unreadable_payload(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        error: str,
        worker_id: str | None,
        now: Any,
    ) -> dict[str, Any] | None:
        """Mark a job dead because its spilled ciphertext cannot be opened.

        Does not hydrate or decrypt. Dependents are cancelled so a missing
        blob cannot stall a tenant's lease loop forever.
        """
        tenant_id = str(row["tenant_id"])
        job_id = row["id"]
        err = (error or "payload blob is unreadable")[:2000]
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                last_error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s
              AND tenant_id = %s
              AND status IN (%s, %s)
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.DEAD.value,
                err,
                now,
                now,
                job_id,
                tenant_id,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
            ),
        ).fetchone()
        if updated is None:
            return None
        if hasattr(self, "_orphan_blob"):
            self._orphan_blob(conn, self._blob_id_of(row.get("payload")))
            self._orphan_blob(conn, self._blob_id_of(row.get("result")))
        if hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="dead_lettered",
                from_status=row.get("status"),
                actor=worker_id,
                detail={"reason": "unreadable payload blob", "error": err},
                now=now,
            )
        if hasattr(self, "_record_metric"):
            self._record_metric(
                conn, tenant_id=tenant_id, metric="dead", count=1, now=now
            )
        if hasattr(self, "_cascade_cancel_dependents"):
            self._cascade_cancel_dependents(
                conn,
                parent_id=job_id,
                tenant_id=tenant_id,
                reason=f"parent job {job_id} payload blob is unreadable",
                now=now,
            )
        if updated.get("parent_job_id") is not None and hasattr(
            self, "_maybe_wake_parent_on_join_locked"
        ):
            self._maybe_wake_parent_on_join_locked(
                conn,
                parent_id=updated["parent_job_id"],
                tenant_id=tenant_id,
                now=now,
            )
        if hasattr(self, "_fail_schedule_generation_for_job"):
            try:
                self._fail_schedule_generation_for_job(conn, updated)
            except BlobStoreError:
                pass
        if hasattr(self, "_on_workflow_step_terminal"):
            try:
                self._on_workflow_step_terminal(
                    conn, updated, success=False, error=err, now=now
                )
            except BlobStoreError:
                pass
        if hasattr(self, "_enqueue_job_webhooks"):
            sealed = self._job(updated, decrypt=False, conn=conn)
            hook_job = replace(
                sealed, payload=dict(REDACTED_PAYLOAD), result=None
            )
            self._enqueue_job_webhooks(
                conn, job=hook_job, event=WebhookEvent.JOB_DEAD, now=now
            )
        return updated

    def _fail_closed_unreadable_delivery(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: Any,
    ) -> None:
        """Open a webhook delivery payload; dead-letter if the blob is gone."""
        from job_queue.crypto import delivery_aad

        try:
            self._open_dict(
                row.get("payload"),
                tenant_id=str(row["tenant_id"]),
                aad=delivery_aad(row["id"]),
                conn=conn,
            )
        except BlobStoreError as exc:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = 'dead',
                    delivering_until = NULL,
                    last_error = %s,
                    completed_at = COALESCE(completed_at, %s),
                    updated_at = %s
                WHERE id = %s
                  AND status IN ('pending', 'delivering')
                """,
                (str(exc)[:2000], now, now, row["id"]),
            )
            self._orphan_blob(conn, self._blob_id_of(row.get("payload")))
            raise UnreadablePayloadError(
                f"delivery {row['id']} payload blob is unreadable"
            ) from exc

    def _fail_closed_unreadable_payload(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        worker_id: str | None,
        now: Any,
    ) -> None:
        """Open the spilled payload; dead-letter and raise if it cannot be read."""
        try:
            self._open_job_payload(
                row.get("payload"),
                tenant_id=str(row["tenant_id"]),
                job_id=row["id"],
                conn=conn,
            )
        except BlobStoreError as exc:
            self._dead_letter_unreadable_payload(
                conn,
                row,
                error=str(exc),
                worker_id=worker_id,
                now=now,
            )
            raise UnreadablePayloadError(
                f"job {row['id']} payload blob is unreadable"
            ) from exc

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
