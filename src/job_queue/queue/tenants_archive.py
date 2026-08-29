"""Per-tenant archival of finished jobs into durable blob storage."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.blob_store import BlobStoreError
from job_queue.crypto import is_blob_pointer
from job_queue.db import transaction
from job_queue.models import JobArchiveResult, JobArchiveSettings, JobStatus
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _utcnow,
)
from job_queue.queue.payload_blobs import BLOB_KIND_ARCHIVE


_ARCHIVE_STATUSES = (
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.DEAD.value,
    JobStatus.CANCELLED.value,
)

ARCHIVE_STUB: dict[str, Any] = {"_jq_archived": True}


def is_archive_stub(value: Any) -> bool:
    return isinstance(value, dict) and value.get("_jq_archived") is True


class TenantsArchiveMixin:
    def set_tenant_job_archive(
        self,
        tenant_id: str,
        *,
        after_seconds: Any = _UNSET,
        max_live_bytes: Any = _UNSET,
        clear: bool = False,
    ) -> JobArchiveSettings:
        """Set per-tenant age/size archival (or clear back to off).

        When *clear* is True both policies are removed (no archival).

        Otherwise pass *after_seconds* and/or *max_live_bytes*:
        ``None`` means **never archive by that policy**. Pass a non-negative
        int for a finite window / byte cap. Omitted keys keep their previous
        value (or stay off if never set).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not clear:
            if after_seconds is not _UNSET:
                if after_seconds is not None and int(after_seconds) < 0:
                    raise ValueError("after_seconds must be >= 0 or None")
            if max_live_bytes is not _UNSET:
                if max_live_bytes is not None and int(max_live_bytes) < 0:
                    raise ValueError("max_live_bytes must be >= 0 or None")
            if after_seconds is _UNSET and max_live_bytes is _UNSET:
                raise ValueError(
                    "provide after_seconds and/or max_live_bytes, or clear=True"
                )
        now = _utcnow()
        with transaction(self._pool) as conn:
            if clear:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, max_concurrent_leases, max_pending_jobs,
                        paused, offboarding, updated_at
                    )
                    VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET archive_after_seconds = NULL,
                        archive_max_live_bytes = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return JobArchiveSettings(None, None)
            existing = conn.execute(
                """
                SELECT archive_after_seconds, archive_max_live_bytes
                FROM tenant_settings
                WHERE tenant_id = %s
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            prev_a = existing["archive_after_seconds"] if existing else None
            prev_b = existing["archive_max_live_bytes"] if existing else None

            def _to_store(val: Any, prev: Any) -> int | None:
                if val is _UNSET:
                    return prev
                if val is None:
                    return _TENANT_RETENTION_FOREVER
                return int(val)

            a_store = _to_store(after_seconds, prev_a)
            b_store = _to_store(max_live_bytes, prev_b)
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    archive_after_seconds, archive_max_live_bytes,
                    updated_at
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET archive_after_seconds = EXCLUDED.archive_after_seconds,
                    archive_max_live_bytes = EXCLUDED.archive_max_live_bytes,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, a_store, b_store, now),
            )
            return self._decode_tenant_archive_row(
                {
                    "archive_after_seconds": a_store,
                    "archive_max_live_bytes": b_store,
                }
            )

    def get_tenant_job_archive(self, tenant_id: str) -> JobArchiveSettings:
        """Return per-tenant archival policy.

        * inherits True → policy is unset (off).
        * inherits False and value ``None`` → explicit never.
        * inherits False and value ``>= 0`` → finite age / size cap.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT archive_after_seconds, archive_max_live_bytes
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                return JobArchiveSettings(None, None)
            return self._decode_tenant_archive_row(row)

    @staticmethod
    def _decode_tenant_archive_row(row: dict[str, Any]) -> JobArchiveSettings:
        def _one(raw: Any) -> tuple[int | None, bool]:
            if raw is None:
                return None, True
            n = int(raw)
            if n < 0:
                return None, False
            return n, False

        a_sec, a_inh = _one(row.get("archive_after_seconds"))
        b_n, b_inh = _one(row.get("archive_max_live_bytes"))
        return JobArchiveSettings(
            archive_after_seconds=a_sec,
            archive_max_live_bytes=b_n,
            after_inherits=a_inh,
            size_inherits=b_inh,
        )

    def process_job_archives(
        self,
        *,
        limit: int = 200,
        tenant_id: str | None = None,
        leader_token: UUID | None = None,
        leader_epoch: int | None = None,
    ) -> JobArchiveResult:
        """Move finished jobs that hit age or size policy into blob storage.

        Each job is archived in its own transaction: write the snapshot file,
        catalog it, then stub the row. A coordinator kill mid-pass leaves
        already-committed stubs durable and uncommitted files for pending-GC.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if limit > 10_000:
            raise ValueError("limit must be <= 10000")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")
        if getattr(self, "_blob_store", None) is None:
            return JobArchiveResult(archived=0, bytes_archived=0, skipped=0)

        remaining = int(limit)
        archived = 0
        bytes_n = 0
        skipped = 0
        policies = self._list_archive_policies(tenant_id=tenant_id)
        for tid, after_secs, max_bytes in policies:
            if remaining < 1:
                break
            if after_secs is not None:
                ids = self._select_age_archive_ids(
                    tid, after_seconds=after_secs, limit=remaining
                )
                for jid in ids:
                    if remaining < 1:
                        break
                    ok, nbytes = self._archive_one_job(
                        jid,
                        tenant_id=tid,
                        leader_token=leader_token,
                        leader_epoch=leader_epoch,
                    )
                    remaining -= 1
                    if ok:
                        archived += 1
                        bytes_n += nbytes
                    else:
                        skipped += 1
            if remaining < 1 or max_bytes is None:
                continue
            while remaining > 0:
                live = self._live_finished_bytes(tid)
                if live <= max_bytes:
                    break
                ids = self._select_oldest_live_finished_ids(tid, limit=1)
                if not ids:
                    break
                ok, nbytes = self._archive_one_job(
                    ids[0],
                    tenant_id=tid,
                    leader_token=leader_token,
                    leader_epoch=leader_epoch,
                )
                remaining -= 1
                if ok:
                    archived += 1
                    bytes_n += nbytes
                else:
                    skipped += 1
                    break
        return JobArchiveResult(
            archived=archived, bytes_archived=bytes_n, skipped=skipped
        )

    def _list_archive_policies(
        self, *, tenant_id: str | None
    ) -> list[tuple[str, int | None, int | None]]:
        clause = ""
        params: list[Any] = []
        if tenant_id is not None:
            clause = "AND tenant_id = %s"
            params.append(tenant_id)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT tenant_id, archive_after_seconds, archive_max_live_bytes
                FROM tenant_settings
                WHERE (
                        (archive_after_seconds IS NOT NULL
                         AND archive_after_seconds >= 0)
                     OR (archive_max_live_bytes IS NOT NULL
                         AND archive_max_live_bytes >= 0)
                )
                {clause}
                ORDER BY tenant_id
                """,
                tuple(params),
            ).fetchall()
        out: list[tuple[str, int | None, int | None]] = []
        for row in rows:
            after = row["archive_after_seconds"]
            size = row["archive_max_live_bytes"]
            after_n = int(after) if after is not None and int(after) >= 0 else None
            size_n = int(size) if size is not None and int(size) >= 0 else None
            if after_n is None and size_n is None:
                continue
            out.append((str(row["tenant_id"]), after_n, size_n))
        return out

    def _select_age_archive_ids(
        self, tenant_id: str, *, after_seconds: int, limit: int
    ) -> list[UUID]:
        if limit < 1:
            return []
        now = _utcnow()
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE tenant_id = %s
                  AND archived_at IS NULL
                  AND archive_blob_id IS NULL
                  AND status = ANY(%s)
                  AND COALESCE(completed_at, updated_at)
                      <= (%s - make_interval(secs => %s))
                ORDER BY COALESCE(completed_at, updated_at) ASC, id ASC
                LIMIT %s
                """,
                (
                    tenant_id,
                    list(_ARCHIVE_STATUSES),
                    now,
                    int(after_seconds),
                    int(limit),
                ),
            ).fetchall()
        return [r["id"] for r in rows]

    def _select_oldest_live_finished_ids(
        self, tenant_id: str, *, limit: int
    ) -> list[UUID]:
        if limit < 1:
            return []
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE tenant_id = %s
                  AND archived_at IS NULL
                  AND archive_blob_id IS NULL
                  AND status = ANY(%s)
                ORDER BY COALESCE(completed_at, updated_at) ASC, id ASC
                LIMIT %s
                """,
                (tenant_id, list(_ARCHIVE_STATUSES), int(limit)),
            ).fetchall()
        return [r["id"] for r in rows]

    def _live_finished_bytes(self, tenant_id: str) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(
                    COALESCE(pg_column_size(payload), 0)
                    + COALESCE(pg_column_size(result), 0)
                    + COALESCE(pg_column_size(checkpoint), 0)
                ), 0)::bigint AS n
                FROM jobs
                WHERE tenant_id = %s
                  AND archived_at IS NULL
                  AND status = ANY(%s)
                """,
                (tenant_id, list(_ARCHIVE_STATUSES)),
            ).fetchone()
        return int(row["n"]) if row else 0

    def _archive_one_job(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        leader_token: UUID | None,
        leader_epoch: int | None,
    ) -> tuple[bool, int]:
        now = _utcnow()
        with transaction(self._pool) as conn:
            if hasattr(self, "_fence_control_leader") and not self._fence_control_leader(
                conn, lease_token=leader_token, epoch=leader_epoch
            ):
                return False, 0
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE SKIP LOCKED
                """,
                (job_id, tenant_id),
            ).fetchone()
            if row is None:
                return False, 0
            if row.get("archived_at") is not None or row.get("archive_blob_id"):
                return False, 0
            if str(row["status"]) not in _ARCHIVE_STATUSES:
                return False, 0
            try:
                snapshot = self._build_archive_snapshot(row)
            except BlobStoreError:
                return False, 0
            blob_id, nbytes = self._write_archive_blob(
                conn,
                tenant_id=tenant_id,
                job_id=job_id,
                snapshot=snapshot,
                key_id=row.get("payload_key_id"),
            )
            old_payload = (
                self._blob_id_of(row.get("payload"))
                if hasattr(self, "_blob_id_of")
                else None
            )
            old_result = (
                self._blob_id_of(row.get("result"))
                if hasattr(self, "_blob_id_of")
                else None
            )
            conn.execute(
                """
                UPDATE jobs
                SET payload = %s,
                    result = NULL,
                    checkpoint = NULL,
                    archived_at = %s,
                    archive_blob_id = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                """,
                (Jsonb(ARCHIVE_STUB), now, blob_id, now, job_id, tenant_id),
            )
            if hasattr(self, "_orphan_blob"):
                if old_payload is not None and old_payload != blob_id:
                    self._orphan_blob(conn, old_payload)
                if old_result is not None and old_result != blob_id:
                    self._orphan_blob(conn, old_result)
            return True, nbytes

    def _build_archive_snapshot(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "v": 1,
            "payload": self._materialize_for_archive(row.get("payload")),
            "result": self._materialize_for_archive(row.get("result")),
            "checkpoint": row.get("checkpoint"),
        }

    def _materialize_for_archive(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value
        if is_blob_pointer(value) and hasattr(self, "_hydrate_envelope"):
            return self._hydrate_envelope(value)
        return value

    def _write_archive_blob(
        self,
        conn: Any,
        *,
        tenant_id: str,
        job_id: UUID,
        snapshot: dict[str, Any],
        key_id: Any = None,
    ) -> tuple[UUID, int]:
        store = getattr(self, "_blob_store", None)
        if store is None:
            raise BlobStoreError("payload blob store is not configured")
        snapshot = {
            "v": snapshot.get("v", 1),
            "payload": self._materialize_for_archive(snapshot.get("payload")),
            "result": self._materialize_for_archive(snapshot.get("result")),
            "checkpoint": snapshot.get("checkpoint"),
        }
        data = json.dumps(snapshot, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
        blob_id = uuid4()
        digest = store.write(blob_id, data)
        kid = key_id
        payload = snapshot.get("payload")
        if isinstance(payload, dict) and payload.get("kid"):
            try:
                kid = UUID(str(payload["kid"]))
            except Exception:  # noqa: BLE001
                kid = key_id
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
                BLOB_KIND_ARCHIVE,
                digest,
                len(data),
                kid,
            ),
        )
        return blob_id, len(data)

    def _read_archive_snapshot(self, blob_id: UUID | str) -> dict[str, Any]:
        store = getattr(self, "_blob_store", None)
        if store is None:
            raise BlobStoreError("payload blob store is not configured")
        raw = store.read(UUID(str(blob_id)))
        try:
            snap = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BlobStoreError(
                f"archive blob {blob_id} is not valid JSON"
            ) from exc
        if not isinstance(snap, dict):
            raise BlobStoreError(f"archive blob {blob_id} is not an object")
        return snap

    def _overlay_archive_row(
        self, data: dict[str, Any], *, conn: Any | None = None
    ) -> dict[str, Any]:
        bid = data.get("archive_blob_id")
        if bid is None and not data.get("archived_at"):
            return data
        if bid is None:
            return data
        snap = self._read_archive_snapshot(bid)
        if "payload" in snap and snap.get("payload") is not None:
            data["payload"] = snap["payload"]
        if "result" in snap:
            data["result"] = snap["result"]
        if "checkpoint" in snap:
            data["checkpoint"] = snap["checkpoint"]
        return data
