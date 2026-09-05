"""Live relocate of one tenant from this Postgres onto another.

Other tenants on both sides keep running. The moving tenant is paused so the
snapshot is complete; each stage is a committed cursor so a coordinator kill
cannot leave dest serving half a tenant or wipe source before dest is live.
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.blob_store import FilesystemBlobStore
from job_queue.crypto import PayloadCrypto, PayloadCryptoError, wrap_dek
from job_queue.db import transaction
from job_queue.models import TenantRelocate
from job_queue.queue._common import RelocateError, _as_utc, _utcnow
from job_queue.queue.tenants_export import (
    SNAPSHOT_KINDS,
    _KIND_SQL,
    relocate_residue_tables,
)


RELOCATE_STAGES: tuple[str, ...] = (
    "pausing",
    "draining",
    "exporting",
    "applying",
    "verifying",
    "cutover",
    "purging",
    "complete",
)

_JSONB_COLUMNS = frozenset(
    {
        "payload",
        "result",
        "checkpoint",
        "detail",
        "steps_def",
        "dag_jobs",
        "workflow",
        "resource_cost",
    }
)

_TIME_COLUMNS = frozenset({"daily_start", "daily_end"})


def relocate_from_row(row: dict[str, Any]) -> TenantRelocate:
    counts = row.get("counts") or {}
    if isinstance(counts, str):
        counts = json.loads(counts)
    return TenantRelocate(
        id=row["id"],
        tenant_id=str(row["tenant_id"]),
        dest_dsn=str(row["dest_dsn"]),
        dest_blob_dir=(
            None if row.get("dest_blob_dir") is None else str(row["dest_blob_dir"])
        ),
        snapshot_id=row.get("snapshot_id"),
        status=str(row["status"]),
        stage=str(row["stage"]),
        kind_index=int(row.get("kind_index") or 0),
        counts={str(k): int(v) for k, v in dict(counts).items()},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row.get("completed_at"),
        error=row.get("error"),
        source_paused=bool(row.get("source_paused")),
    )


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc(raw)
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


def _sql_value(col: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict) and set(value.keys()) == {"$b64"}:
        return base64.b64decode(str(value["$b64"]))
    if col in _TIME_COLUMNS:
        return value
    if col.endswith("_at") or col in ("sampled_at", "bucket_start"):
        if isinstance(value, str):
            return _parse_dt(value)
        return value
    if col in _JSONB_COLUMNS:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value
        if isinstance(value, (dict, list)):
            return Jsonb(value)
        return value
    if isinstance(value, dict):
        return Jsonb(value)
    return value


class TenantsRelocateMixin:
    def relocate_tenant(
        self,
        tenant_id: str,
        *,
        dest_dsn: str | None = None,
        dest_blob_dir: str | None = None,
        dest_kek: str | None = None,
        dest_kek_id: str | None = None,
        dest_keks: dict[str, str] | None = None,
        batch_size: int = 100,
        run_until_complete: bool = False,
    ) -> TenantRelocate:
        """Start or resume moving *tenant_id* onto *dest_dsn*.

        Source is paused first so no new work is created after the snapshot.
        Dest stays paused+restoring until cutover. Each ``advance`` is one
        transaction (or one dest batch); a coordinator crash leaves a cursor
        another node can resume. Other tenants are not touched.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        rst = self._ensure_relocate(
            tenant_id,
            dest_dsn=dest_dsn,
            dest_blob_dir=dest_blob_dir,
            dest_kek=dest_kek,
            dest_kek_id=dest_kek_id,
            dest_keks=dest_keks,
        )
        if rst.status == "complete":
            return rst
        if run_until_complete:
            for _ in range(20000):
                rst = self.advance_tenant_relocate(
                    tenant_id, batch_size=batch_size
                )
                if rst.status in ("complete", "failed", "aborted"):
                    return rst
            raise RelocateError("relocate did not complete within batch budget")
        return self.advance_tenant_relocate(tenant_id, batch_size=batch_size)

    def get_tenant_relocate(self, tenant_id: str) -> TenantRelocate | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM tenant_relocations
                WHERE tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            return relocate_from_row(row) if row else None

    def advance_tenant_relocate(
        self, tenant_id: str, *, batch_size: int = 100
    ) -> TenantRelocate:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"relocate:{tenant_id}",),
            )
            row = conn.execute(
                """
                SELECT * FROM tenant_relocations
                WHERE tenant_id = %s AND status = 'running'
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                latest = conn.execute(
                    """
                    SELECT * FROM tenant_relocations
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (tenant_id,),
                ).fetchone()
                if latest is None:
                    raise RelocateError(
                        f"no relocate in progress for tenant {tenant_id!r}"
                    )
                return relocate_from_row(latest)
        try:
            updated = self._advance_relocate_stage(row, batch_size=batch_size, now=now)
            return updated
        except RelocateError as exc:
            if exc.fatal:
                self.abort_tenant_relocate(tenant_id, error=str(exc))
            raise
        except Exception as exc:  # noqa: BLE001
            self._mark_relocate_error(row["id"], str(exc)[:2000])
            raise RelocateError(
                f"relocate failed for tenant {tenant_id!r}: {exc}"
            ) from exc

    def process_tenant_relocations(
        self, *, limit: int = 10, batch_size: int = 100
    ) -> list[TenantRelocate]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT tenant_id FROM tenant_relocations
                WHERE status = 'running'
                ORDER BY updated_at ASC
                LIMIT %s
                """,
                (int(limit),),
            ).fetchall()
        out: list[TenantRelocate] = []
        for r in rows:
            try:
                out.append(
                    self.advance_tenant_relocate(
                        str(r["tenant_id"]), batch_size=batch_size
                    )
                )
            except RelocateError:
                continue
        return out

    def abort_tenant_relocate(
        self, tenant_id: str, *, error: str | None = None
    ) -> TenantRelocate:
        """Undo a relocate that has not cut dest over.

        Source is unfrozen (paused only if it was paused before the move).
        Partial dest rows written while dest was ``restoring`` are wiped.
        Dest tenants that already had data are left alone.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        dest_row: dict[str, Any] | None = None
        wipe_dest = False
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"relocate:{tenant_id}",),
            )
            row = conn.execute(
                """
                SELECT * FROM tenant_relocations
                WHERE tenant_id = %s AND status = 'running'
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                latest = conn.execute(
                    """
                    SELECT * FROM tenant_relocations
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (tenant_id,),
                ).fetchone()
                if latest is None:
                    raise RelocateError(
                        f"no relocate in progress for tenant {tenant_id!r}"
                    )
                return relocate_from_row(latest)
            stage = str(row["stage"])
            if stage in ("cutover", "purging", "complete"):
                raise RelocateError(
                    f"tenant {tenant_id!r} already cut over; abort is refused"
                )
            relocating = self._is_tenant_relocating(conn, tenant_id)
            if relocating:
                paused = bool(row.get("source_paused"))
                conn.execute(
                    """
                    UPDATE tenant_settings
                    SET paused = %s,
                        relocating = FALSE,
                        relocated = FALSE,
                        restoring = FALSE,
                        updated_at = %s
                    WHERE tenant_id = %s
                    """,
                    (paused, now, tenant_id),
                )
            sid = row.get("snapshot_id")
            if sid is not None:
                conn.execute(
                    "DELETE FROM tenant_snapshots WHERE id = %s",
                    (sid,),
                )
            updated = conn.execute(
                """
                UPDATE tenant_relocations
                SET status = 'aborted',
                    error = %s,
                    updated_at = %s,
                    completed_at = %s
                WHERE id = %s
                RETURNING *
                """,
                (
                    (error or row.get("error") or "aborted")[:2000],
                    now,
                    now,
                    row["id"],
                ),
            ).fetchone()
            assert updated is not None
            dest_row = dict(row)
            wipe_dest = stage in ("applying", "verifying")
            out = relocate_from_row(updated)
        if wipe_dest and dest_row is not None:
            try:
                dest = self._relocate_dest_queue(dest_row)
                with dest._pool.connection() as dconn:
                    dest_ours = dest._is_tenant_restoring(dconn, tenant_id)
                if dest_ours:
                    self._wipe_relocate_dest_copy(dest, tenant_id)
            except Exception:
                pass
        return out

    def _ensure_relocate(
        self,
        tenant_id: str,
        *,
        dest_dsn: str | None,
        dest_blob_dir: str | None,
        dest_kek: str | None = None,
        dest_kek_id: str | None = None,
        dest_keks: dict[str, str] | None = None,
    ) -> TenantRelocate:
        now = _utcnow()
        dest_dsn = (dest_dsn or os.environ.get("JOB_QUEUE_RELOCATE_DEST_DSN") or "").strip()
        if dest_blob_dir is None:
            raw_dir = os.environ.get("JOB_QUEUE_RELOCATE_DEST_BLOB_DIR")
            dest_blob_dir = raw_dir.strip() if raw_dir else None
        if dest_kek is None:
            dest_kek = os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEK") or None
        if dest_kek_id is None:
            dest_kek_id = os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEK_ID") or None
        if dest_keks is None:
            raw_keks = os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEKS")
            if raw_keks:
                parsed = json.loads(raw_keks)
                if not isinstance(parsed, dict):
                    raise ValueError("dest_keks must be a JSON object of id → key")
                dest_keks = {str(k): str(v) for k, v in parsed.items()}
        if dest_kek is not None:
            dest_kek = dest_kek.strip() or None
        if dest_kek_id is not None:
            dest_kek_id = dest_kek_id.strip() or None
        if not dest_dsn:
            raise ValueError("dest_dsn is required")
        if dest_dsn == getattr(self, "_dsn", None):
            raise RelocateError("dest_dsn must be a different Postgres")
        try:
            self._relocate_dest_crypto(
                dest_kek=dest_kek, dest_kek_id=dest_kek_id, dest_keks=dest_keks
            )
        except (PayloadCryptoError, ValueError) as exc:
            raise RelocateError(
                f"destination KEK is not usable: {exc}"
            ) from exc
        dest = self._relocate_dest_queue(
            {
                "dest_dsn": dest_dsn,
                "dest_blob_dir": dest_blob_dir,
                "dest_kek": dest_kek,
                "dest_kek_id": dest_kek_id,
                "dest_keks": dest_keks,
            }
        )
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"relocate:{tenant_id}",),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise RelocateError(
                    f"tenant {tenant_id!r} is offboarding and cannot relocate"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise RelocateError(
                    f"tenant {tenant_id!r} is restoring and cannot relocate"
                )
            if self._is_tenant_relocated(conn, tenant_id):
                existing = conn.execute(
                    """
                    SELECT * FROM tenant_relocations
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (tenant_id,),
                ).fetchone()
                if existing is not None and existing["status"] == "complete":
                    return relocate_from_row(existing)
                raise RelocateError(
                    f"tenant {tenant_id!r} has already been relocated"
                )
            existing = conn.execute(
                """
                SELECT * FROM tenant_relocations
                WHERE tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            if existing is not None and existing["status"] == "running":
                if str(existing["dest_dsn"]) != dest_dsn:
                    raise RelocateError(
                        f"tenant {tenant_id!r} already relocating to a different dest"
                    )
                return relocate_from_row(existing)
            if existing is not None and existing["status"] == "complete":
                if str(existing["dest_dsn"]) == dest_dsn:
                    return relocate_from_row(existing)
            with dest._pool.connection() as dconn:
                leftover = relocate_residue_tables(dconn, tenant_id)
            if leftover:
                raise RelocateError(
                    f"dest tenant {tenant_id!r} is not empty; leftover: "
                    + ", ".join(leftover)
                )
            row = conn.execute(
                """
                INSERT INTO tenant_relocations (
                    id, tenant_id, dest_dsn, dest_blob_dir, status, stage,
                    kind_index, blob_seq, blob_offset, counts,
                    dest_kek_id, dest_kek, dest_keks,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, 'running', 'pausing',
                    0, 0, 0, '{}'::jsonb,
                    %s, %s, %s,
                    %s, %s
                )
                RETURNING *
                """,
                (
                    uuid4(),
                    tenant_id,
                    dest_dsn,
                    dest_blob_dir,
                    dest_kek_id,
                    dest_kek,
                    Jsonb(dest_keks) if dest_keks else None,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return relocate_from_row(row)

    def _advance_relocate_stage(
        self, row: dict[str, Any], *, batch_size: int, now: datetime
    ) -> TenantRelocate:
        stage = str(row["stage"])
        if stage == "pausing":
            return self._relocate_pause(row, now=now)
        if stage == "draining":
            return self._relocate_drain(row, now=now)
        if stage == "exporting":
            return self._relocate_export(row, now=now)
        if stage == "applying":
            return self._relocate_apply(row, batch_size=batch_size, now=now)
        if stage == "verifying":
            return self._relocate_verify(row, now=now)
        if stage == "cutover":
            return self._relocate_cutover(row, now=now)
        if stage == "purging":
            return self._relocate_purge(row, now=now)
        if stage == "complete":
            return relocate_from_row(row)
        raise RelocateError(f"unknown relocate stage {stage!r}")

    def _relocate_pause(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        with transaction(self._pool) as conn:
            was_paused = self._is_tenant_paused(conn, tenant_id)
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, restoring, relocating, relocated,
                    updated_at
                ) VALUES (%s, NULL, NULL, TRUE, FALSE, FALSE, TRUE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    relocating = TRUE,
                    relocated = FALSE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )
            conn.execute(
                """
                UPDATE tenant_relocations
                SET source_paused = %s, updated_at = %s
                WHERE id = %s
                """,
                (was_paused, now, row["id"]),
            )
            return self._set_relocate_stage(conn, row["id"], "draining", now=now)

    def _relocate_drain(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        with transaction(self._pool) as conn:
            leased = self._count_leased(conn, tenant_id)
            delivering = self._count_webhook_delivering(conn, tenant_id)
            running_acts = 0
            if hasattr(self, "_count_running_activities"):
                running_acts = int(
                    self._count_running_activities(conn, tenant_id) or 0
                )
            if leased > 0 or delivering > 0 or running_acts > 0:
                updated = conn.execute(
                    """
                    UPDATE tenant_relocations
                    SET updated_at = %s, error = NULL
                    WHERE id = %s
                    RETURNING *
                    """,
                    (now, row["id"]),
                ).fetchone()
                assert updated is not None
                return relocate_from_row(updated)
            return self._set_relocate_stage(conn, row["id"], "exporting", now=now)

    def _relocate_export(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        sid = row.get("snapshot_id")
        if sid is not None:
            snap = self.get_tenant_snapshot(sid)
            if snap is not None and snap.status == "ready":
                with transaction(self._pool) as conn:
                    return self._set_relocate_stage(
                        conn, row["id"], "applying", now=now, snapshot_id=sid
                    )
        snap = self.export_tenant(tenant_id)
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE tenant_relocations
                SET snapshot_id = %s,
                    counts = %s,
                    stage = 'applying',
                    kind_index = 0,
                    blob_seq = 0,
                    blob_offset = 0,
                    updated_at = %s,
                    error = NULL
                WHERE id = %s
                """,
                (snap.id, Jsonb(dict(snap.counts)), now, row["id"]),
            )
            out = conn.execute(
                "SELECT * FROM tenant_relocations WHERE id = %s",
                (row["id"],),
            ).fetchone()
            assert out is not None
            return relocate_from_row(out)

    def _relocate_apply(
        self, row: dict[str, Any], *, batch_size: int, now: datetime
    ) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        snapshot_id = row.get("snapshot_id")
        if snapshot_id is None:
            raise RelocateError("relocate snapshot missing")
        dest = self._relocate_dest_queue(row)
        self._prepare_dest_tenant(dest, tenant_id, resuming=int(row["kind_index"] or 0) > 0)
        kind_index = int(row["kind_index"] or 0)
        blob_seq = int(row["blob_seq"] or 0)
        blob_offset = int(row["blob_offset"] or 0)
        remaining = batch_size
        with self._pool.connection() as src:
            with transaction(dest._pool) as dconn:
                while remaining > 0 and kind_index < len(SNAPSHOT_KINDS):
                    kind = SNAPSHOT_KINDS[kind_index]
                    blob = src.execute(
                        """
                        SELECT seq, row_count, rows
                        FROM tenant_snapshot_blobs
                        WHERE snapshot_id = %s AND kind = %s AND seq = %s
                        """,
                        (snapshot_id, kind, blob_seq),
                    ).fetchone()
                    if blob is None:
                        kind_index += 1
                        blob_seq = 0
                        blob_offset = 0
                        continue
                    rows = blob["rows"] or []
                    if isinstance(rows, str):
                        rows = json.loads(rows)
                    chunk = list(rows)[blob_offset : blob_offset + remaining]
                    self._relocate_apply_kind(
                        dest, dconn, kind=kind, rows=chunk, tenant_id=tenant_id
                    )
                    blob_offset += len(chunk)
                    remaining -= max(1, len(chunk))
                    if blob_offset >= int(blob["row_count"] or len(rows)):
                        blob_seq += 1
                        blob_offset = 0
                        nxt = src.execute(
                            """
                            SELECT 1 FROM tenant_snapshot_blobs
                            WHERE snapshot_id = %s AND kind = %s AND seq = %s
                            """,
                            (snapshot_id, kind, blob_seq),
                        ).fetchone()
                        if nxt is None:
                            kind_index += 1
                            blob_seq = 0
                            blob_offset = 0
        done = kind_index >= len(SNAPSHOT_KINDS)
        next_stage = "verifying" if done else "applying"
        with transaction(self._pool) as conn:
            updated = conn.execute(
                """
                UPDATE tenant_relocations
                SET stage = %s,
                    kind_index = %s,
                    blob_seq = %s,
                    blob_offset = %s,
                    updated_at = %s,
                    error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (next_stage, kind_index, blob_seq, blob_offset, now, row["id"]),
            ).fetchone()
            assert updated is not None
            return relocate_from_row(updated)

    def _relocate_verify(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        dest = self._relocate_dest_queue(row)
        expected = dict(row.get("counts") or {})
        if isinstance(expected, str):
            expected = json.loads(expected)
        with dest._pool.connection() as dconn:
            for kind, want in expected.items():
                if kind not in _KIND_SQL:
                    continue
                _cols, table = _KIND_SQL[kind]
                if kind == "tenant_settings":
                    continue
                if kind == "inbound_trigger_nonces":
                    got = dconn.execute(
                        """
                        SELECT COUNT(*) AS n
                        FROM inbound_trigger_nonces n
                        JOIN inbound_triggers t ON t.id = n.trigger_id
                        WHERE t.tenant_id = %s
                        """,
                        (tenant_id,),
                    ).fetchone()["n"]
                else:
                    got = dconn.execute(
                        f"SELECT COUNT(*) AS n FROM {table} WHERE tenant_id = %s",
                        (tenant_id,),
                    ).fetchone()["n"]
                if int(got) < int(want or 0):
                    raise RelocateError(
                        f"dest {kind} count {got} < exported {want}"
                    )
                if kind == "payload_keys" and int(want or 0) > 0:
                    self._relocate_verify_payload_keys(
                        dest, dconn, tenant_id=tenant_id
                    )
        with transaction(self._pool) as conn:
            return self._set_relocate_stage(conn, row["id"], "cutover", now=now)

    def _relocate_cutover(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        dest = self._relocate_dest_queue(row)
        exported = None
        sid = row.get("snapshot_id")
        if sid is not None:
            with self._pool.connection() as src:
                blob = src.execute(
                    """
                    SELECT rows FROM tenant_snapshot_blobs
                    WHERE snapshot_id = %s AND kind = 'tenant_settings'
                    ORDER BY seq ASC LIMIT 1
                    """,
                    (sid,),
                ).fetchone()
                if blob is not None:
                    raw = blob["rows"] or []
                    if isinstance(raw, str):
                        raw = json.loads(raw)
                    if raw:
                        exported = raw[0]
        dest._finalize_restore_locked_on_pool(
            tenant_id=tenant_id,
            exported_settings=exported,
            now=now,
            paused=bool(row.get("source_paused")),
        )
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE tenant_settings
                SET paused = TRUE,
                    relocating = FALSE,
                    relocated = TRUE,
                    restoring = FALSE,
                    updated_at = %s
                WHERE tenant_id = %s
                """,
                (now, tenant_id),
            )
            return self._set_relocate_stage(conn, row["id"], "purging", now=now)

    def _relocate_purge(self, row: dict[str, Any], *, now: datetime) -> TenantRelocate:
        tenant_id = str(row["tenant_id"])
        with transaction(self._pool) as conn:
            if self._count_leased(conn, tenant_id) > 0:
                raise RelocateError("refusing to purge source while jobs are leased")
            if self._count_webhook_delivering(conn, tenant_id) > 0:
                raise RelocateError(
                    "refusing to purge source while webhooks are delivering"
                )
            if hasattr(self, "_count_running_activities"):
                if int(self._count_running_activities(conn, tenant_id) or 0) > 0:
                    raise RelocateError(
                        "refusing to purge source while workflow activities "
                        "are running"
                    )
            self._purge_relocated_source(conn, tenant_id=tenant_id, now=now)
            updated = conn.execute(
                """
                UPDATE tenant_relocations
                SET status = 'complete',
                    stage = 'complete',
                    updated_at = %s,
                    completed_at = %s,
                    error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (now, now, row["id"]),
            ).fetchone()
            assert updated is not None
            return relocate_from_row(updated)

    def _purge_relocated_source(
        self, conn: Any, *, tenant_id: str, now: datetime
    ) -> None:
        """Wipe source tenant data but keep the relocate tombstone."""
        _ = now
        conn.execute("DELETE FROM job_dag_batches WHERE tenant_id = %s", (tenant_id,))
        if hasattr(self, "_orphan_blobs_for_jobs"):
            ids = conn.execute(
                "SELECT id FROM jobs WHERE tenant_id = %s", (tenant_id,)
            ).fetchall()
            self._orphan_blobs_for_jobs(conn, [r["id"] for r in ids])
        if hasattr(self, "_purge_tenant_blobs"):
            self._purge_tenant_blobs(conn, tenant_id)
        elif hasattr(self, "_orphan_blobs_for_tenant"):
            self._orphan_blobs_for_tenant(conn, tenant_id)
        conn.execute("DELETE FROM jobs WHERE tenant_id = %s", (tenant_id,))
        conn.execute(
            "DELETE FROM schedule_blackouts WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM schedule_generations WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute("DELETE FROM workflow_signals WHERE tenant_id = %s", (tenant_id,))
        conn.execute(
            "DELETE FROM workflow_step_runs WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute("DELETE FROM workflow_runs WHERE tenant_id = %s", (tenant_id,))
        conn.execute("DELETE FROM schedules WHERE tenant_id = %s", (tenant_id,))
        conn.execute(
            "DELETE FROM webhook_deliveries WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM webhook_endpoints WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM inbound_job_callbacks WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute("DELETE FROM inbound_triggers WHERE tenant_id = %s", (tenant_id,))
        conn.execute("DELETE FROM tenant_fairness WHERE tenant_id = %s", (tenant_id,))
        conn.execute(
            "DELETE FROM payload_key_rotations WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM tenant_payload_keys WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM job_audit_events WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM job_side_effects WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM duration_estimates WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM job_worker_affinity WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute("DELETE FROM alert_events WHERE tenant_id = %s", (tenant_id,))
        conn.execute("DELETE FROM alert_rules WHERE tenant_id = %s", (tenant_id,))
        conn.execute(
            "DELETE FROM queue_metric_buckets WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM queue_metric_snapshots WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM tenant_usage_buckets WHERE tenant_id = %s", (tenant_id,)
        )
        conn.execute(
            "DELETE FROM tenant_restores WHERE source_tenant_id = %s OR target_tenant_id = %s",
            (tenant_id, tenant_id),
        )
        conn.execute(
            "DELETE FROM tenant_snapshots WHERE source_tenant_id = %s",
            (tenant_id,),
        )
        conn.execute(
            """
            UPDATE tenant_settings
            SET paused = TRUE,
                relocating = FALSE,
                relocated = TRUE,
                restoring = FALSE,
                offboarding = FALSE,
                max_concurrent_leases = NULL,
                max_pending_jobs = NULL,
                updated_at = NOW()
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        )
        crypto = getattr(self, "_crypto", None)
        if crypto is not None:
            crypto.drop_tenant(tenant_id)

    def _wipe_relocate_dest_copy(self, dest: Any, tenant_id: str) -> None:
        """Remove a half-applied dest copy. Other dest tenants stay."""
        with transaction(dest._pool) as conn:
            dest._purge_relocated_source(conn, tenant_id=tenant_id, now=_utcnow())
            conn.execute(
                "DELETE FROM tenant_settings WHERE tenant_id = %s",
                (tenant_id,),
            )
            conn.execute(
                "DELETE FROM tenant_relocations WHERE tenant_id = %s",
                (tenant_id,),
            )
            crypto = getattr(dest, "_crypto", None)
            if crypto is not None:
                crypto.drop_tenant(tenant_id)

    def _prepare_dest_tenant(
        self, dest: Any, tenant_id: str, *, resuming: bool
    ) -> None:
        now = _utcnow()
        with transaction(dest._pool) as conn:
            leftover = relocate_residue_tables(conn, tenant_id)
            restoring = dest._is_tenant_restoring(conn, tenant_id)
            relocated = dest._is_tenant_relocated(conn, tenant_id)
            if leftover and not restoring and not resuming:
                raise RelocateError(
                    f"dest tenant {tenant_id!r} is not empty; leftover: "
                    + ", ".join(leftover),
                    fatal=True,
                )
            if relocated and leftover:
                raise RelocateError(
                    f"dest tenant {tenant_id!r} already exists and is not a relocate target"
                )
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, restoring, relocating, relocated,
                    updated_at
                ) VALUES (%s, NULL, NULL, TRUE, FALSE, TRUE, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    restoring = TRUE,
                    relocating = FALSE,
                    relocated = FALSE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )

    def _relocate_apply_kind(
        self,
        dest: Any,
        dconn: Any,
        *,
        kind: str,
        rows: list[Any],
        tenant_id: str,
    ) -> None:
        if kind == "tenant_settings":
            return
        if kind == "payload_blobs":
            self._relocate_apply_blobs(dest, dconn, rows, tenant_id)
            return
        if kind == "payload_keys":
            self._relocate_apply_payload_keys(dest, dconn, rows, tenant_id)
            return
        if kind not in _KIND_SQL:
            return
        cols, table = _KIND_SQL[kind]
        col_names = [c.strip() for c in cols.split(",") if c.strip()]
        placeholders = ", ".join(["%s"] * len(col_names))
        sql = (
            f"INSERT INTO {table} ({', '.join(col_names)}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        )
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            values = [_sql_value(c, raw.get(c)) for c in col_names]
            dconn.execute(sql, values)

    def _relocate_apply_payload_keys(
        self, dest: Any, dconn: Any, rows: list[Any], tenant_id: str
    ) -> None:
        """Unwrap source DEKs and rewrap under dest's active KEK.

        Ciphertext stays as-is (same key_id / AAD). If source cannot unwrap
        a DEK, dest would be unreadable — fail before cutover.
        """
        src_crypto = self._require_crypto()
        dest_crypto = dest._require_crypto()
        for raw in rows:
            if not isinstance(raw, dict) or not raw.get("key_id"):
                continue
            key_id = UUID(str(raw["key_id"]))
            wrapped = _sql_value("wrapped_dek", raw.get("wrapped_dek"))
            nonce = _sql_value("wrap_nonce", raw.get("wrap_nonce"))
            kek_id = str(raw.get("kek_id") or "")
            try:
                dek = src_crypto.unwrap_stored_dek(
                    tenant_id=tenant_id,
                    key_id=key_id,
                    wrapped=bytes(wrapped or b""),
                    nonce=bytes(nonce or b""),
                    kek_id=kek_id,
                )
            except Exception as exc:  # noqa: BLE001
                raise RelocateError(
                    f"cannot unwrap payload key {key_id} for tenant "
                    f"{tenant_id!r} (kek_id={kek_id!r}); dest would be "
                    f"unreadable: {exc}",
                    fatal=True,
                ) from exc
            new_wrapped, new_nonce = wrap_dek(
                dest_crypto.active_kek,
                dek,
                tenant_id=tenant_id,
                key_id=key_id,
            )
            dest_crypto.cache_dek(tenant_id, key_id, dek)
            dconn.execute(
                """
                INSERT INTO tenant_payload_keys (
                    tenant_id, key_id, wrapped_dek, wrap_nonce, kek_id,
                    status, created_at, retired_at
                ) VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s)
                ON CONFLICT (tenant_id, key_id) DO UPDATE
                SET wrapped_dek = EXCLUDED.wrapped_dek,
                    wrap_nonce = EXCLUDED.wrap_nonce,
                    kek_id = EXCLUDED.kek_id,
                    status = EXCLUDED.status,
                    retired_at = EXCLUDED.retired_at
                """,
                (
                    tenant_id,
                    key_id,
                    new_wrapped,
                    new_nonce,
                    dest_crypto.active_kek_id,
                    raw.get("status") or "decrypt_only",
                    _parse_dt(raw.get("created_at")),
                    _parse_dt(raw.get("retired_at")),
                ),
            )

    def _relocate_verify_payload_keys(
        self, dest: Any, dconn: Any, *, tenant_id: str
    ) -> None:
        dest_crypto = dest._require_crypto()
        rows = dconn.execute(
            """
            SELECT key_id, wrapped_dek, wrap_nonce, kek_id
            FROM tenant_payload_keys
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
        for raw in rows:
            try:
                dest_crypto.unwrap_stored_dek(
                    tenant_id=tenant_id,
                    key_id=raw["key_id"],
                    wrapped=bytes(raw["wrapped_dek"] or b""),
                    nonce=bytes(raw["wrap_nonce"] or b""),
                    kek_id=str(raw["kek_id"] or ""),
                )
            except Exception as exc:  # noqa: BLE001
                raise RelocateError(
                    f"destination cannot open payload key {raw['key_id']} "
                    f"for tenant {tenant_id!r} (kek_id={raw['kek_id']!r}): {exc}",
                    fatal=True,
                ) from exc

    def _relocate_apply_blobs(
        self, dest: Any, dconn: Any, rows: list[Any], tenant_id: str
    ) -> None:
        store = getattr(dest, "_blob_store", None)
        for raw in rows:
            if not isinstance(raw, dict) or not raw.get("id"):
                continue
            bid = UUID(str(raw["id"]))
            data_b64 = raw.get("data_b64") or ""
            status = str(raw.get("status") or "committed")
            if store is not None and data_b64:
                store.write(bid, base64.b64decode(str(data_b64)))
            elif not data_b64:
                status = "orphan"
            dconn.execute(
                """
                INSERT INTO job_payload_blobs (
                    id, tenant_id, job_id, kind, sha256, nbytes, key_id,
                    status, created_at, committed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, COALESCE(%s, NOW()), %s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    bid,
                    tenant_id,
                    raw.get("job_id"),
                    raw.get("kind") or "other",
                    raw.get("sha256") or "",
                    int(raw.get("nbytes") or 0),
                    raw.get("key_id"),
                    status,
                    _parse_dt(raw.get("created_at")),
                    _parse_dt(raw.get("committed_at")),
                ),
            )

    def _relocate_dest_crypto(
        self,
        *,
        dest_kek: str | None,
        dest_kek_id: str | None,
        dest_keks: dict[str, str] | None,
    ) -> PayloadCrypto:
        if dest_kek or dest_keks:
            env: dict[str, str] = {}
            if dest_kek:
                env["JOB_QUEUE_PAYLOAD_KEK"] = dest_kek
            if dest_kek_id:
                env["JOB_QUEUE_PAYLOAD_KEK_ID"] = dest_kek_id
            if dest_keks:
                env["JOB_QUEUE_PAYLOAD_KEKS"] = json.dumps(dest_keks)
            return PayloadCrypto.from_env(env)
        return self._require_crypto()

    def _relocate_dest_queue(self, row: dict[str, Any]) -> Any:
        from job_queue.queue.core import JobQueue

        dest_dsn = str(row["dest_dsn"])
        dest_blob_dir = row.get("dest_blob_dir")
        dest_keks = row.get("dest_keks")
        if isinstance(dest_keks, str):
            dest_keks = json.loads(dest_keks)
        if dest_keks is not None:
            dest_keks = {str(k): str(v) for k, v in dict(dest_keks).items()}
        dest_crypto = self._relocate_dest_crypto(
            dest_kek=row.get("dest_kek"),
            dest_kek_id=row.get("dest_kek_id"),
            dest_keks=dest_keks,
        )
        key = (dest_dsn, dest_blob_dir or "", dest_crypto.active_kek_id)
        cache = getattr(self, "_relocate_dests", None)
        if cache is None:
            self._relocate_dests = {}
            cache = self._relocate_dests
        cached = cache.get(key)
        if cached is not None:
            return cached
        q = JobQueue.connect(
            dest_dsn,
            min_size=1,
            max_size=4,
            require_worker_registration=False,
        )
        q._crypto = dest_crypto
        if dest_blob_dir:
            q._blob_store = FilesystemBlobStore(dest_blob_dir)
        cache[key] = q
        return q

    def _finalize_restore_locked_on_pool(
        self,
        *,
        tenant_id: str,
        exported_settings: Any,
        now: datetime,
        paused: bool = False,
    ) -> None:
        with transaction(self._pool) as conn:
            live_settings = dict(exported_settings or {})
            # Relocate itself pauses source for a consistent snapshot.
            # Dest must keep the pre-relocate pause flag, not that freeze.
            live_settings["paused"] = bool(paused)
            live_settings["relocating"] = False
            live_settings["relocated"] = False
            live_settings["restoring"] = False
            self._finalize_restore_locked(
                conn,
                target_tenant_id=tenant_id,
                exported_settings=live_settings,
                now=now,
            )
            conn.execute(
                """
                UPDATE tenant_settings
                SET paused = %s,
                    relocating = FALSE,
                    relocated = FALSE,
                    restoring = FALSE,
                    updated_at = %s
                WHERE tenant_id = %s
                """,
                (bool(paused), now, tenant_id),
            )

    def _set_relocate_stage(
        self,
        conn: Any,
        relocate_id: UUID,
        stage: str,
        *,
        now: datetime,
        snapshot_id: UUID | None = None,
    ) -> TenantRelocate:
        if snapshot_id is None:
            updated = conn.execute(
                """
                UPDATE tenant_relocations
                SET stage = %s, updated_at = %s, error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (stage, now, relocate_id),
            ).fetchone()
        else:
            updated = conn.execute(
                """
                UPDATE tenant_relocations
                SET stage = %s, snapshot_id = %s, updated_at = %s, error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (stage, snapshot_id, now, relocate_id),
            ).fetchone()
        assert updated is not None
        return relocate_from_row(updated)

    def _mark_relocate_error(self, relocate_id: UUID, error: str) -> None:
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE tenant_relocations
                SET error = %s, updated_at = %s
                WHERE id = %s
                """,
                (error, _utcnow(), relocate_id),
            )
