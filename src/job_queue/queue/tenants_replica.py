"""Live shadow replica of one tenant onto another Postgres.

Source keeps serving while dest stays paused+restoring and catches up.
Cutover freezes source, does a final reconcile, then dest is the only
live side. A coordinator kill leaves a committed cursor; dest is never
unpaused while source is still accepting writes.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import TenantReplica
from job_queue.queue._common import ReplicaError, RelocateError, _utcnow
from job_queue.queue.tenants_export import (
    KIND_PK,
    RELOCATE_SNAPSHOT_KINDS,
    SNAPSHOT_FORMAT_VERSION,
    _KIND_SQL,
    relocate_residue_tables,
)
from job_queue.queue.tenants_relocate import _redact_dest_dsn


REPLICA_STAGES: tuple[str, ...] = (
    "bootstrapping",
    "catching_up",
    "freezing",
    "verifying",
    "cutover",
    "purging",
    "complete",
)


def replica_from_row(row: dict[str, Any]) -> TenantReplica:
    counts = row.get("counts") or {}
    if isinstance(counts, str):
        counts = json.loads(counts)
    dest_dsn = str(row["dest_dsn"])
    if str(row["status"]) in ("complete", "aborted", "failed"):
        dest_dsn = _redact_dest_dsn(dest_dsn)
    return TenantReplica(
        id=row["id"],
        tenant_id=str(row["tenant_id"]),
        dest_dsn=dest_dsn,
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
        blob_seq=int(row.get("blob_seq") or 0),
        blob_offset=int(row.get("blob_offset") or 0),
        catchup_rounds=int(row.get("catchup_rounds") or 0),
        cutover_requested=bool(row.get("cutover_requested")),
    )


class TenantsReplicaMixin:
    def replicate_tenant(
        self,
        tenant_id: str,
        *,
        dest_dsn: str | None = None,
        dest_blob_dir: str | None = None,
        dest_kek: str | None = None,
        dest_kek_id: str | None = None,
        dest_keks: dict[str, str] | None = None,
        batch_size: int = 100,
        min_catchup_rounds: int = 1,
        run_until_complete: bool = False,
    ) -> TenantReplica:
        """Start or resume a shadow replica of *tenant_id* onto *dest_dsn*.

        Source keeps serving. Dest is paused+restoring until cutover.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if min_catchup_rounds < 0:
            raise ValueError("min_catchup_rounds must be >= 0")
        rst = self._ensure_replica(
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
            return self._run_replica_until_complete(
                tenant_id, batch_size=batch_size, min_catchup_rounds=min_catchup_rounds
            )
        return self.advance_tenant_replica(tenant_id, batch_size=batch_size)

    def get_tenant_replica(self, tenant_id: str) -> TenantReplica | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM tenant_replicas
                WHERE tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            return replica_from_row(row) if row else None

    def advance_tenant_replica(
        self, tenant_id: str, *, batch_size: int = 100, wait: bool = True
    ) -> TenantReplica:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        now = _utcnow()
        fatal: ReplicaError | None = None
        with self._hold_replica_lock(tenant_id, wait=wait) as held:
            if not held:
                current = self.get_tenant_replica(tenant_id)
                if current is None:
                    raise ReplicaError(
                        f"no replica in progress for tenant {tenant_id!r}"
                    )
                return current
            with transaction(self._pool) as conn:
                row = conn.execute(
                    """
                    SELECT * FROM tenant_replicas
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
                        SELECT * FROM tenant_replicas
                        WHERE tenant_id = %s
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (tenant_id,),
                    ).fetchone()
                    if latest is None:
                        raise ReplicaError(
                            f"no replica in progress for tenant {tenant_id!r}"
                        )
                    return replica_from_row(latest)
            try:
                return self._advance_replica_stage(
                    row, batch_size=batch_size, now=now
                )
            except ReplicaError as exc:
                if exc.fatal:
                    fatal = exc
                else:
                    raise
            except RelocateError as exc:
                if exc.fatal:
                    fatal = ReplicaError(str(exc), fatal=True)
                else:
                    raise ReplicaError(str(exc)) from exc
            except Exception as exc:  # noqa: BLE001
                self._mark_replica_error(row["id"], str(exc)[:2000])
                raise ReplicaError(
                    f"replica failed for tenant {tenant_id!r}: {exc}"
                ) from exc
        if fatal is not None:
            self.abort_tenant_replica(tenant_id, error=str(fatal))
            raise fatal
        raise ReplicaError(f"replica failed for tenant {tenant_id!r}")

    def cutover_tenant_replica(
        self, tenant_id: str, *, batch_size: int = 100
    ) -> TenantReplica:
        """Request cutover and drive until dest is the only live side."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET cutover_requested = TRUE, updated_at = %s
                WHERE tenant_id = %s AND status = 'running'
                RETURNING *
                """,
                (_utcnow(), tenant_id),
            ).fetchone()
        if updated is None:
            current = self.get_tenant_replica(tenant_id)
            if current is None:
                raise ReplicaError(
                    f"no replica in progress for tenant {tenant_id!r}"
                )
            if current.status == "complete":
                return current
            raise ReplicaError(
                f"no replica in progress for tenant {tenant_id!r}"
            )
        return self._run_replica_until_complete(
            tenant_id, batch_size=batch_size, min_catchup_rounds=0
        )

    def process_tenant_replicas(
        self, *, limit: int = 10, batch_size: int = 100
    ) -> list[TenantReplica]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT tenant_id FROM tenant_replicas
                WHERE status = 'running'
                ORDER BY updated_at ASC
                LIMIT %s
                """,
                (int(limit),),
            ).fetchall()
        out: list[TenantReplica] = []
        for r in rows:
            try:
                out.append(
                    self.advance_tenant_replica(
                        str(r["tenant_id"]), batch_size=batch_size, wait=False
                    )
                )
            except ReplicaError:
                continue
        return out

    def abort_tenant_replica(
        self, tenant_id: str, *, error: str | None = None
    ) -> TenantReplica:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        dest_row: dict[str, Any] | None = None
        wipe_dest = False
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"replica:{tenant_id}",),
            )
            row = conn.execute(
                """
                SELECT * FROM tenant_replicas
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
                    SELECT * FROM tenant_replicas
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (tenant_id,),
                ).fetchone()
                if latest is None:
                    raise ReplicaError(
                        f"no replica in progress for tenant {tenant_id!r}"
                    )
                return replica_from_row(latest)
            stage = str(row["stage"])
            if stage in ("cutover", "purging", "complete"):
                raise ReplicaError(
                    f"tenant {tenant_id!r} already cut over; abort is refused"
                )
            if self._is_tenant_relocating(conn, tenant_id):
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
                conn.execute("DELETE FROM tenant_snapshots WHERE id = %s", (sid,))
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET status = 'aborted',
                    dest_dsn = %s,
                    dest_kek = NULL,
                    dest_keks = NULL,
                    error = %s,
                    updated_at = %s,
                    completed_at = %s
                WHERE id = %s
                RETURNING *
                """,
                (
                    _redact_dest_dsn(str(row["dest_dsn"])),
                    (error or row.get("error") or "aborted")[:2000],
                    now,
                    now,
                    row["id"],
                ),
            ).fetchone()
            assert updated is not None
            dest_row = dict(row)
            wipe_dest = stage in (
                "bootstrapping",
                "catching_up",
                "freezing",
                "verifying",
            )
            out = replica_from_row(updated)
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

    def _run_replica_until_complete(
        self,
        tenant_id: str,
        *,
        batch_size: int,
        min_catchup_rounds: int,
    ) -> TenantReplica:
        last_sig: tuple[Any, ...] | None = None
        stalled = 0
        while True:
            rst = self.advance_tenant_replica(tenant_id, batch_size=batch_size)
            if rst.status in ("complete", "failed", "aborted"):
                return rst
            if (
                rst.stage == "catching_up"
                and rst.catchup_rounds >= min_catchup_rounds
                and not rst.cutover_requested
            ):
                with transaction(self._pool) as conn:
                    conn.execute(
                        """
                        UPDATE tenant_replicas
                        SET cutover_requested = TRUE, updated_at = %s
                        WHERE tenant_id = %s AND status = 'running'
                        """,
                        (_utcnow(), tenant_id),
                    )
            sig = (
                rst.stage,
                rst.kind_index,
                rst.blob_seq,
                rst.blob_offset,
                rst.snapshot_id,
                rst.catchup_rounds,
                rst.cutover_requested,
            )
            if rst.stage == "freezing" and rst.kind_index == 0:
                last_sig = sig
                stalled = 0
                continue
            if sig == last_sig:
                stalled += 1
                if stalled >= 32:
                    raise ReplicaError(
                        "replica did not complete; cursor did not advance"
                    )
            else:
                stalled = 0
            last_sig = sig

    @contextmanager
    def _hold_replica_lock(
        self, tenant_id: str, *, wait: bool = True
    ) -> Iterator[bool]:
        key = f"replica:{tenant_id}"
        with self._pool.connection() as conn:
            if wait:
                conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (key,))
                held = True
            else:
                row = conn.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s)) AS ok",
                    (key,),
                ).fetchone()
                held = bool(row and row["ok"])
            conn.commit()
            try:
                yield held
            finally:
                if held:
                    conn.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))", (key,)
                    )
                    conn.commit()

    def _ensure_replica(
        self,
        tenant_id: str,
        *,
        dest_dsn: str | None,
        dest_blob_dir: str | None,
        dest_kek: str | None = None,
        dest_kek_id: str | None = None,
        dest_keks: dict[str, str] | None = None,
    ) -> TenantReplica:
        now = _utcnow()
        dest_dsn = (dest_dsn or os.environ.get("JOB_QUEUE_REPLICA_DEST_DSN") or os.environ.get("JOB_QUEUE_RELOCATE_DEST_DSN") or "").strip()
        if dest_blob_dir is None:
            raw_dir = (
                os.environ.get("JOB_QUEUE_REPLICA_DEST_BLOB_DIR")
                or os.environ.get("JOB_QUEUE_RELOCATE_DEST_BLOB_DIR")
            )
            dest_blob_dir = raw_dir.strip() if raw_dir else None
        if dest_kek is None:
            dest_kek = os.environ.get("JOB_QUEUE_REPLICA_DEST_KEK") or os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEK") or None
        if dest_kek_id is None:
            dest_kek_id = os.environ.get("JOB_QUEUE_REPLICA_DEST_KEK_ID") or os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEK_ID") or None
        if dest_keks is None:
            raw_keks = os.environ.get("JOB_QUEUE_REPLICA_DEST_KEKS") or os.environ.get("JOB_QUEUE_RELOCATE_DEST_KEKS")
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
            raise ReplicaError("dest_dsn must be a different Postgres")
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
                (f"replica:{tenant_id}",),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ReplicaError(
                    f"tenant {tenant_id!r} is offboarding and cannot replicate"
                )
            if self._is_tenant_relocated(conn, tenant_id):
                raise ReplicaError(
                    f"tenant {tenant_id!r} has already been relocated"
                )
            running_rel = conn.execute(
                """
                SELECT 1 FROM tenant_relocations
                WHERE tenant_id = %s AND status = 'running'
                LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            if running_rel is not None:
                raise ReplicaError(
                    f"tenant {tenant_id!r} is already relocating"
                )
            existing = conn.execute(
                """
                SELECT * FROM tenant_replicas
                WHERE tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            if existing is not None and existing["status"] == "running":
                if str(existing["dest_dsn"]) != dest_dsn:
                    raise ReplicaError(
                        f"tenant {tenant_id!r} already replicating to a different dest"
                    )
                return replica_from_row(existing)
            if existing is not None and existing["status"] == "complete":
                return replica_from_row(existing)
            with dest._pool.connection() as dconn:
                leftover = relocate_residue_tables(dconn, tenant_id)
            if leftover:
                raise ReplicaError(
                    f"dest tenant {tenant_id!r} is not empty; leftover: "
                    + ", ".join(leftover),
                    fatal=True,
                )
            row = conn.execute(
                """
                INSERT INTO tenant_replicas (
                    id, tenant_id, dest_dsn, dest_blob_dir, status, stage,
                    kind_index, blob_seq, blob_offset, catchup_rounds,
                    cutover_requested, counts,
                    dest_kek_id, dest_kek, dest_keks,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, 'running', 'bootstrapping',
                    0, 0, 0, 0,
                    FALSE, '{}'::jsonb,
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
            return replica_from_row(row)

    def _advance_replica_stage(
        self, row: dict[str, Any], *, batch_size: int, now: datetime
    ) -> TenantReplica:
        stage = str(row["stage"])
        if stage == "bootstrapping":
            return self._replica_copy(
                row, batch_size=batch_size, now=now, upsert=False, next_idle="catching_up"
            )
        if stage == "catching_up":
            if row.get("cutover_requested") and row.get("snapshot_id") is None:
                return self._replica_begin_freeze(row, now=now)
            return self._replica_copy(
                row,
                batch_size=batch_size,
                now=now,
                upsert=True,
                next_idle="catching_up",
            )
        if stage == "freezing":
            return self._replica_freeze(row, batch_size=batch_size, now=now)
        if stage == "verifying":
            return self._replica_verify(row, now=now)
        if stage == "cutover":
            return self._replica_cutover(row, now=now)
        if stage == "purging":
            return self._replica_purge(row, now=now)
        if stage == "complete":
            return replica_from_row(row)
        raise ReplicaError(f"unknown replica stage {stage!r}")

    def _replica_begin_freeze(
        self, row: dict[str, Any], *, now: datetime
    ) -> TenantReplica:
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
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET stage = 'freezing',
                    source_paused = %s,
                    kind_index = 0,
                    blob_seq = 0,
                    blob_offset = 0,
                    snapshot_id = NULL,
                    updated_at = %s,
                    error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (was_paused, now, row["id"]),
            ).fetchone()
            assert updated is not None
            return replica_from_row(updated)

    def _replica_freeze(
        self, row: dict[str, Any], *, batch_size: int, now: datetime
    ) -> TenantReplica:
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
                    UPDATE tenant_replicas
                    SET updated_at = %s, error = NULL
                    WHERE id = %s
                    RETURNING *
                    """,
                    (now, row["id"]),
                ).fetchone()
                assert updated is not None
                return replica_from_row(updated)
        return self._replica_copy(
            row, batch_size=batch_size, now=now, upsert=True, next_idle="verifying"
        )

    def _replica_copy(
        self,
        row: dict[str, Any],
        *,
        batch_size: int,
        now: datetime,
        upsert: bool,
        next_idle: str,
    ) -> TenantReplica:
        """One export or apply page. Source stays live unless already frozen."""
        tenant_id = str(row["tenant_id"])
        sid = row.get("snapshot_id")
        kind_index = int(row["kind_index"] or 0)
        if sid is not None:
            snap = self.get_tenant_snapshot(sid)
            if snap is not None and snap.status == "ready":
                return self._replica_apply_page(
                    row, batch_size=batch_size, now=now, upsert=upsert, next_idle=next_idle
                )
        with transaction(self._pool) as conn:
            if sid is None:
                sid = uuid4()
                conn.execute(
                    """
                    INSERT INTO tenant_snapshots (
                        id, source_tenant_id, format_version, status, counts,
                        created_at
                    ) VALUES (
                        %s, %s, %s, 'capturing', '{}'::jsonb, %s
                    )
                    """,
                    (sid, tenant_id, SNAPSHOT_FORMAT_VERSION, now),
                )
                claimed = conn.execute(
                    """
                    UPDATE tenant_replicas
                    SET snapshot_id = %s, kind_index = 0, blob_seq = 0,
                        blob_offset = 0, updated_at = %s
                    WHERE id = %s AND snapshot_id IS NULL
                    RETURNING id
                    """,
                    (sid, now, row["id"]),
                ).fetchone()
                if claimed is None:
                    conn.execute(
                        "DELETE FROM tenant_snapshots WHERE id = %s", (sid,)
                    )
                    out = conn.execute(
                        "SELECT * FROM tenant_replicas WHERE id = %s",
                        (row["id"],),
                    ).fetchone()
                    assert out is not None
                    return replica_from_row(out)
                kind_index = 0
            wrote = False
            seq = 0
            while kind_index < len(RELOCATE_SNAPSHOT_KINDS):
                kind = RELOCATE_SNAPSHOT_KINDS[kind_index]
                seq, after = self._export_kind_resume(
                    conn, snapshot_id=sid, kind=kind
                )
                n, _after = self._export_kind_page(
                    conn,
                    snapshot_id=sid,
                    kind=kind,
                    tenant_id=tenant_id,
                    after=after,
                    limit=max(1, int(batch_size)),
                    seq=seq,
                    include_blob_bytes=False,
                    hydrate_sealed=False,
                )
                if n == 0:
                    kind_index += 1
                    continue
                wrote = True
                break
            counts = self._snapshot_kind_counts(conn, sid)
            if kind_index >= len(RELOCATE_SNAPSHOT_KINDS):
                conn.execute(
                    """
                    UPDATE tenant_snapshots
                    SET status = 'ready', counts = %s, completed_at = %s
                    WHERE id = %s
                    """,
                    (Jsonb(counts), now, sid),
                )
                updated = conn.execute(
                    """
                    UPDATE tenant_replicas
                    SET snapshot_id = %s, counts = %s, kind_index = 0,
                        blob_seq = 0, blob_offset = 0, updated_at = %s,
                        error = NULL
                    WHERE id = %s
                    RETURNING *
                    """,
                    (sid, Jsonb(counts), now, row["id"]),
                ).fetchone()
            else:
                updated = conn.execute(
                    """
                    UPDATE tenant_replicas
                    SET snapshot_id = %s, counts = %s, kind_index = %s,
                        blob_seq = %s, blob_offset = 0, updated_at = %s,
                        error = NULL
                    WHERE id = %s
                    RETURNING *
                    """,
                    (
                        sid,
                        Jsonb(counts),
                        kind_index,
                        (seq + 1) if wrote else 0,
                        now,
                        row["id"],
                    ),
                ).fetchone()
            assert updated is not None
            return replica_from_row(updated)

    def _replica_apply_page(
        self,
        row: dict[str, Any],
        *,
        batch_size: int,
        now: datetime,
        upsert: bool,
        next_idle: str,
    ) -> TenantReplica:
        tenant_id = str(row["tenant_id"])
        snapshot_id = row.get("snapshot_id")
        if snapshot_id is None:
            raise ReplicaError("replica snapshot missing")
        dest = self._relocate_dest_queue(row)
        orig_kind = int(row["kind_index"] or 0)
        orig_seq = int(row["blob_seq"] or 0)
        orig_off = int(row["blob_offset"] or 0)
        self._prepare_dest_tenant(
            dest, tenant_id, resuming=orig_kind > 0 or int(row.get("catchup_rounds") or 0) > 0
        )
        kind_index = orig_kind
        blob_seq = orig_seq
        blob_offset = orig_off
        remaining = batch_size
        with self._pool.connection() as src:
            with transaction(dest._pool) as dconn:
                while remaining > 0 and kind_index < len(RELOCATE_SNAPSHOT_KINDS):
                    kind = RELOCATE_SNAPSHOT_KINDS[kind_index]
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
                        dest,
                        dconn,
                        kind=kind,
                        rows=chunk,
                        tenant_id=tenant_id,
                        upsert=upsert,
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
        done = kind_index >= len(RELOCATE_SNAPSHOT_KINDS)
        catchup_rounds = int(row.get("catchup_rounds") or 0)
        cutover_requested = bool(row.get("cutover_requested"))
        next_stage = str(row["stage"])
        next_snap = snapshot_id
        next_kind = kind_index
        next_seq = blob_seq
        next_off = blob_offset
        if done:
            if str(row["stage"]) == "bootstrapping":
                next_stage = "catching_up"
                catchup_rounds = 0
            elif str(row["stage"]) == "catching_up":
                catchup_rounds += 1
                next_stage = "freezing" if cutover_requested else "catching_up"
            else:
                next_stage = next_idle
            if next_stage != str(row["stage"]) or next_stage == "catching_up":
                if next_stage in ("catching_up", "verifying", "freezing"):
                    next_snap = None
                    next_kind = 0
                    next_seq = 0
                    next_off = 0
                    with transaction(self._pool) as conn:
                        conn.execute(
                            "DELETE FROM tenant_snapshots WHERE id = %s",
                            (snapshot_id,),
                        )
            if next_stage == "catching_up" and cutover_requested:
                next_stage = "freezing"
        with transaction(self._pool) as conn:
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET stage = %s,
                    snapshot_id = %s,
                    kind_index = %s,
                    blob_seq = %s,
                    blob_offset = %s,
                    catchup_rounds = %s,
                    updated_at = %s,
                    error = NULL
                WHERE id = %s
                  AND kind_index = %s
                  AND blob_seq = %s
                  AND blob_offset = %s
                RETURNING *
                """,
                (
                    next_stage,
                    next_snap,
                    next_kind,
                    next_seq,
                    next_off,
                    catchup_rounds,
                    now,
                    row["id"],
                    orig_kind,
                    orig_seq,
                    orig_off,
                ),
            ).fetchone()
            if updated is None:
                out = conn.execute(
                    "SELECT * FROM tenant_replicas WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                assert out is not None
                return replica_from_row(out)
            return replica_from_row(updated)

    def _replica_verify(self, row: dict[str, Any], *, now: datetime) -> TenantReplica:
        tenant_id = str(row["tenant_id"])
        dest = self._relocate_dest_queue(row)
        with self._pool.connection() as src:
            with dest._pool.connection() as dconn:
                for kind in RELOCATE_SNAPSHOT_KINDS:
                    if kind in ("tenant_settings",):
                        continue
                    want = self._replica_count_kind(src, kind, tenant_id)
                    got = self._replica_count_kind(dconn, kind, tenant_id)
                    if int(got) < int(want):
                        raise ReplicaError(
                            f"dest {kind} count {got} < source {want}"
                        )
                    if kind == "payload_keys" and int(want) > 0:
                        self._relocate_verify_payload_keys(
                            dest, dconn, tenant_id=tenant_id
                        )
                self._replica_reconcile_deletes(src, dest, dconn, tenant_id)
        with transaction(self._pool) as conn:
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET stage = 'cutover', updated_at = %s, error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (now, row["id"]),
            ).fetchone()
            assert updated is not None
            return replica_from_row(updated)

    def _replica_count_kind(self, conn: Any, kind: str, tenant_id: str) -> int:
        if kind not in _KIND_SQL:
            return 0
        _cols, table = _KIND_SQL[kind]
        if kind == "inbound_trigger_nonces":
            row = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM inbound_trigger_nonces n
                JOIN inbound_triggers t ON t.id = n.trigger_id
                WHERE t.tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
        elif kind == "tenant_snapshots":
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM tenant_snapshots WHERE source_tenant_id = %s",
                (tenant_id,),
            ).fetchone()
        elif kind == "tenant_snapshot_blobs":
            row = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM tenant_snapshot_blobs b
                JOIN tenant_snapshots s ON s.id = b.snapshot_id
                WHERE s.source_tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
        elif kind == "tenant_restores":
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM tenant_restores
                WHERE source_tenant_id = %s OR target_tenant_id = %s
                """,
                (tenant_id, tenant_id),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchone()
        return int(row["n"] or 0)

    def _replica_reconcile_deletes(
        self, src: Any, dest: Any, dconn: Any, tenant_id: str
    ) -> None:
        """Drop dest rows the frozen source no longer has."""
        for kind, pk in KIND_PK.items():
            if kind in (
                "tenant_settings",
                "inbound_trigger_nonces",
                "tenant_snapshots",
                "tenant_snapshot_blobs",
                "tenant_restores",
            ):
                continue
            if kind not in _KIND_SQL:
                continue
            _cols, table = _KIND_SQL[kind]
            if "tenant_id" not in _cols:
                continue
            src_rows = src.execute(
                f"SELECT {', '.join(pk)} FROM {table} WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchall()
            keep = {tuple(r[c] for c in pk) for r in src_rows}
            dest_rows = dconn.execute(
                f"SELECT {', '.join(pk)} FROM {table} WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchall()
            for r in dest_rows:
                key = tuple(r[c] for c in pk)
                if key in keep:
                    continue
                wh = " AND ".join(f"{c} = %s" for c in pk)
                dconn.execute(f"DELETE FROM {table} WHERE {wh}", key)

    def _replica_cutover(self, row: dict[str, Any], *, now: datetime) -> TenantReplica:
        tenant_id = str(row["tenant_id"])
        dest = self._relocate_dest_queue(row)
        exported = None
        with self._pool.connection() as src:
            settings = src.execute(
                "SELECT * FROM tenant_settings WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchone()
        if settings is not None:
            exported = dict(settings)
            exported["paused"] = bool(row.get("source_paused"))
            exported["relocating"] = False
            exported["relocated"] = False
            exported["restoring"] = False
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
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET stage = 'purging', updated_at = %s, error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (now, row["id"]),
            ).fetchone()
            assert updated is not None
            return replica_from_row(updated)

    def _replica_purge(self, row: dict[str, Any], *, now: datetime) -> TenantReplica:
        tenant_id = str(row["tenant_id"])
        with transaction(self._pool) as conn:
            if self._count_leased(conn, tenant_id) > 0:
                raise ReplicaError("refusing to purge source while jobs are leased")
            self._purge_relocated_source(conn, tenant_id=tenant_id, now=now)
            updated = conn.execute(
                """
                UPDATE tenant_replicas
                SET status = 'complete',
                    stage = 'complete',
                    dest_dsn = %s,
                    dest_kek = NULL,
                    dest_keks = NULL,
                    updated_at = %s,
                    completed_at = %s,
                    error = NULL
                WHERE id = %s
                RETURNING *
                """,
                (_redact_dest_dsn(str(row["dest_dsn"])), now, now, row["id"]),
            ).fetchone()
            assert updated is not None
            return replica_from_row(updated)

    def _mark_replica_error(self, replica_id: UUID, error: str) -> None:
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE tenant_replicas
                SET error = %s, updated_at = %s
                WHERE id = %s
                """,
                (error, _utcnow(), replica_id),
            )
