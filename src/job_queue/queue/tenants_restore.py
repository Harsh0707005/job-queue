"""Resumable restore of a tenant snapshot into an empty target."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import (
    delivery_aad,
    job_aad,
    result_aad,
    schedule_aad,
    workflow_steps_aad,
)
from job_queue.db import transaction
from job_queue.models import TenantRestore
from job_queue.queue._common import RestoreError, _as_utc, _utcnow
from job_queue.queue.tenants_export import (
    SNAPSHOT_KINDS,
    snapshot_from_row,
    tenant_residue_tables,
)


def restore_from_row(row: dict[str, Any]) -> TenantRestore:
    applied = row.get("applied") or {}
    if isinstance(applied, str):
        applied = json.loads(applied)
    return TenantRestore(
        id=row["id"],
        snapshot_id=row["snapshot_id"],
        target_tenant_id=str(row["target_tenant_id"]),
        source_tenant_id=str(row["source_tenant_id"]),
        status=str(row["status"]),
        kind_index=int(row.get("kind_index") or 0),
        applied={str(k): int(v) for k, v in dict(applied).items()},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row.get("completed_at"),
        error=row.get("error"),
    )


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc(raw)
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


def _parse_uuid(raw: Any) -> UUID | None:
    if raw is None:
        return None
    return UUID(str(raw))


def _parse_bytes(raw: Any) -> bytes | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw)
    if isinstance(raw, dict) and "$b64" in raw:
        return base64.b64decode(str(raw["$b64"]))
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw)
        except Exception:  # noqa: BLE001
            return raw.encode()
    return None


class TenantsRestoreMixin:
    def restore_tenant(
        self,
        target_tenant_id: str,
        *,
        snapshot_id: UUID,
        batch_size: int = 100,
        run_until_complete: bool = False,
    ) -> TenantRestore:
        """Start or resume restore of *snapshot_id* into empty *target*.

        Target is paused + ``restoring`` until the last batch completes so
        workers never claim half-copied rows. Each advance is one transaction;
        a coordinator crash leaves a cursor that another node can resume.
        Source tenant is not paused.
        """
        if not target_tenant_id:
            raise ValueError("target_tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        rst = self._ensure_restore(target_tenant_id, snapshot_id=snapshot_id)
        if rst.status == "complete":
            return rst
        if run_until_complete:
            for _ in range(10000):
                rst = self.advance_tenant_restore(
                    target_tenant_id, batch_size=batch_size
                )
                if rst.status in ("complete", "failed", "aborted"):
                    return rst
            raise RestoreError("restore did not complete within batch budget")
        return self.advance_tenant_restore(
            target_tenant_id, batch_size=batch_size
        )

    def get_tenant_restore(self, target_tenant_id: str) -> TenantRestore | None:
        if not target_tenant_id:
            raise ValueError("target_tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM tenant_restores
                WHERE target_tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (target_tenant_id,),
            ).fetchone()
            return restore_from_row(row) if row else None

    def advance_tenant_restore(
        self, target_tenant_id: str, *, batch_size: int = 100
    ) -> TenantRestore:
        if not target_tenant_id:
            raise ValueError("target_tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (target_tenant_id,),
            )
            row = conn.execute(
                """
                SELECT * FROM tenant_restores
                WHERE target_tenant_id = %s AND status = 'restoring'
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (target_tenant_id,),
            ).fetchone()
            if row is None:
                latest = conn.execute(
                    """
                    SELECT * FROM tenant_restores
                    WHERE target_tenant_id = %s
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (target_tenant_id,),
                ).fetchone()
                if latest is None:
                    raise RestoreError(
                        f"no restore in progress for tenant {target_tenant_id!r}"
                    )
                return restore_from_row(latest)

            id_map: dict[str, dict[str, str]] = dict(row.get("id_map") or {})
            # psycopg may give Json
            if not isinstance(id_map, dict):
                id_map = {}
            id_map = {str(k): dict(v) for k, v in id_map.items()}
            applied = dict(row.get("applied") or {})
            kind_index = int(row["kind_index"] or 0)
            blob_seq = int(row["blob_seq"] or 0)
            blob_offset = int(row["blob_offset"] or 0)
            snapshot_id = row["snapshot_id"]
            target = str(row["target_tenant_id"])
            remaining = batch_size

            try:
                while remaining > 0 and kind_index < len(SNAPSHOT_KINDS):
                    kind = SNAPSHOT_KINDS[kind_index]
                    blob = conn.execute(
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
                    n = self._restore_kind_rows(
                        conn,
                        kind=kind,
                        rows=chunk,
                        target_tenant_id=target,
                        id_map=id_map,
                    )
                    applied[kind] = int(applied.get(kind) or 0) + n
                    blob_offset += len(chunk)
                    remaining -= max(1, len(chunk))
                    if blob_offset >= int(blob["row_count"] or len(rows)):
                        blob_seq += 1
                        blob_offset = 0
                        nxt = conn.execute(
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
                if done:
                    exported = row.get("exported_settings")
                    self._finalize_restore_locked(
                        conn,
                        target_tenant_id=target,
                        exported_settings=exported,
                        now=now,
                    )
                    updated = conn.execute(
                        """
                        UPDATE tenant_restores
                        SET status = 'complete',
                            kind_index = %s,
                            blob_seq = %s,
                            blob_offset = %s,
                            applied = %s,
                            id_map = %s,
                            updated_at = %s,
                            completed_at = %s,
                            error = NULL
                        WHERE id = %s
                        RETURNING *
                        """,
                        (
                            kind_index,
                            blob_seq,
                            blob_offset,
                            Jsonb(applied),
                            Jsonb(id_map),
                            now,
                            now,
                            row["id"],
                        ),
                    ).fetchone()
                else:
                    updated = conn.execute(
                        """
                        UPDATE tenant_restores
                        SET kind_index = %s,
                            blob_seq = %s,
                            blob_offset = %s,
                            applied = %s,
                            id_map = %s,
                            updated_at = %s
                        WHERE id = %s
                        RETURNING *
                        """,
                        (
                            kind_index,
                            blob_seq,
                            blob_offset,
                            Jsonb(applied),
                            Jsonb(id_map),
                            now,
                            row["id"],
                        ),
                    ).fetchone()
                assert updated is not None
                return restore_from_row(updated)
            except Exception as exc:  # noqa: BLE001
                conn.execute(
                    """
                    UPDATE tenant_restores
                    SET error = %s, updated_at = %s
                    WHERE id = %s
                    """,
                    (str(exc)[:2000], now, row["id"]),
                )
                raise RestoreError(
                    f"restore failed for tenant {target_tenant_id!r}: {exc}"
                ) from exc

    def process_tenant_restores(
        self, *, limit: int = 20, batch_size: int = 100
    ) -> list[TenantRestore]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT target_tenant_id FROM tenant_restores
                WHERE status = 'restoring'
                ORDER BY updated_at ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        out: list[TenantRestore] = []
        for r in rows:
            try:
                out.append(
                    self.advance_tenant_restore(
                        str(r["target_tenant_id"]), batch_size=batch_size
                    )
                )
            except RestoreError:
                continue
        return out

    def _ensure_restore(
        self, target_tenant_id: str, *, snapshot_id: UUID
    ) -> TenantRestore:
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (target_tenant_id,),
            )
            snap = conn.execute(
                """
                SELECT id, source_tenant_id, status
                FROM tenant_snapshots WHERE id = %s
                """,
                (snapshot_id,),
            ).fetchone()
            if snap is None:
                raise RestoreError(f"snapshot {snapshot_id} not found")
            if snap["status"] != "ready":
                raise RestoreError(
                    f"snapshot {snapshot_id} is {snap['status']!r}, expected ready"
                )
            existing = conn.execute(
                """
                SELECT * FROM tenant_restores
                WHERE target_tenant_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (target_tenant_id,),
            ).fetchone()
            if existing is not None and existing["status"] == "restoring":
                if existing["snapshot_id"] != snapshot_id:
                    raise RestoreError(
                        f"tenant {target_tenant_id!r} already restoring "
                        f"snapshot {existing['snapshot_id']}"
                    )
                return restore_from_row(existing)
            if existing is not None and existing["status"] == "complete":
                if existing["snapshot_id"] == snapshot_id:
                    return restore_from_row(existing)
            leftover = tenant_residue_tables(conn, target_tenant_id)
            if leftover:
                raise RestoreError(
                    f"tenant {target_tenant_id!r} is not empty; leftover: "
                    + ", ".join(leftover)
                )
            settings_blob = conn.execute(
                """
                SELECT rows FROM tenant_snapshot_blobs
                WHERE snapshot_id = %s AND kind = 'tenant_settings'
                ORDER BY seq ASC LIMIT 1
                """,
                (snapshot_id,),
            ).fetchone()
            exported_settings = None
            if settings_blob is not None:
                raw = settings_blob["rows"] or []
                if isinstance(raw, str):
                    raw = json.loads(raw)
                if raw:
                    exported_settings = raw[0]
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, restoring, updated_at
                ) VALUES (%s, NULL, NULL, TRUE, FALSE, TRUE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    restoring = TRUE,
                    offboarding = FALSE,
                    updated_at = EXCLUDED.updated_at
                """,
                (target_tenant_id, now),
            )
            rid = uuid4()
            row = conn.execute(
                """
                INSERT INTO tenant_restores (
                    id, snapshot_id, target_tenant_id, source_tenant_id,
                    status, kind_index, blob_seq, blob_offset, applied,
                    id_map, exported_settings, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    'restoring', 0, 0, 0, '{}'::jsonb,
                    '{}'::jsonb, %s, %s, %s
                )
                RETURNING *
                """,
                (
                    rid,
                    snapshot_id,
                    target_tenant_id,
                    snap["source_tenant_id"],
                    Jsonb(exported_settings) if exported_settings else None,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return restore_from_row(row)

    def _target_accepts_restore(self, conn: Any, tenant_id: str) -> bool:
        return not tenant_residue_tables(conn, tenant_id)

    def _map_id(
        self, id_map: dict[str, dict[str, str]], kind: str, old: Any
    ) -> UUID | None:
        if old is None:
            return None
        bucket = id_map.setdefault(kind, {})
        key = str(old)
        if key in bucket:
            return UUID(bucket[key])
        new = uuid4()
        bucket[key] = str(new)
        return new

    def _mapped(
        self, id_map: dict[str, dict[str, str]], kind: str, old: Any
    ) -> UUID | None:
        if old is None:
            return None
        bucket = id_map.get(kind) or {}
        val = bucket.get(str(old))
        return UUID(val) if val else None

    def _restore_kind_rows(
        self,
        conn: Any,
        *,
        kind: str,
        rows: list[Any],
        target_tenant_id: str,
        id_map: dict[str, dict[str, str]],
    ) -> int:
        n = 0
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            fn = getattr(self, f"_restore_{kind}", None)
            if fn is None:
                continue
            fn(conn, raw, target_tenant_id, id_map)
            n += 1
        return n

    def _restore_payload_keys(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO tenant_payload_keys (
                tenant_id, key_id, wrapped_dek, wrap_nonce, kek_id,
                status, created_at, retired_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, key_id) DO NOTHING
            """,
            (
                target,
                _parse_uuid(raw["key_id"]),
                _parse_bytes(raw.get("wrapped_dek")),
                _parse_bytes(raw.get("wrap_nonce")),
                raw.get("kek_id"),
                raw.get("status") or "decrypt_only",
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("retired_at")),
            ),
        )

    def _restore_payload_key_rotations(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "payload_key_rotations", raw["id"])
        conn.execute(
            """
            INSERT INTO payload_key_rotations (
                id, tenant_id, from_key_id, to_key_id, status,
                jobs_rewrapped, schedules_rewrapped, created_at, updated_at,
                completed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                _parse_uuid(raw.get("from_key_id")),
                _parse_uuid(raw.get("to_key_id")),
                raw.get("status") or "complete",
                int(raw.get("jobs_rewrapped") or 0),
                int(raw.get("schedules_rewrapped") or 0),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
                _parse_dt(raw.get("completed_at")),
            ),
        )

    def _restore_tenant_settings(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        # Keep paused+restoring until finalize. Limits/rates applied then.
        return

    def _restore_tenant_fairness(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO tenant_fairness (tenant_id, last_lease_at)
            VALUES (%s, %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET last_lease_at = EXCLUDED.last_lease_at
            """,
            (target, _parse_dt(raw.get("last_lease_at")) or _utcnow()),
        )

    def _restore_duration_estimates(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO duration_estimates (
                tenant_id, queue_name, kind, ewma_seconds, samples, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, queue_name, kind) DO NOTHING
            """,
            (
                target,
                raw.get("queue_name") or "default",
                raw.get("kind") or "",
                float(raw.get("ewma_seconds") or 0),
                int(raw.get("samples") or 0),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
            ),
        )

    def _restore_schedules(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        old_id = _parse_uuid(raw["id"])
        new_id = self._map_id(id_map, "schedules", old_id)
        payload = raw.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(payload, dict):
            plain = self._open_dict(
                payload,
                tenant_id=target,
                aad=schedule_aad(old_id),
                conn=conn,
            )
            sealed, kid = self._seal_dict(
                conn,
                tenant_id=target,
                plaintext=plain,
                aad=schedule_aad(new_id),
            )
        else:
            sealed, kid = {}, None
        conn.execute(
            """
            INSERT INTO schedules (
                id, tenant_id, name, queue_name, payload, interval_seconds,
                cron_expr, timezone, priority, max_attempts,
                visibility_timeout_seconds, enabled, next_run_at,
                last_enqueued_at, created_at, updated_at, labels, payload_key_id,
                required_skills
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                raw.get("name"),
                raw.get("queue_name") or "default",
                Jsonb(sealed),
                raw.get("interval_seconds"),
                raw.get("cron_expr"),
                raw.get("timezone") or "UTC",
                int(raw.get("priority") or 0),
                int(raw.get("max_attempts") or 3),
                int(raw.get("visibility_timeout_seconds") or 30),
                bool(raw.get("enabled", True)),
                _parse_dt(raw.get("next_run_at")) or _utcnow(),
                _parse_dt(raw.get("last_enqueued_at")),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
                list(raw.get("labels") or []),
                kid,
                list(raw.get("required_skills") or []),
            ),
        )

    def _restore_workflow_runs(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        old_id = _parse_uuid(raw["id"])
        new_id = self._map_id(id_map, "workflow_runs", old_id)
        steps = self._parse_steps_def(
            {"id": old_id, "tenant_id": target, "steps_def": raw.get("steps_def")},
            conn,
        )
        sealed, kid = self._seal_dict(
            conn,
            tenant_id=target,
            plaintext={"steps": steps},
            aad=workflow_steps_aad(new_id),
        )
        conn.execute(
            """
            INSERT INTO workflow_runs (
                id, tenant_id, name, status, steps_def, current_index,
                fail_step_index, fail_reason, max_compensate_retries,
                created_at, updated_at, completed_at, payload_key_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                raw.get("name"),
                raw.get("status") or "running",
                Jsonb(sealed),
                int(raw.get("current_index") or 0),
                raw.get("fail_step_index"),
                raw.get("fail_reason"),
                int(raw.get("max_compensate_retries") or 5),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
                _parse_dt(raw.get("completed_at")),
                kid,
            ),
        )

    def _restore_jobs(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        old_id = _parse_uuid(raw["id"])
        new_id = self._map_id(id_map, "jobs", old_id)
        payload = raw.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        plain = self._open_dict(
            payload if isinstance(payload, dict) else {},
            tenant_id=target,
            aad=job_aad(old_id),
            conn=conn,
        )
        sealed, kid = self._seal_dict(
            conn, tenant_id=target, plaintext=plain, aad=job_aad(new_id)
        )
        result = raw.get("result")
        sealed_result = None
        if result is not None:
            opened = self._open_json_value(
                result, tenant_id=target, aad=result_aad(old_id), conn=conn
            )
            sealed_result, _ = self._seal_json_value(
                conn, tenant_id=target, value=opened, aad=result_aad(new_id)
            )
        status = str(raw.get("status") or "pending")
        attempts = int(raw.get("attempts") or 0)
        leased_by = raw.get("leased_by")
        lease_token = _parse_uuid(raw.get("lease_token"))
        lease_tenant_id = raw.get("lease_tenant_id")
        lease_expires_at = _parse_dt(raw.get("lease_expires_at"))
        lease_started_at = _parse_dt(raw.get("lease_started_at"))
        if status == "leased":
            status = "pending"
            attempts = max(0, attempts - 1)
            leased_by = None
            lease_token = None
            lease_tenant_id = None
            lease_expires_at = None
            lease_started_at = None
        deps = tuple(
            self._map_id(id_map, "jobs", x)
            for x in (raw.get("depends_on") or [])
        )
        waiting = tuple(
            self._map_id(id_map, "jobs", x)
            for x in (raw.get("waiting_on") or [])
        )
        parent = (
            self._map_id(id_map, "jobs", raw.get("parent_job_id"))
            if raw.get("parent_job_id")
            else None
        )
        sched = (
            self._map_id(id_map, "schedules", raw.get("schedule_id"))
            if raw.get("schedule_id")
            else None
        )
        part_id = (
            self._compute_partition_id(conn, tenant_id=target, job_id=new_id)
            if hasattr(self, "_compute_partition_id")
            else int(raw.get("partition_id") or 0)
        )
        conn.execute(
            """
            INSERT INTO jobs (
                id, tenant_id, queue_name, payload, status, priority, attempts,
                max_attempts, visibility_timeout_seconds, max_runtime_seconds,
                retry_backoff_base_seconds, retry_backoff_max_seconds,
                available_at, dedupe_key, schedule_id, depends_on,
                parent_job_id, waiting_on, labels, required_skills,
                leased_by, lease_token, lease_tenant_id, lease_expires_at,
                lease_started_at, created_at, updated_at, completed_at, result,
                checkpoint, checkpointed_at, deadline_at,
                estimated_duration_seconds, preemptible, preempt_count,
                last_preempted_at, approval_requested_at, approval_deadline_at,
                approval_message, approval_outcome, approval_resolved_at,
                await_children_started_at, await_children_deadline_at,
                await_children_outcome, await_children_resolved_at,
                last_error, partition_id, payload_key_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                raw.get("queue_name") or "default",
                Jsonb(sealed),
                status,
                int(raw.get("priority") or 0),
                attempts,
                int(raw.get("max_attempts") or 3),
                int(raw.get("visibility_timeout_seconds") or 30),
                int(raw.get("max_runtime_seconds") or 3600),
                int(raw.get("retry_backoff_base_seconds") or 1),
                int(raw.get("retry_backoff_max_seconds") or 300),
                _parse_dt(raw.get("available_at")) or _utcnow(),
                raw.get("dedupe_key"),
                sched,
                list(deps),
                parent,
                list(waiting),
                list(raw.get("labels") or []),
                list(raw.get("required_skills") or []),
                leased_by,
                lease_token,
                lease_tenant_id,
                lease_expires_at,
                lease_started_at,
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
                _parse_dt(raw.get("completed_at")),
                Jsonb(sealed_result) if sealed_result is not None else None,
                Jsonb(raw["checkpoint"])
                if isinstance(raw.get("checkpoint"), (dict, list))
                else raw.get("checkpoint"),
                _parse_dt(raw.get("checkpointed_at")),
                _parse_dt(raw.get("deadline_at")),
                raw.get("estimated_duration_seconds"),
                bool(raw.get("preemptible", True)),
                int(raw.get("preempt_count") or 0),
                _parse_dt(raw.get("last_preempted_at")),
                _parse_dt(raw.get("approval_requested_at")),
                _parse_dt(raw.get("approval_deadline_at")),
                raw.get("approval_message"),
                raw.get("approval_outcome"),
                _parse_dt(raw.get("approval_resolved_at")),
                _parse_dt(raw.get("await_children_started_at")),
                _parse_dt(raw.get("await_children_deadline_at")),
                raw.get("await_children_outcome"),
                _parse_dt(raw.get("await_children_resolved_at")),
                raw.get("last_error"),
                part_id,
                kid,
            ),
        )

    def _restore_job_dag_batches(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "job_dag_batches", raw["id"])
        job_ids = [
            self._map_id(id_map, "jobs", x) for x in (raw.get("job_ids") or [])
        ]
        refs = [str(r) for r in (raw.get("refs") or [])]
        conn.execute(
            """
            INSERT INTO job_dag_batches (
                id, tenant_id, queue_name, batch_key, job_ids, refs, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                raw.get("queue_name") or "default",
                raw.get("batch_key") or str(new_id),
                job_ids,
                refs,
                _parse_dt(raw.get("created_at")) or _utcnow(),
            ),
        )

    def _restore_workflow_step_runs(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "workflow_step_runs", raw["id"])
        run_id = self._map_id(id_map, "workflow_runs", raw.get("run_id"))
        job_id = (
            self._map_id(id_map, "jobs", raw.get("job_id"))
            if raw.get("job_id")
            else None
        )
        conn.execute(
            """
            INSERT INTO workflow_step_runs (
                id, run_id, tenant_id, step_index, name, direction, status,
                job_id, result, error, idempotency_key, retry_count,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                run_id,
                target,
                int(raw.get("step_index") or 0),
                raw.get("name") or "step",
                raw.get("direction") or "forward",
                raw.get("status") or "pending",
                job_id,
                Jsonb(raw["result"]) if isinstance(raw.get("result"), (dict, list)) else raw.get("result"),
                raw.get("error"),
                raw.get("idempotency_key") or f"{new_id}:step",
                int(raw.get("retry_count") or 0),
                _parse_dt(raw.get("started_at")),
                _parse_dt(raw.get("completed_at")),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
            ),
        )

    def _restore_job_side_effects(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        job_id = self._map_id(id_map, "jobs", raw.get("job_id"))
        conn.execute(
            """
            INSERT INTO job_side_effects (
                job_id, effect_key, tenant_id, status, idempotency_key, result,
                started_by, started_at, completed_by, lease_token,
                created_at, completed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (job_id, effect_key) DO NOTHING
            """,
            (
                job_id,
                raw.get("effect_key") or "default",
                target,
                raw.get("status") or "completed",
                raw.get("idempotency_key") or f"{job_id}:eff",
                Jsonb(raw["result"])
                if isinstance(raw.get("result"), (dict, list))
                else raw.get("result"),
                raw.get("started_by"),
                _parse_dt(raw.get("started_at")),
                raw.get("completed_by"),
                _parse_uuid(raw.get("lease_token")),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("completed_at")),
            ),
        )

    def _restore_job_audit_events(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "job_audit_events", raw.get("event_id"))
        job_id = self._map_id(id_map, "jobs", raw.get("job_id"))
        detail = raw.get("detail") or {}
        if isinstance(detail, str):
            detail = json.loads(detail)
        conn.execute(
            """
            INSERT INTO job_audit_events (
                event_id, tenant_id, job_id, seq, event_type, from_status,
                to_status, actor, detail, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                new_id,
                target,
                job_id,
                int(raw.get("seq") or 1),
                raw.get("event_type") or "unknown",
                raw.get("from_status"),
                raw.get("to_status"),
                raw.get("actor"),
                Jsonb(detail) if isinstance(detail, dict) else Jsonb({}),
                _parse_dt(raw.get("created_at")) or _utcnow(),
            ),
        )

    def _restore_webhook_endpoints(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "webhook_endpoints", raw["id"])
        conn.execute(
            """
            INSERT INTO webhook_endpoints (
                id, tenant_id, url, secret, events, enabled, max_attempts,
                backoff_base_seconds, backoff_max_seconds, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                target,
                raw.get("url"),
                raw.get("secret"),
                list(raw.get("events") or []),
                bool(raw.get("enabled", True)),
                int(raw.get("max_attempts") or 5),
                int(raw.get("backoff_base_seconds") or 1),
                int(raw.get("backoff_max_seconds") or 300),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
            ),
        )

    def _restore_webhook_deliveries(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        old_id = _parse_uuid(raw["id"])
        new_id = self._map_id(id_map, "webhook_deliveries", old_id)
        endpoint_id = self._map_id(
            id_map, "webhook_endpoints", raw.get("endpoint_id")
        )
        job_id = (
            self._map_id(id_map, "jobs", raw.get("job_id"))
            if raw.get("job_id")
            else None
        )
        payload = raw.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        plain = self._open_dict(
            payload if isinstance(payload, dict) else {},
            tenant_id=target,
            aad=delivery_aad(old_id),
            conn=conn,
        )
        if isinstance(plain.get("job_id"), str) and job_id is not None:
            plain["job_id"] = str(job_id)
        if isinstance(plain.get("delivery_id"), str):
            plain["delivery_id"] = str(new_id)
        if isinstance(plain.get("tenant_id"), str):
            plain["tenant_id"] = target
        sealed, kid = self._seal_dict(
            conn, tenant_id=target, plaintext=plain, aad=delivery_aad(new_id)
        )
        status = str(raw.get("status") or "pending")
        delivering_until = _parse_dt(raw.get("delivering_until"))
        if status == "delivering":
            status = "pending"
            delivering_until = None
        conn.execute(
            """
            INSERT INTO webhook_deliveries (
                id, endpoint_id, tenant_id, job_id, event, payload, status,
                attempts, max_attempts, backoff_base_seconds, backoff_max_seconds,
                next_attempt_at, delivering_until, last_error, last_status_code,
                created_at, updated_at, completed_at, signing_secret,
                payload_key_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                endpoint_id,
                target,
                job_id,
                raw.get("event") or "job.completed",
                Jsonb(sealed),
                status,
                int(raw.get("attempts") or 0),
                int(raw.get("max_attempts") or 5),
                int(raw.get("backoff_base_seconds") or 1),
                int(raw.get("backoff_max_seconds") or 300),
                _parse_dt(raw.get("next_attempt_at")) or _utcnow(),
                delivering_until,
                raw.get("last_error"),
                raw.get("last_status_code"),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
                _parse_dt(raw.get("completed_at")),
                raw.get("signing_secret"),
                kid,
            ),
        )

    def _restore_alert_rules(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        if str(raw.get("scope") or "") == "overall":
            return
        new_id = self._map_id(id_map, "alert_rules", raw["id"])
        name = str(raw.get("name") or "rule")
        existing = conn.execute(
            "SELECT 1 FROM alert_rules WHERE name = %s", (name,)
        ).fetchone()
        if existing is not None:
            name = f"{name}__rst_{str(new_id)[:8]}"
        conn.execute(
            """
            INSERT INTO alert_rules (
                id, name, enabled, scope, tenant_id, worker_id, metric,
                comparator, threshold, window_seconds, cooldown_seconds,
                last_fired_at, muted_until, auto_pause_tenant,
                recovery_threshold, recovery_evaluations,
                healthy_interval_seconds, healthy_streak, last_healthy_at,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                name,
                bool(raw.get("enabled", True)),
                raw.get("scope") or "tenant",
                target if raw.get("scope") == "tenant" else raw.get("tenant_id"),
                raw.get("worker_id"),
                raw.get("metric") or "dead_count",
                raw.get("comparator") or "gt",
                float(raw.get("threshold") or 0),
                int(raw.get("window_seconds") or 300),
                int(raw.get("cooldown_seconds") or 300),
                _parse_dt(raw.get("last_fired_at")),
                _parse_dt(raw.get("muted_until")),
                bool(raw.get("auto_pause_tenant") or False),
                raw.get("recovery_threshold"),
                int(raw.get("recovery_evaluations") or 2),
                int(raw.get("healthy_interval_seconds") or 1),
                int(raw.get("healthy_streak") or 0),
                _parse_dt(raw.get("last_healthy_at")),
                _parse_dt(raw.get("created_at")) or _utcnow(),
                _parse_dt(raw.get("updated_at")) or _utcnow(),
            ),
        )

    def _restore_alert_events(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        new_id = self._map_id(id_map, "alert_events", raw["id"])
        rule_id = self._mapped(id_map, "alert_rules", raw.get("rule_id"))
        if rule_id is None:
            return
        conn.execute(
            """
            INSERT INTO alert_events (
                id, rule_id, rule_name, metric, scope, tenant_id, worker_id,
                value, threshold, comparator, window_seconds, detail, fired_at,
                resolved_at, resolved_by, resolution_note
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (id) DO NOTHING
            """,
            (
                new_id,
                rule_id,
                raw.get("rule_name") or "",
                raw.get("metric") or "",
                raw.get("scope") or "tenant",
                target,
                raw.get("worker_id"),
                float(raw.get("value") or 0),
                float(raw.get("threshold") or 0),
                raw.get("comparator") or "gt",
                raw.get("window_seconds"),
                Jsonb(raw.get("detail") or {}),
                _parse_dt(raw.get("fired_at")) or _utcnow(),
                _parse_dt(raw.get("resolved_at")),
                raw.get("resolved_by"),
                raw.get("resolution_note"),
            ),
        )

    def _restore_queue_metric_buckets(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO queue_metric_buckets (
                bucket_start, tenant_id, metric, count, sum_ms
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bucket_start, tenant_id, metric) DO NOTHING
            """,
            (
                _parse_dt(raw.get("bucket_start")) or _utcnow(),
                target,
                raw.get("metric") or "ack",
                int(raw.get("count") or 0),
                float(raw.get("sum_ms") or 0),
            ),
        )

    def _restore_tenant_usage_buckets(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO tenant_usage_buckets (
                bucket_start, tenant_id,
                enqueues, leases, acks, nacks, fails, dead_letters,
                cancels, releases, preemptions, schedule_fires,
                webhook_successes, webhook_failures, spawn_children,
                runtime_ms, wait_ms
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (bucket_start, tenant_id) DO NOTHING
            """,
            (
                _parse_dt(raw.get("bucket_start")) or _utcnow(),
                target,
                int(raw.get("enqueues") or 0),
                int(raw.get("leases") or 0),
                int(raw.get("acks") or 0),
                int(raw.get("nacks") or 0),
                int(raw.get("fails") or 0),
                int(raw.get("dead_letters") or 0),
                int(raw.get("cancels") or 0),
                int(raw.get("releases") or 0),
                int(raw.get("preemptions") or 0),
                int(raw.get("schedule_fires") or 0),
                int(raw.get("webhook_successes") or 0),
                int(raw.get("webhook_failures") or 0),
                int(raw.get("spawn_children") or 0),
                int(raw.get("runtime_ms") or 0),
                int(raw.get("wait_ms") or 0),
            ),
        )

    def _restore_queue_metric_snapshots(
        self, conn: Any, raw: dict, target: str, id_map: dict
    ) -> None:
        conn.execute(
            """
            INSERT INTO queue_metric_snapshots (
                sampled_at, tenant_id, pending, leased, claimable, failed,
                dead, completed, cancelled, awaiting_approval,
                awaiting_children, queue_lag_seconds, avg_pending_age_seconds,
                max_pending_age_seconds, webhook_pending, webhook_delivering,
                webhook_succeeded, webhook_dead, schedules_enabled, schedules_due
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (sampled_at, tenant_id) DO NOTHING
            """,
            (
                _parse_dt(raw.get("sampled_at")) or _utcnow(),
                target,
                int(raw.get("pending") or 0),
                int(raw.get("leased") or 0),
                int(raw.get("claimable") or 0),
                int(raw.get("failed") or 0),
                int(raw.get("dead") or 0),
                int(raw.get("completed") or 0),
                int(raw.get("cancelled") or 0),
                int(raw.get("awaiting_approval") or 0),
                int(raw.get("awaiting_children") or 0),
                raw.get("queue_lag_seconds"),
                raw.get("avg_pending_age_seconds"),
                raw.get("max_pending_age_seconds"),
                int(raw.get("webhook_pending") or 0),
                int(raw.get("webhook_delivering") or 0),
                int(raw.get("webhook_succeeded") or 0),
                int(raw.get("webhook_dead") or 0),
                int(raw.get("schedules_enabled") or 0),
                int(raw.get("schedules_due") or 0),
            ),
        )

    def _finalize_restore_locked(
        self,
        conn: Any,
        *,
        target_tenant_id: str,
        exported_settings: Any,
        now: datetime,
    ) -> None:
        paused = False
        max_c = None
        max_p = None
        enq_rate = None
        enq_burst = None
        lease_rate = None
        lease_burst = None
        c_ret = None
        d_ret = None
        def_ttl = None
        def_exp = None
        if isinstance(exported_settings, dict):
            paused = bool(exported_settings.get("paused") or False)
            max_c = exported_settings.get("max_concurrent_leases")
            max_p = exported_settings.get("max_pending_jobs")
            enq_rate = exported_settings.get("enqueue_rate_per_sec")
            enq_burst = exported_settings.get("enqueue_burst")
            lease_rate = exported_settings.get("lease_rate_per_sec")
            lease_burst = exported_settings.get("lease_burst")
            c_ret = exported_settings.get("completed_retention_seconds")
            d_ret = exported_settings.get("dead_retention_seconds")
            def_ttl = exported_settings.get("default_job_ttl_seconds")
            def_exp = exported_settings.get("default_expiry_action")
        conn.execute(
            """
            INSERT INTO tenant_settings (
                tenant_id, max_concurrent_leases, max_pending_jobs,
                paused, offboarding, restoring,
                enqueue_rate_per_sec, enqueue_burst,
                lease_rate_per_sec, lease_burst,
                completed_retention_seconds, dead_retention_seconds,
                default_job_ttl_seconds, default_expiry_action,
                updated_at
            ) VALUES (
                %s, %s, %s,
                %s, FALSE, FALSE,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s
            )
            ON CONFLICT (tenant_id) DO UPDATE
            SET max_concurrent_leases = EXCLUDED.max_concurrent_leases,
                max_pending_jobs = EXCLUDED.max_pending_jobs,
                paused = EXCLUDED.paused,
                offboarding = FALSE,
                restoring = FALSE,
                enqueue_rate_per_sec = EXCLUDED.enqueue_rate_per_sec,
                enqueue_burst = EXCLUDED.enqueue_burst,
                lease_rate_per_sec = EXCLUDED.lease_rate_per_sec,
                lease_burst = EXCLUDED.lease_burst,
                completed_retention_seconds = EXCLUDED.completed_retention_seconds,
                dead_retention_seconds = EXCLUDED.dead_retention_seconds,
                default_job_ttl_seconds = EXCLUDED.default_job_ttl_seconds,
                default_expiry_action = EXCLUDED.default_expiry_action,
                updated_at = EXCLUDED.updated_at
            """,
            (
                target_tenant_id,
                max_c,
                max_p,
                paused,
                enq_rate,
                enq_burst,
                lease_rate,
                lease_burst,
                c_ret,
                d_ret,
                def_ttl,
                def_exp,
                now,
            ),
        )
