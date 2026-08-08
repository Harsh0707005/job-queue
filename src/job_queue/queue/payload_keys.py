"""Live per-tenant payload DEK rotation (crash-safe, non-stopping)."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import (
    delivery_aad,
    encrypt_json,
    job_aad,
    result_aad,
    schedule_aad,
    workflow_steps_aad,
)
from job_queue.db import transaction
from job_queue.models import PayloadKeyRotation, TenantPayloadKey
from job_queue.queue._common import _utcnow


def _row_to_key(row: dict[str, Any]) -> TenantPayloadKey:
    return TenantPayloadKey(
        tenant_id=str(row["tenant_id"]),
        key_id=row["key_id"],
        kek_id=str(row["kek_id"]),
        status=str(row["status"]),
        created_at=row["created_at"],
        retired_at=row.get("retired_at"),
    )


def _row_to_rotation(row: dict[str, Any]) -> PayloadKeyRotation:
    return PayloadKeyRotation(
        id=row["id"],
        tenant_id=str(row["tenant_id"]),
        from_key_id=row.get("from_key_id"),
        to_key_id=row["to_key_id"],
        status=str(row["status"]),
        jobs_rewrapped=int(row.get("jobs_rewrapped") or 0),
        schedules_rewrapped=int(row.get("schedules_rewrapped") or 0),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row.get("completed_at"),
    )


_ROTATION_COLUMNS = """
    id, tenant_id, from_key_id, to_key_id, status,
    jobs_rewrapped, schedules_rewrapped, created_at, updated_at, completed_at
"""

_KEY_COLUMNS = """
    tenant_id, key_id, kek_id, status, created_at, retired_at
"""


class PayloadKeysMixin:
    def list_tenant_payload_keys(self, tenant_id: str) -> list[TenantPayloadKey]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_KEY_COLUMNS}
                FROM tenant_payload_keys
                WHERE tenant_id = %s
                ORDER BY created_at ASC, key_id ASC
                """,
                (tenant_id,),
            ).fetchall()
            return [_row_to_key(r) for r in rows]

    def get_payload_key_rotation(
        self, tenant_id: str, *, rotation_id: UUID | None = None
    ) -> PayloadKeyRotation | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            if rotation_id is not None:
                row = conn.execute(
                    f"""
                    SELECT {_ROTATION_COLUMNS}
                    FROM payload_key_rotations
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (rotation_id, tenant_id),
                ).fetchone()
            else:
                row = conn.execute(
                    f"""
                    SELECT {_ROTATION_COLUMNS}
                    FROM payload_key_rotations
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (tenant_id,),
                ).fetchone()
            return _row_to_rotation(row) if row else None

    def rotate_tenant_payload_key(self, tenant_id: str) -> PayloadKeyRotation:
        """Mint a new active DEK without stopping workers.

        Old active key becomes ``decrypt_only`` in the same transaction so
        pending jobs keep decrypting. No plaintext is written. Incomplete
        rotations stay resumable after a crash.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        crypto = self._require_crypto()
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            old = conn.execute(
                """
                SELECT key_id, wrapped_dek, wrap_nonce, kek_id, status
                FROM tenant_payload_keys
                WHERE tenant_id = %s AND status = 'active'
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            from_key_id = old["key_id"] if old else None
            if old is not None:
                conn.execute(
                    """
                    UPDATE tenant_payload_keys
                    SET status = 'decrypt_only'
                    WHERE tenant_id = %s AND key_id = %s AND status = 'active'
                    """,
                    (tenant_id, from_key_id),
                )
            new_id = uuid4()
            _dek, wrapped, nonce = crypto.wrap_new_dek(
                tenant_id=tenant_id, key_id=new_id
            )
            conn.execute(
                """
                INSERT INTO tenant_payload_keys (
                    tenant_id, key_id, wrapped_dek, wrap_nonce, kek_id,
                    status, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    'active', %s
                )
                """,
                (tenant_id, new_id, wrapped, nonce, crypto.active_kek_id, now),
            )
            rot_id = uuid4()
            row = conn.execute(
                f"""
                INSERT INTO payload_key_rotations (
                    id, tenant_id, from_key_id, to_key_id, status,
                    jobs_rewrapped, schedules_rewrapped,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, 'reencrypting',
                    0, 0,
                    %s, %s
                )
                RETURNING {_ROTATION_COLUMNS}
                """,
                (rot_id, tenant_id, from_key_id, new_id, now, now),
            ).fetchone()
            assert row is not None
            return _row_to_rotation(row)

    def advance_payload_key_rotation(
        self,
        tenant_id: str,
        *,
        batch_size: int = 200,
        rotation_id: UUID | None = None,
    ) -> PayloadKeyRotation:
        """Re-seal a batch onto the active key; retire unused old keys.

        Each job/schedule UPDATE is ciphertext → ciphertext. A crash leaves
        some rows on the old key and some on the new key; both still decrypt.
        Wrapped DEK bytes are overwritten only when nothing references them.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if batch_size > 2000:
            raise ValueError("batch_size must be <= 2000")

        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if rotation_id is not None:
                rot = conn.execute(
                    f"""
                    SELECT {_ROTATION_COLUMNS}
                    FROM payload_key_rotations
                    WHERE id = %s AND tenant_id = %s
                    FOR UPDATE
                    """,
                    (rotation_id, tenant_id),
                ).fetchone()
                if rot is None:
                    raise ValueError("rotation not found")
            else:
                rot = conn.execute(
                    f"""
                    SELECT {_ROTATION_COLUMNS}
                    FROM payload_key_rotations
                    WHERE tenant_id = %s AND status = 'reencrypting'
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (tenant_id,),
                ).fetchone()
                if rot is None:
                    # No open rotation: still encrypt leftover plaintext /
                    # retire unused decrypt_only keys against the active DEK.
                    active = conn.execute(
                        """
                        SELECT key_id FROM tenant_payload_keys
                        WHERE tenant_id = %s AND status = 'active'
                        """,
                        (tenant_id,),
                    ).fetchone()
                    if active is None:
                        raise ValueError(
                            f"tenant {tenant_id!r} has no active payload key"
                        )
                    jobs_n = self._reencrypt_jobs_batch(
                        conn,
                        tenant_id=tenant_id,
                        to_key_id=active["key_id"],
                        limit=batch_size,
                    )
                    sched_n = self._reencrypt_schedules_batch(
                        conn,
                        tenant_id=tenant_id,
                        to_key_id=active["key_id"],
                        limit=batch_size,
                    )
                    self._reencrypt_deliveries_batch(
                        conn,
                        tenant_id=tenant_id,
                        to_key_id=active["key_id"],
                        limit=batch_size,
                    )
                    self._reencrypt_workflow_runs_batch(
                        conn,
                        tenant_id=tenant_id,
                        to_key_id=active["key_id"],
                        limit=batch_size,
                    )
                    self._retire_unreferenced_keys(conn, tenant_id=tenant_id)
                    synthetic = {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "from_key_id": None,
                        "to_key_id": active["key_id"],
                        "status": "complete",
                        "jobs_rewrapped": jobs_n,
                        "schedules_rewrapped": sched_n,
                        "created_at": now,
                        "updated_at": now,
                        "completed_at": now,
                    }
                    return _row_to_rotation(synthetic)

            if rot["status"] == "complete":
                return _row_to_rotation(rot)
            if rot["status"] == "aborted":
                return _row_to_rotation(rot)

            to_key_id = rot["to_key_id"]
            jobs_n = self._reencrypt_jobs_batch(
                conn,
                tenant_id=tenant_id,
                to_key_id=to_key_id,
                limit=batch_size,
            )
            sched_n = self._reencrypt_schedules_batch(
                conn,
                tenant_id=tenant_id,
                to_key_id=to_key_id,
                limit=batch_size,
            )
            self._reencrypt_deliveries_batch(
                conn,
                tenant_id=tenant_id,
                to_key_id=to_key_id,
                limit=batch_size,
            )
            self._reencrypt_workflow_runs_batch(
                conn,
                tenant_id=tenant_id,
                to_key_id=to_key_id,
                limit=batch_size,
            )
            remaining = self._count_unsealed(conn, tenant_id, to_key_id)
            new_jobs = int(rot["jobs_rewrapped"] or 0) + jobs_n
            new_sched = int(rot["schedules_rewrapped"] or 0) + sched_n
            if remaining == 0:
                self._retire_unreferenced_keys(conn, tenant_id=tenant_id)
                updated = conn.execute(
                    f"""
                    UPDATE payload_key_rotations
                    SET jobs_rewrapped = %s,
                        schedules_rewrapped = %s,
                        status = 'complete',
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                    RETURNING {_ROTATION_COLUMNS}
                    """,
                    (new_jobs, new_sched, now, now, rot["id"]),
                ).fetchone()
            else:
                updated = conn.execute(
                    f"""
                    UPDATE payload_key_rotations
                    SET jobs_rewrapped = %s,
                        schedules_rewrapped = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING {_ROTATION_COLUMNS}
                    """,
                    (new_jobs, new_sched, now, rot["id"]),
                ).fetchone()
            assert updated is not None
            return _row_to_rotation(updated)

    def process_payload_key_rotations(
        self,
        *,
        limit: int = 20,
        batch_size: int = 200,
        tenant_id: str | None = None,
    ) -> list[PayloadKeyRotation]:
        """Resume incomplete rotations (and leftover plaintext) after a crash."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        tenants: list[str]
        with self._pool.connection() as conn:
            if tenant_id:
                tenants = [tenant_id]
            else:
                rows = conn.execute(
                    """
                    SELECT DISTINCT tenant_id
                    FROM payload_key_rotations
                    WHERE status = 'reencrypting'
                    ORDER BY tenant_id
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
                tenants = [str(r["tenant_id"]) for r in rows]
        out: list[PayloadKeyRotation] = []
        for tid in tenants[:limit]:
            out.append(
                self.advance_payload_key_rotation(tid, batch_size=batch_size)
            )
        return out

    def _count_unsealed(self, conn: Any, tenant_id: str, active_key_id: UUID) -> int:
        total = 0
        for table in (
            "jobs",
            "schedules",
            "webhook_deliveries",
            "workflow_runs",
        ):
            row = conn.execute(
                f"""
                SELECT COUNT(*)::int AS n
                FROM {table}
                WHERE tenant_id = %s
                  AND (payload_key_id IS DISTINCT FROM %s)
                """,
                (tenant_id, active_key_id),
            ).fetchone()
            total += int(row["n"] if row else 0)
        return total

    def _reencrypt_jobs_batch(
        self,
        conn: Any,
        *,
        tenant_id: str,
        to_key_id: UUID,
        limit: int,
    ) -> int:
        rec = self._ensure_active_key(conn, tenant_id)
        if rec.key_id != to_key_id:
            # Mint vs advance race: use whatever is active now.
            to_key_id = rec.key_id
        rows = conn.execute(
            """
            SELECT id, payload, result, payload_key_id
            FROM jobs
            WHERE tenant_id = %s
              AND (payload_key_id IS DISTINCT FROM %s)
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (tenant_id, to_key_id, limit),
        ).fetchall()
        n = 0
        for row in rows:
            plain = self._open_job_payload(
                row["payload"],
                tenant_id=tenant_id,
                job_id=row["id"],
                conn=conn,
            )
            env = encrypt_json(
                rec.dek,
                key_id=rec.key_id,
                plaintext=plain,
                aad=job_aad(row["id"]),
            )
            result_env = row.get("result")
            if result_env is not None:
                opened_result = self._open_json_value(
                    result_env,
                    tenant_id=tenant_id,
                    aad=result_aad(row["id"]),
                    conn=conn,
                )
                result_env, _ = self._seal_json_value(
                    conn,
                    tenant_id=tenant_id,
                    value=opened_result,
                    aad=result_aad(row["id"]),
                )
            conn.execute(
                """
                UPDATE jobs
                SET payload = %s,
                    result = %s,
                    payload_key_id = %s,
                    updated_at = NOW()
                WHERE id = %s AND tenant_id = %s
                """,
                (
                    Jsonb(env),
                    None if result_env is None else Jsonb(result_env),
                    rec.key_id,
                    row["id"],
                    tenant_id,
                ),
            )
            n += 1
        return n

    def _reencrypt_schedules_batch(
        self,
        conn: Any,
        *,
        tenant_id: str,
        to_key_id: UUID,
        limit: int,
    ) -> int:
        rec = self._ensure_active_key(conn, tenant_id)
        if rec.key_id != to_key_id:
            to_key_id = rec.key_id
        rows = conn.execute(
            """
            SELECT id, payload, payload_key_id
            FROM schedules
            WHERE tenant_id = %s
              AND (payload_key_id IS DISTINCT FROM %s)
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (tenant_id, to_key_id, limit),
        ).fetchall()
        n = 0
        for row in rows:
            plain = self._open_schedule_payload(
                row["payload"],
                tenant_id=tenant_id,
                schedule_id=row["id"],
                conn=conn,
            )
            env = encrypt_json(
                rec.dek,
                key_id=rec.key_id,
                plaintext=plain,
                aad=schedule_aad(row["id"]),
            )
            conn.execute(
                """
                UPDATE schedules
                SET payload = %s,
                    payload_key_id = %s,
                    updated_at = NOW()
                WHERE id = %s AND tenant_id = %s
                """,
                (Jsonb(env), rec.key_id, row["id"], tenant_id),
            )
            n += 1
        return n

    def _reencrypt_deliveries_batch(
        self,
        conn: Any,
        *,
        tenant_id: str,
        to_key_id: UUID,
        limit: int,
    ) -> int:
        rec = self._ensure_active_key(conn, tenant_id)
        if rec.key_id != to_key_id:
            to_key_id = rec.key_id
        rows = conn.execute(
            """
            SELECT id, payload, payload_key_id
            FROM webhook_deliveries
            WHERE tenant_id = %s
              AND (payload_key_id IS DISTINCT FROM %s)
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (tenant_id, to_key_id, limit),
        ).fetchall()
        n = 0
        for row in rows:
            plain = self._open_dict(
                row["payload"],
                tenant_id=tenant_id,
                aad=delivery_aad(row["id"]),
                conn=conn,
            )
            env = encrypt_json(
                rec.dek,
                key_id=rec.key_id,
                plaintext=plain,
                aad=delivery_aad(row["id"]),
            )
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET payload = %s,
                    payload_key_id = %s,
                    updated_at = NOW()
                WHERE id = %s AND tenant_id = %s
                """,
                (Jsonb(env), rec.key_id, row["id"], tenant_id),
            )
            n += 1
        return n

    def _reencrypt_workflow_runs_batch(
        self,
        conn: Any,
        *,
        tenant_id: str,
        to_key_id: UUID,
        limit: int,
    ) -> int:
        rec = self._ensure_active_key(conn, tenant_id)
        if rec.key_id != to_key_id:
            to_key_id = rec.key_id
        rows = conn.execute(
            """
            SELECT id, tenant_id, steps_def, payload_key_id
            FROM workflow_runs
            WHERE tenant_id = %s
              AND (payload_key_id IS DISTINCT FROM %s)
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (tenant_id, to_key_id, limit),
        ).fetchall()
        n = 0
        for row in rows:
            steps = self._parse_steps_def(row, conn)
            env, kid = self._seal_dict(
                conn,
                tenant_id=tenant_id,
                plaintext={"steps": steps},
                aad=workflow_steps_aad(row["id"]),
            )
            conn.execute(
                """
                UPDATE workflow_runs
                SET steps_def = %s,
                    payload_key_id = %s,
                    updated_at = NOW()
                WHERE id = %s AND tenant_id = %s
                """,
                (Jsonb(env), kid, row["id"], tenant_id),
            )
            n += 1
        return n

    def _retire_unreferenced_keys(self, conn: Any, *, tenant_id: str) -> int:
        """Zeroize wrapped DEKs that no ciphertext still names.

        Never deletes the active key. Crash after zeroize is not a leak:
        material is already gone and no ciphertext needed it.
        """
        crypto = self._require_crypto()
        rows = conn.execute(
            """
            SELECT key_id
            FROM tenant_payload_keys
            WHERE tenant_id = %s
              AND status = 'decrypt_only'
            FOR UPDATE
            """,
            (tenant_id,),
        ).fetchall()
        retired = 0
        for row in rows:
            kid = row["key_id"]
            job_ref = conn.execute(
                """
                SELECT 1 FROM jobs
                WHERE tenant_id = %s AND payload_key_id = %s
                LIMIT 1
                """,
                (tenant_id, kid),
            ).fetchone()
            if job_ref is not None:
                continue
            sched_ref = conn.execute(
                """
                SELECT 1 FROM schedules
                WHERE tenant_id = %s AND payload_key_id = %s
                LIMIT 1
                """,
                (tenant_id, kid),
            ).fetchone()
            if sched_ref is not None:
                continue
            # Also treat envelope kid on legacy rows missing payload_key_id.
            env_job = conn.execute(
                """
                SELECT 1 FROM jobs
                WHERE tenant_id = %s
                  AND payload_key_id IS NULL
                  AND payload ? '_jq_enc'
                  AND payload->>'kid' = %s
                LIMIT 1
                """,
                (tenant_id, str(kid)),
            ).fetchone()
            if env_job is not None:
                continue
            env_sched = conn.execute(
                """
                SELECT 1 FROM schedules
                WHERE tenant_id = %s
                  AND payload_key_id IS NULL
                  AND payload ? '_jq_enc'
                  AND payload->>'kid' = %s
                LIMIT 1
                """,
                (tenant_id, str(kid)),
            ).fetchone()
            if env_sched is not None:
                continue
            deliv_ref = conn.execute(
                """
                SELECT 1 FROM webhook_deliveries
                WHERE tenant_id = %s AND payload_key_id = %s
                LIMIT 1
                """,
                (tenant_id, kid),
            ).fetchone()
            if deliv_ref is not None:
                continue
            wf_ref = conn.execute(
                """
                SELECT 1 FROM workflow_runs
                WHERE tenant_id = %s AND payload_key_id = %s
                LIMIT 1
                """,
                (tenant_id, kid),
            ).fetchone()
            if wf_ref is not None:
                continue
            conn.execute(
                """
                UPDATE tenant_payload_keys
                SET wrapped_dek = '\\x'::bytea,
                    wrap_nonce = '\\x'::bytea,
                    status = 'retired',
                    retired_at = NOW()
                WHERE tenant_id = %s AND key_id = %s AND status = 'decrypt_only'
                """,
                (tenant_id, kid),
            )
            crypto.drop_dek(tenant_id, kid)
            retired += 1
        return retired
