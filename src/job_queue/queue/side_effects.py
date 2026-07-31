"""JobQueue side_effects domain (SideEffectsMixin)."""
from __future__ import annotations

import json

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import init_schema, transaction
from job_queue.models import (
    AuditConsistencyReport,
    BulkCancelResult,
    Job,
    JobAuditEvent,
    JobProgressEvent,
    JobRetentionSettings,
    JobSideEffect,
    JobStatus,
    PreemptResult,
    RetentionPurgeResult,
    Schedule,
    ScheduleFire,
    SpawnChildrenResult,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
    _ACTIVE_JOB_STATUSES,
    _CANCELABLE_STATUSES,
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _DEDUPE_LOCK_CLASS,
    _DEFAULT_ESTIMATE_SECONDS,
    _DEPS_SATISFIED_SQL,
    _EWMA_ALPHA,
    _JOB_COLUMNS,
    _MAX_LABELS,
    _MAX_LABEL_LEN,
    _MAX_PREEMPTS_PER_JOB,
    _NOTIFY_MAX_BYTES,
    _SCHEDULE_COLUMNS,
    _SIDE_EFFECT_SELECT,
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
    ApprovalError,
    ApprovalRequested,
    CancelError,
    ChildrenAwaited,
    DependencyError,
    LeaseError,
    MergeError,
    RateLimitError,
    RebuildError,
    ReplayError,
    ScheduleError,
    SpawnError,
    _SkipLeaseCandidate,
    _as_utc,
    _normalize_labels,
    _normalize_status_filter,
    _row_to_job,
    _row_to_schedule,
    _utcnow,
    compute_retry_backoff_seconds,
)

class SideEffectsMixin:
    _SIDE_EFFECT_SELECT = _SIDE_EFFECT_SELECT

    def side_effect_done(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        effect_key: str,
    ) -> bool:
        """True if *effect_key* was already completed for this job (any worker)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        key = self._normalize_effect_key(effect_key)
        with self._pool.connection() as conn:
            # Allow read if job currently owned by tenant OR effect row tenant matches
            # (covers merge: effects stay by job_id).
            row = conn.execute(
                """
                SELECT 1
                FROM job_side_effects se
                WHERE se.job_id = %s
                  AND se.effect_key = %s
                  AND se.status = 'completed'
                  AND (
                        se.tenant_id = %s
                        OR EXISTS (
                            SELECT 1 FROM jobs j
                            WHERE j.id = se.job_id AND j.tenant_id = %s
                        )
                  )
                """,
                (job_id, key, tenant_id, tenant_id),
            ).fetchone()
            return row is not None


    def get_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        effect_key: str,
    ) -> JobSideEffect | None:
        """Return a side-effect marker (started or completed), or None."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        key = self._normalize_effect_key(effect_key)
        cols = self._SIDE_EFFECT_SELECT
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {cols}
                FROM job_side_effects se
                WHERE se.job_id = %s
                  AND se.effect_key = %s
                  AND (
                        se.tenant_id = %s
                        OR EXISTS (
                            SELECT 1 FROM jobs j
                            WHERE j.id = se.job_id AND j.tenant_id = %s
                        )
                  )
                """,
                (job_id, key, tenant_id, tenant_id),
            ).fetchone()
            return self._row_to_side_effect(row) if row else None


    def list_side_effects(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
    ) -> list[JobSideEffect]:
        """List side effects for a job (tenant-authorized via ownership)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        cols = self._SIDE_EFFECT_SELECT
        with self._pool.connection() as conn:
            job = conn.execute(
                "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
                (job_id, tenant_id),
            ).fetchone()
            if job is None:
                # Still allow if effects exist under this tenant (job missing/rebuild).
                rows = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s AND tenant_id = %s
                    ORDER BY effect_key ASC
                    """,
                    (job_id, tenant_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s
                    ORDER BY effect_key ASC
                    """,
                    (job_id,),
                ).fetchall()
            return [self._row_to_side_effect(r) for r in rows]


    def begin_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        effect_key: str,
        idempotency_key: str | None = None,
    ) -> JobSideEffect:
        """Record intent to run *effect_key* **before** the external call.

        Inserts ``status=started`` with a stable ``idempotency_key`` (default
        ``{job_id}:{effect_key}``). If already ``started`` or ``completed``,
        returns the existing row unchanged (same key for reclaim retries).

        Call this under the live lease, then invoke the external system with
        the returned ``idempotency_key``, then :meth:`complete_side_effect`.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not worker_id:
            raise ValueError("worker_id is required")
        key = self._normalize_effect_key(effect_key)
        now = _utcnow()
        idem = (
            str(idempotency_key).strip()
            if idempotency_key is not None and str(idempotency_key).strip()
            else f"{job_id}:{key}"
        )
        if len(idem) > 512:
            raise ValueError("idempotency_key must be <= 512 characters")

        cols = self._SIDE_EFFECT_SELECT
        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)
            owner = row["tenant_id"]

            existing = conn.execute(
                f"""
                SELECT {cols}
                FROM job_side_effects
                WHERE job_id = %s AND effect_key = %s
                FOR UPDATE
                """,
                (job_id, key),
            ).fetchone()
            if existing is not None:
                # Already started or completed — reclaim must reuse this key.
                if existing["status"] == "started":
                    # Track that a new lease is retrying; keep original key/time.
                    updated = conn.execute(
                        f"""
                        UPDATE job_side_effects
                        SET lease_token = %s
                        WHERE job_id = %s AND effect_key = %s AND status = 'started'
                        RETURNING {cols}
                        """,
                        (lease_token, job_id, key),
                    ).fetchone()
                    return self._row_to_side_effect(updated or existing)
                return self._row_to_side_effect(existing)

            inserted = conn.execute(
                f"""
                INSERT INTO job_side_effects (
                    job_id, effect_key, tenant_id, status, idempotency_key,
                    result, started_by, started_at, completed_by, lease_token,
                    created_at, completed_at
                ) VALUES (
                    %s, %s, %s, 'started', %s,
                    NULL, %s, %s, NULL, %s,
                    %s, NULL
                )
                ON CONFLICT (job_id, effect_key) DO NOTHING
                RETURNING {cols}
                """,
                (
                    job_id,
                    key,
                    owner,
                    idem,
                    worker_id,
                    now,
                    lease_token,
                    now,
                ),
            ).fetchone()
            if inserted is None:
                existing = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s AND effect_key = %s
                    """,
                    (job_id, key),
                ).fetchone()
                assert existing is not None
                return self._row_to_side_effect(existing)

            self._append_job_audit(
                conn,
                job_row=row,
                event_type="side_effect_started",
                from_status=row["status"],
                actor=worker_id,
                detail={
                    "effect_key": key,
                    "idempotency_key": idem,
                },
                now=now,
            )
            return self._row_to_side_effect(inserted)


    def complete_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        effect_key: str,
        result: Any = None,
    ) -> JobSideEffect:
        """Mark a side effect completed under a live lease (idempotent).

        Prefer calling :meth:`begin_side_effect` first. Completing an existing
        ``started`` row upgrades it; completing with no prior begin inserts a
        completed row (legacy path). Safe to call again: returns the existing
        completed row without changing it.

        Call **after** a successful external side effect that used the
        begin-phase ``idempotency_key``.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not worker_id:
            raise ValueError("worker_id is required")
        key = self._normalize_effect_key(effect_key)
        now = _utcnow()
        result_json = Jsonb(result) if result is not None else None
        default_idem = f"{job_id}:{key}"
        cols = self._SIDE_EFFECT_SELECT

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)
            owner = row["tenant_id"]

            existing = conn.execute(
                f"""
                SELECT {cols}
                FROM job_side_effects
                WHERE job_id = %s AND effect_key = %s
                FOR UPDATE
                """,
                (job_id, key),
            ).fetchone()
            if existing is not None and existing["status"] == "completed":
                return self._row_to_side_effect(existing)

            if existing is not None and existing["status"] == "started":
                updated = conn.execute(
                    f"""
                    UPDATE job_side_effects
                    SET status = 'completed',
                        result = %s,
                        completed_by = %s,
                        lease_token = %s,
                        completed_at = %s
                    WHERE job_id = %s AND effect_key = %s AND status = 'started'
                    RETURNING {cols}
                    """,
                    (
                        result_json,
                        worker_id,
                        lease_token,
                        now,
                        job_id,
                        key,
                    ),
                ).fetchone()
                if updated is not None:
                    self._append_job_audit(
                        conn,
                        job_row=row,
                        event_type="side_effect_completed",
                        from_status=row["status"],
                        actor=worker_id,
                        detail={
                            "effect_key": key,
                            "idempotency_key": existing["idempotency_key"],
                            "has_result": result is not None,
                            "from_status": "started",
                        },
                        now=now,
                    )
                    return self._row_to_side_effect(updated)
                # Race: concurrent complete finished it.
                final = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s AND effect_key = %s
                    """,
                    (job_id, key),
                ).fetchone()
                assert final is not None
                return self._row_to_side_effect(final)

            # No prior begin — insert completed (legacy single-call path).
            inserted = conn.execute(
                f"""
                INSERT INTO job_side_effects (
                    job_id, effect_key, tenant_id, status, idempotency_key,
                    result, started_by, started_at, completed_by, lease_token,
                    created_at, completed_at
                ) VALUES (
                    %s, %s, %s, 'completed', %s,
                    %s, %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (job_id, effect_key) DO NOTHING
                RETURNING {cols}
                """,
                (
                    job_id,
                    key,
                    owner,
                    default_idem,
                    result_json,
                    worker_id,
                    now,
                    worker_id,
                    lease_token,
                    now,
                    now,
                ),
            ).fetchone()
            if inserted is None:
                existing = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s AND effect_key = %s
                    """,
                    (job_id, key),
                ).fetchone()
                assert existing is not None
                return self._row_to_side_effect(existing)

            self._append_job_audit(
                conn,
                job_row=row,
                event_type="side_effect_completed",
                from_status=row["status"],
                actor=worker_id,
                detail={
                    "effect_key": key,
                    "idempotency_key": default_idem,
                    "has_result": result is not None,
                    "from_status": None,
                },
                now=now,
            )
            return self._row_to_side_effect(inserted)


    @staticmethod
    def _normalize_effect_key(effect_key: str) -> str:
        if not effect_key or not str(effect_key).strip():
            raise ValueError("effect_key is required")
        key = str(effect_key).strip()
        if len(key) > 256:
            raise ValueError("effect_key must be <= 256 characters")
        return key


    @staticmethod
    def _row_to_side_effect(row: dict[str, Any]) -> JobSideEffect:
        result = row.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                pass
        idem = row.get("idempotency_key")
        if not idem:
            idem = f"{row['job_id']}:{row['effect_key']}"
        return JobSideEffect(
            job_id=row["job_id"],
            effect_key=row["effect_key"],
            tenant_id=row["tenant_id"],
            status=row["status"],
            idempotency_key=str(idem),
            result=result,
            started_by=row.get("started_by"),
            started_at=row.get("started_at"),
            completed_by=row.get("completed_by"),
            lease_token=row.get("lease_token"),
            created_at=row["created_at"],
            completed_at=row.get("completed_at"),
        )


