"""JobQueue tenant limits domain: concurrency, pending, enqueue rate, retention, purge."""
from __future__ import annotations

import math

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


class TenantsLimitsMixin:
    def set_concurrency_limit(
        self,
        tenant_id: str,
        max_concurrent: int | None,
    ) -> None:
        """Set how many jobs may be leased at once for *tenant_id*.

        Pass ``None`` to clear the limit (unlimited). Does not clear *paused*.
        ``max_concurrent`` must be >= 1 when set.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1 or None")

        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, %s, NULL, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET max_concurrent_leases = EXCLUDED.max_concurrent_leases,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, max_concurrent, now),
            )
            # Drop empty settings rows (no limits, not paused, not offboarding).
            if max_concurrent is None:
                self._delete_empty_tenant_settings(conn, tenant_id)

    def get_concurrency_limit(self, tenant_id: str) -> int | None:
        """Return the tenant's max concurrent leases, or None if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT max_concurrent_leases
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                return None
            return row["max_concurrent_leases"]

    def set_pending_limit(
        self,
        tenant_id: str,
        max_pending: int | None,
    ) -> None:
        """Set how many *pending* jobs a tenant may hold in the queue.

        When the cap is reached, further ``enqueue`` (and schedule ticks /
        dead-letter replay that would create a new pending row) fail with
        ``ValueError``. Pass ``None`` to clear (unlimited). Does not clear
        *paused* / concurrency / offboarding. ``max_pending`` must be >= 1
        when set.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_pending is not None and max_pending < 1:
            raise ValueError("max_pending must be >= 1 or None")

        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, %s, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET max_pending_jobs = EXCLUDED.max_pending_jobs,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, max_pending, now),
            )
            if max_pending is None:
                self._delete_empty_tenant_settings(conn, tenant_id)

    def get_pending_limit(self, tenant_id: str) -> int | None:
        """Return the tenant's max pending jobs, or None if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._read_pending_limit(conn, tenant_id)

    def set_enqueue_rate_limit(
        self,
        tenant_id: str,
        per_second: float | None,
        *,
        burst: int | None = None,
    ) -> None:
        """Cap how fast *tenant_id* may enqueue new jobs (token bucket).

        *per_second* is the steady refill rate. *burst* is the max bucket
        size (defaults to ``max(1, ceil(per_second))``). Pass ``None`` for
        *per_second* to clear (unlimited). Does not clear pause / other caps.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        rate, burst_n = self._normalize_rate_limit(per_second, burst)
        now = _utcnow()
        with transaction(self._pool) as conn:
            if rate is None:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, max_concurrent_leases, max_pending_jobs,
                        paused, offboarding, updated_at
                    )
                    VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET enqueue_rate_per_sec = NULL,
                        enqueue_burst = NULL,
                        enqueue_tokens = NULL,
                        enqueue_tokens_at = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return
            assert burst_n is not None
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    enqueue_rate_per_sec, enqueue_burst,
                    enqueue_tokens, enqueue_tokens_at,
                    updated_at
                )
                VALUES (
                    %s, NULL, NULL, FALSE, FALSE,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET enqueue_rate_per_sec = EXCLUDED.enqueue_rate_per_sec,
                    enqueue_burst = EXCLUDED.enqueue_burst,
                    enqueue_tokens = EXCLUDED.enqueue_tokens,
                    enqueue_tokens_at = EXCLUDED.enqueue_tokens_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, rate, burst_n, float(burst_n), now, now),
            )

    def get_enqueue_rate_limit(
        self, tenant_id: str
    ) -> tuple[float | None, int | None]:
        """Return ``(per_second, burst)`` or ``(None, None)`` if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT enqueue_rate_per_sec, enqueue_burst
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None or row["enqueue_rate_per_sec"] is None:
                return None, None
            return float(row["enqueue_rate_per_sec"]), (
                int(row["enqueue_burst"])
                if row["enqueue_burst"] is not None
                else None
            )

    def set_job_retention(
        self,
        *,
        completed_seconds: int | None = None,
        dead_seconds: int | None = None,
    ) -> JobRetentionSettings:
        """Set global retention for terminal jobs (seconds).

        Pass ``None`` for a status to keep those jobs forever. ``0`` means
        eligible for purge on the next cleaner pass. Per-tenant overrides via
        :meth:`set_tenant_job_retention` take precedence when set.
        """
        if completed_seconds is not None and completed_seconds < 0:
            raise ValueError("completed_seconds must be >= 0 or None")
        if dead_seconds is not None and dead_seconds < 0:
            raise ValueError("dead_seconds must be >= 0 or None")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO queue_settings (
                    id, completed_retention_seconds, dead_retention_seconds,
                    updated_at
                ) VALUES (1, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET completed_retention_seconds = EXCLUDED.completed_retention_seconds,
                    dead_retention_seconds = EXCLUDED.dead_retention_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                (completed_seconds, dead_seconds, now),
            )
        return JobRetentionSettings(
            completed_retention_seconds=completed_seconds,
            dead_retention_seconds=dead_seconds,
        )

    def get_job_retention(self) -> JobRetentionSettings:
        """Return global completed/dead retention (None = keep forever)."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM queue_settings
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                return JobRetentionSettings(None, None)
            return JobRetentionSettings(
                completed_retention_seconds=row["completed_retention_seconds"],
                dead_retention_seconds=row["dead_retention_seconds"],
            )

    def set_tenant_job_retention(
        self,
        tenant_id: str,
        *,
        completed_seconds: Any = _UNSET,
        dead_seconds: Any = _UNSET,
        clear: bool = False,
    ) -> JobRetentionSettings:
        """Override retention for one tenant (or clear back to global).

        When *clear* is True both overrides are removed (inherit global).

        Otherwise pass *completed_seconds* and/or *dead_seconds*:
        ``None`` means **keep forever for this tenant** (does **not** inherit
        global). Pass a non-negative int for a finite window. Omitted keys
        keep their previous override (or inherit if never set). Use *clear*
        (not ``None``) to fall back to global defaults.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not clear:
            if completed_seconds is not _UNSET:
                if completed_seconds is not None and int(completed_seconds) < 0:
                    raise ValueError("completed_seconds must be >= 0 or None")
            if dead_seconds is not _UNSET:
                if dead_seconds is not None and int(dead_seconds) < 0:
                    raise ValueError("dead_seconds must be >= 0 or None")
            if completed_seconds is _UNSET and dead_seconds is _UNSET:
                raise ValueError(
                    "provide completed_seconds and/or dead_seconds, or clear=True"
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
                    SET completed_retention_seconds = NULL,
                        dead_retention_seconds = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return JobRetentionSettings(
                    None,
                    None,
                    completed_inherits=True,
                    dead_inherits=True,
                )
            # Merge with existing stored overrides.
            existing = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM tenant_settings
                WHERE tenant_id = %s
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            prev_c = (
                existing["completed_retention_seconds"] if existing else None
            )
            prev_d = existing["dead_retention_seconds"] if existing else None

            def _to_store(val: Any, prev: Any) -> int | None:
                if val is _UNSET:
                    return prev  # may be None (inherit)
                if val is None:
                    return _TENANT_RETENTION_FOREVER
                return int(val)

            c_store = _to_store(completed_seconds, prev_c)
            d_store = _to_store(dead_seconds, prev_d)
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    completed_retention_seconds, dead_retention_seconds,
                    updated_at
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET completed_retention_seconds = EXCLUDED.completed_retention_seconds,
                    dead_retention_seconds = EXCLUDED.dead_retention_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, c_store, d_store, now),
            )
            return self._decode_tenant_retention_row(
                {
                    "completed_retention_seconds": c_store,
                    "dead_retention_seconds": d_store,
                }
            )

    def get_tenant_job_retention(self, tenant_id: str) -> JobRetentionSettings:
        """Return per-tenant retention override.

        * ``completed_inherits`` / ``dead_inherits`` True → use global.
        * inherits False and seconds ``None`` → explicit forever for tenant.
        * inherits False and seconds ``>= 0`` → finite tenant window.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                return JobRetentionSettings(
                    None,
                    None,
                    completed_inherits=True,
                    dead_inherits=True,
                )
            return self._decode_tenant_retention_row(row)

    @staticmethod
    def _decode_tenant_retention_row(row: dict[str, Any]) -> JobRetentionSettings:
        def _one(raw: Any) -> tuple[int | None, bool]:
            if raw is None:
                return None, True  # inherit
            n = int(raw)
            if n < 0:
                return None, False  # forever
            return n, False

        c_sec, c_inh = _one(row.get("completed_retention_seconds"))
        d_sec, d_inh = _one(row.get("dead_retention_seconds"))
        return JobRetentionSettings(
            completed_retention_seconds=c_sec,
            dead_retention_seconds=d_sec,
            completed_inherits=c_inh,
            dead_inherits=d_inh,
        )

    def purge_expired_jobs(
        self,
        *,
        limit: int = 500,
        tenant_id: str | None = None,
    ) -> RetentionPurgeResult:
        """Delete terminal jobs older than their retention window.

        Effective retention per job:

        * tenant override ``>= 0`` → that many seconds
        * tenant override ``-1`` (API ``None``) → keep forever (ignore global)
        * tenant override NULL / no row → global setting
        * global ``NULL`` → keep forever

        Also deletes matching ``job_side_effects`` rows. Audit is retained.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if limit > 10_000:
            raise ValueError("limit must be <= 10000")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")

        now = _utcnow()
        half = max(1, limit // 2)
        with transaction(self._pool) as conn:
            # Ensure global row exists for JOIN.
            conn.execute(
                """
                INSERT INTO queue_settings (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING
                """
            )
            completed_ids = self._select_expired_terminal_ids(
                conn,
                status=JobStatus.COMPLETED.value,
                now=now,
                limit=half,
                tenant_id=tenant_id,
            )
            dead_ids = self._select_expired_terminal_ids(
                conn,
                status=JobStatus.DEAD.value,
                now=now,
                limit=limit - len(completed_ids),
                tenant_id=tenant_id,
            )
            all_ids = completed_ids + dead_ids
            side_n = 0
            if all_ids:
                se = conn.execute(
                    """
                    DELETE FROM job_side_effects
                    WHERE job_id = ANY(%s)
                    RETURNING job_id, effect_key
                    """,
                    (all_ids,),
                ).fetchall()
                side_n = len(se)
                conn.execute(
                    "DELETE FROM jobs WHERE id = ANY(%s)",
                    (all_ids,),
                )
            return RetentionPurgeResult(
                completed_deleted=len(completed_ids),
                dead_deleted=len(dead_ids),
                side_effects_deleted=side_n,
            )

    @staticmethod
    def _read_concurrency_limit(conn: Any, tenant_id: str) -> int | None:
        row = conn.execute(
            """
            SELECT max_concurrent_leases
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return None
        return row["max_concurrent_leases"]

    @staticmethod
    def _read_pending_limit(conn: Any, tenant_id: str) -> int | None:
        row = conn.execute(
            """
            SELECT max_pending_jobs
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return None
        return row["max_pending_jobs"]

    def _assert_pending_capacity(self, conn: Any, tenant_id: str) -> None:
        """Raise ValueError if tenant is at its pending job cap.

        Serializes with other enqueue / schedule / replay paths for this
        tenant via a transaction-scoped advisory lock.
        """
        limit = self._read_pending_limit(conn, tenant_id)
        if limit is None:
            return
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (tenant_id,),
        )
        # Re-read under lock in case settings changed (same txn usually static).
        limit = self._read_pending_limit(conn, tenant_id)
        if limit is None:
            return
        n = self._count_pending(conn, tenant_id)
        if n >= limit:
            raise ValueError(
                f"tenant {tenant_id!r} pending job cap reached "
                f"({n}/{limit}); enqueue is rejected"
            )

    @staticmethod
    def _normalize_rate_limit(
        per_second: float | None,
        burst: int | None,
    ) -> tuple[float | None, int | None]:
        if per_second is None:
            if burst is not None:
                raise ValueError("burst requires per_second to be set")
            return None, None
        rate = float(per_second)
        if rate <= 0:
            raise ValueError("per_second must be > 0 or None")
        if burst is None:
            burst_n = max(1, int(math.ceil(rate)))
        else:
            burst_n = int(burst)
            if burst_n < 1:
                raise ValueError("burst must be >= 1")
        return rate, burst_n

    def _consume_enqueue_rate(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> None:
        """Consume one enqueue token or raise :class:`RateLimitError`."""
        ok, retry_after = self._try_consume_rate_tokens(
            conn,
            tenant_id,
            kind="enqueue",
            now=now,
            cost=1.0,
        )
        if ok:
            return
        raise RateLimitError(
            f"tenant {tenant_id!r} enqueue rate limit exceeded"
            + (
                f"; retry after ~{retry_after:.3f}s"
                if retry_after is not None
                else ""
            ),
            tenant_id=tenant_id,
            kind="enqueue",
            retry_after_seconds=retry_after,
        )

    def _try_consume_rate_tokens(
        self,
        conn: Any,
        tenant_id: str,
        *,
        kind: str,
        now: datetime,
        cost: float = 1.0,
    ) -> tuple[bool, float | None]:
        """Token-bucket consume under the tenant advisory lock.

        Returns ``(True, None)`` if allowed (or unlimited), else
        ``(False, retry_after_seconds)``.
        """
        if kind not in ("enqueue", "lease"):
            raise ValueError(f"unknown rate kind {kind!r}")
        rate_col = f"{kind}_rate_per_sec"
        burst_col = f"{kind}_burst"
        tokens_col = f"{kind}_tokens"
        at_col = f"{kind}_tokens_at"

        # Caller often already holds the tenant lock; take it again (re-entrant
        # xact lock is fine) so standalone paths are safe.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (tenant_id,),
        )
        row = conn.execute(
            f"""
            SELECT {rate_col} AS rate, {burst_col} AS burst,
                   {tokens_col} AS tokens, {at_col} AS tokens_at
            FROM tenant_settings
            WHERE tenant_id = %s
            FOR UPDATE
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["rate"] is None:
            return True, None

        rate = float(row["rate"])
        if rate <= 0:
            return True, None
        burst = int(row["burst"] or max(1, int(math.ceil(rate))))
        tokens = row["tokens"]
        tokens_at = row["tokens_at"]
        if tokens is None:
            tokens = float(burst)
        else:
            tokens = float(tokens)
        if tokens_at is None:
            tokens_at = now
        else:
            tokens_at = _as_utc(tokens_at)

        elapsed = max(0.0, (now - tokens_at).total_seconds())
        tokens = min(float(burst), tokens + elapsed * rate)
        if tokens + 1e-12 < cost:
            need = cost - tokens
            retry_after = need / rate if rate > 0 else None
            # Persist refilled (but not consumed) tokens so time advances.
            conn.execute(
                f"""
                UPDATE tenant_settings
                SET {tokens_col} = %s,
                    {at_col} = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                """,
                (tokens, now, now, tenant_id),
            )
            return False, retry_after

        tokens -= cost
        conn.execute(
            f"""
            UPDATE tenant_settings
            SET {tokens_col} = %s,
                {at_col} = %s,
                updated_at = %s
            WHERE tenant_id = %s
            """,
            (tokens, now, now, tenant_id),
        )
        return True, None

    @staticmethod
    def _delete_empty_tenant_settings(conn: Any, tenant_id: str) -> None:
        conn.execute(
            """
            DELETE FROM tenant_settings
            WHERE tenant_id = %s
              AND max_concurrent_leases IS NULL
              AND max_pending_jobs IS NULL
              AND enqueue_rate_per_sec IS NULL
              AND lease_rate_per_sec IS NULL
              AND completed_retention_seconds IS NULL
              AND dead_retention_seconds IS NULL
              AND paused = FALSE
              AND offboarding = FALSE
            """,
            (tenant_id,),
        )
