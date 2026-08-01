"""JobQueue job retention settings and purge."""

from __future__ import annotations

from typing import Any

from job_queue.db import transaction
from job_queue.models import (
    JobRetentionSettings,
    JobStatus,
    RetentionPurgeResult,
)
from job_queue.queue._common import (
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _utcnow,
)


class TenantsRetentionMixin:
    def set_job_retention(
        self,
        *,
        completed_seconds: int | None = None,
        dead_seconds: int | None = None,
        metrics_seconds: Any = _UNSET,
    ) -> JobRetentionSettings:
        """Set global retention for terminal jobs and metric history (seconds).

        Pass ``None`` for a status to keep those jobs forever. ``0`` means
        eligible for purge on the next cleaner pass. Per-tenant overrides via
        :meth:`set_tenant_job_retention` take precedence when set.

        *metrics_seconds* controls ``queue_metric_buckets`` /
        ``queue_metric_snapshots`` age (``None`` = keep forever). Omit to leave
        the current metrics retention unchanged.
        """
        if completed_seconds is not None and completed_seconds < 0:
            raise ValueError("completed_seconds must be >= 0 or None")
        if dead_seconds is not None and dead_seconds < 0:
            raise ValueError("dead_seconds must be >= 0 or None")
        if metrics_seconds is not _UNSET:
            if metrics_seconds is not None and int(metrics_seconds) < 0:
                raise ValueError("metrics_seconds must be >= 0 or None")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO queue_settings (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING
                """
            )
            if metrics_seconds is _UNSET:
                conn.execute(
                    """
                    UPDATE queue_settings
                    SET completed_retention_seconds = %s,
                        dead_retention_seconds = %s,
                        updated_at = %s
                    WHERE id = 1
                    """,
                    (completed_seconds, dead_seconds, now),
                )
            else:
                m_sec = None if metrics_seconds is None else int(metrics_seconds)
                conn.execute(
                    """
                    UPDATE queue_settings
                    SET completed_retention_seconds = %s,
                        dead_retention_seconds = %s,
                        metrics_retention_seconds = %s,
                        updated_at = %s
                    WHERE id = 1
                    """,
                    (completed_seconds, dead_seconds, m_sec, now),
                )
        return self.get_job_retention()

    def get_job_retention(self) -> JobRetentionSettings:
        """Return global completed/dead/metrics retention (None = keep forever)."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds,
                       metrics_retention_seconds
                FROM queue_settings
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                return JobRetentionSettings(None, None, metrics_retention_seconds=None)
            return JobRetentionSettings(
                completed_retention_seconds=row["completed_retention_seconds"],
                dead_retention_seconds=row["dead_retention_seconds"],
                metrics_retention_seconds=row.get("metrics_retention_seconds"),
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
            prev_c = existing["completed_retention_seconds"] if existing else None
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
            # Metric history purge shares the cleaner pass (global, not per-tenant).
            buckets_n, snaps_n = self._purge_expired_metrics_locked(
                conn, now=now, limit=limit
            )
            return RetentionPurgeResult(
                completed_deleted=len(completed_ids),
                dead_deleted=len(dead_ids),
                side_effects_deleted=side_n,
                metric_buckets_deleted=buckets_n,
                metric_snapshots_deleted=snaps_n,
            )
