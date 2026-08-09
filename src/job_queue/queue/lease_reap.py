"""JobQueue process_deadlines and offboarding lease reaping."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    PreemptResult,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _utcnow,
)


class LeaseReapMixin:
    def process_deadlines(
        self,
        *,
        tenant_id: str | None = None,
        queue_name: str = "default",
        limit: int = 20,
    ) -> list[PreemptResult]:
        """Proactively preempt less-urgent work for deadline-critical pending.

        Safe to call from a scheduler loop. Returns one
        :class:`~job_queue.models.PreemptResult` per successful preemption.
        Lease paths also invoke the same logic when a tenant is at its
        concurrency cap.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        results: list[PreemptResult] = []
        with transaction(self._pool) as conn:
            if tenant_id is not None:
                tenants = [tenant_id]
            else:
                rows = conn.execute(
                    """
                    SELECT DISTINCT tenant_id
                    FROM jobs
                    WHERE queue_name = %s
                      AND status = %s
                      AND deadline_at IS NOT NULL
                      AND available_at <= %s
                    LIMIT %s
                    """,
                    (
                        queue_name,
                        JobStatus.PENDING.value,
                        now,
                        max(limit * 4, 20),
                    ),
                ).fetchall()
                tenants = [r["tenant_id"] for r in rows]
            for tid in tenants:
                if len(results) >= limit:
                    break
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (tid,),
                )
                if self._is_tenant_paused(conn, tid):
                    continue
                pr = self._try_preempt_for_tenant(
                    conn,
                    tenant_id=tid,
                    queue_name=queue_name,
                    now=now,
                )
                if pr is not None:
                    results.append(pr)
        return results

    @staticmethod
    def _select_expired_terminal_ids(
        conn: Any,
        *,
        status: str,
        now: datetime,
        limit: int,
        tenant_id: str | None,
    ) -> list[UUID]:
        """Pick job ids of *status* older than effective retention.

        Tenant column semantics:
          NULL → inherit global; -1 → never purge; >=0 → seconds.
        """
        if limit < 1:
            return []
        ret_col = (
            "completed_retention_seconds"
            if status == JobStatus.COMPLETED.value
            else "dead_retention_seconds"
        )
        tenant_clause = ""
        params: list[Any] = [status]
        if tenant_id is not None:
            tenant_clause = "AND j.tenant_id = %s"
            params.append(tenant_id)
        params.append(now)
        params.append(limit)
        # effective_secs:
        #   tenant set and >= 0  → tenant value
        #   tenant set and < 0   → never (NULL)
        #   tenant NULL          → global (may be NULL forever)
        rows = conn.execute(
            f"""
            SELECT j.id
            FROM jobs j
            CROSS JOIN queue_settings qs
            LEFT JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
            WHERE j.status = %s
              {tenant_clause}
              AND (
                    CASE
                      WHEN ts.{ret_col} IS NULL THEN qs.{ret_col}
                      WHEN ts.{ret_col} < 0 THEN NULL
                      ELSE ts.{ret_col}
                    END
                  ) IS NOT NULL
              AND COALESCE(j.completed_at, j.updated_at)
                  < (
                        %s - make_interval(
                            secs => (
                                CASE
                                  WHEN ts.{ret_col} IS NULL THEN qs.{ret_col}
                                  WHEN ts.{ret_col} < 0 THEN NULL
                                  ELSE ts.{ret_col}
                                END
                            )
                        )
                    )
            ORDER BY COALESCE(j.completed_at, j.updated_at) ASC
            LIMIT %s
            FOR UPDATE OF j SKIP LOCKED
            """,
            tuple(params),
        ).fetchall()
        return [r["id"] for r in rows]

    def _reap_offboarding_expired_leases(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
    ) -> None:
        """Cancel expired leases for offboarding tenants.

        Offboarding marks tenants paused, so expired leases are never reclaimed.
        Without this sweep, abandoned holders would leave the tenant non-empty
        forever. Cancelling expired leases lets ``_maybe_finalize_offboard``
        purge the tenant once nothing is running.
        """
        if tenant_id is not None:
            if not self._is_tenant_offboarding(conn, tenant_id):
                return
            expired = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND lease_expires_at < %s
                FOR UPDATE
                """,
                (tenant_id, JobStatus.LEASED.value, now),
            ).fetchall()
        else:
            expired = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = %s
                  AND lease_expires_at < %s
                  AND EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = jobs.tenant_id
                          AND ts.offboarding = TRUE
                  )
                FOR UPDATE OF jobs
                """,
                (JobStatus.LEASED.value, now),
            ).fetchall()

        tenants: set[str] = set()
        for row in expired:
            tid = row["tenant_id"]
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'lease expired during tenant offboard'
                    ),
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    row["id"],
                    tid,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                continue
            tenants.add(tid)
            if hasattr(self, "_record_lease_end_usage"):
                self._record_lease_end_usage(conn, row, now=now, cancels=1)
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tid,
                reason=(
                    f"parent job {row['id']} cancelled (lease expired during offboard)"
                ),
                now=now,
            )

        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)
