"""JobQueue approval timeout processing."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _utcnow,
)


class ApprovalTimeoutsMixin:
    def process_approval_timeouts(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """Auto-reject ``awaiting_approval`` jobs past their deadline.

        Returns the cancelled job rows. Safe to call from a scheduler loop;
        lease paths also reap timeouts opportunistically.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            return self._expire_approval_timeouts_locked(
                conn, now=now, tenant_id=tenant_id, limit=limit
            )

    def _expire_parked_timeouts_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> list[Job]:
        """Reap approval and await-children deadlines (used by the lease path)."""
        expired = self._expire_approval_timeouts_locked(
            conn, now=now, tenant_id=tenant_id, limit=limit
        )
        expired.extend(
            self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id, limit=limit
            )
        )
        return expired

    def _expire_approval_timeouts_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> list[Job]:
        """Cancel awaiting_approval rows past approval_deadline_at."""
        if tenant_id is not None:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND approval_deadline_at IS NOT NULL
                  AND approval_deadline_at < %s
                ORDER BY approval_deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    tenant_id,
                    JobStatus.AWAITING_APPROVAL.value,
                    now,
                    limit,
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = %s
                  AND approval_deadline_at IS NOT NULL
                  AND approval_deadline_at < %s
                ORDER BY approval_deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    JobStatus.AWAITING_APPROVAL.value,
                    now,
                    limit,
                ),
            ).fetchall()

        cancelled: list[Job] = []
        for row in rows:
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    approval_outcome = 'timeout',
                    approval_resolved_at = %s,
                    approval_deadline_at = NULL,
                    last_error = COALESCE(last_error, 'approval timed out'),
                    updated_at = %s
                WHERE id = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    now,
                    row["id"],
                    JobStatus.AWAITING_APPROVAL.value,
                ),
            ).fetchone()
            if updated is None:
                continue
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="approval_timeout",
                from_status=JobStatus.AWAITING_APPROVAL.value,
                actor="system",
                detail={},
                now=now,
            )
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=row["tenant_id"],
                reason=f"parent job {row['id']} approval timed out",
                now=now,
            )
            cancelled.append(self._job(updated))
        return cancelled
