"""JobQueue spawn join internals: wake/resume/fail parent, expire await-children."""
from __future__ import annotations

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


class SpawnJoinMixin:
    def _maybe_wake_parent_on_join_locked(
        self,
        conn: Any,
        *,
        parent_id: UUID,
        tenant_id: str,
        now: datetime,
    ) -> None:
        """If *parent_id* is awaiting children, resume or fail based on kids.

        * All waiting_on completed → parent ``pending`` (join success).
        * Any waiting_on failed/dead/cancelled → parent cancelled with error;
          remaining non-terminal siblings cascade-cancelled.
        * Otherwise still waiting (no-op).
        """
        parent = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (parent_id, tenant_id),
        ).fetchone()
        if parent is None:
            return
        if parent["status"] != JobStatus.AWAITING_CHILDREN.value:
            return

        waiting = list(parent.get("waiting_on") or [])
        if not waiting:
            # Nothing to wait on — treat as joined so parent is not stuck.
            self._resume_parent_join_locked(
                conn, parent=parent, now=now, child_ids=[]
            )
            return

        rows = conn.execute(
            f"""
            SELECT id, status, last_error
            FROM jobs
            WHERE tenant_id = %s
              AND id = ANY(%s)
            """,
            (tenant_id, waiting),
        ).fetchall()
        by_id = {r["id"]: r for r in rows}

        # Missing child rows count as failure (cannot complete join).
        terminal_fail = {
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
        }
        activeish = {
            JobStatus.PENDING.value,
            JobStatus.LEASED.value,
            JobStatus.AWAITING_APPROVAL.value,
            JobStatus.AWAITING_CHILDREN.value,
        }

        failed: list[dict[str, Any]] = []
        still_running = False
        completed = 0
        for cid in waiting:
            row = by_id.get(cid)
            if row is None:
                failed.append(
                    {
                        "id": cid,
                        "status": "missing",
                        "last_error": "child row missing",
                    }
                )
                continue
            st = row["status"]
            if st == JobStatus.COMPLETED.value:
                completed += 1
            elif st in terminal_fail:
                failed.append(row)
            elif st in activeish:
                still_running = True
            else:
                # Unexpected status — treat as failure to avoid hang.
                failed.append(row)

        if failed:
            first = failed[0]
            err = (
                f"child job {first['id']} ended as {first['status']}"
                + (
                    f": {first['last_error']}"
                    if first.get("last_error")
                    else ""
                )
            )
            self._fail_parent_join_locked(
                conn,
                parent=parent,
                now=now,
                reason=err,
                outcome="failed",
            )
            return

        if still_running or completed < len(waiting):
            return

        # All children completed.
        self._resume_parent_join_locked(
            conn, parent=parent, now=now, child_ids=waiting
        )

    def _resume_parent_join_locked(
        self,
        conn: Any,
        *,
        parent: dict[str, Any],
        now: datetime,
        child_ids: list[UUID],
    ) -> None:
        parent_id = parent["id"]
        cp = parent.get("checkpoint")
        if isinstance(cp, dict):
            new_cp = {
                **cp,
                "phase": "children_joined",
                "waiting_on": [str(x) for x in child_ids],
            }
        else:
            new_cp = {
                "phase": "children_joined",
                "waiting_on": [str(x) for x in child_ids],
            }
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                available_at = %s,
                await_children_outcome = 'joined',
                await_children_resolved_at = %s,
                await_children_deadline_at = NULL,
                checkpoint = %s,
                checkpointed_at = %s,
                last_error = NULL,
                updated_at = %s
            WHERE id = %s
              AND status = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.PENDING.value,
                now,
                now,
                Jsonb(new_cp),
                now,
                now,
                parent_id,
                JobStatus.AWAITING_CHILDREN.value,
            ),
        ).fetchone()
        if updated is None:
            return
        self._append_job_audit(
            conn,
            job_row=updated,
            event_type="children_joined",
            from_status=JobStatus.AWAITING_CHILDREN.value,
            actor="system",
            detail={"child_ids": [str(x) for x in child_ids]},
            now=now,
        )

    def _fail_parent_join_locked(
        self,
        conn: Any,
        *,
        parent: dict[str, Any],
        now: datetime,
        reason: str,
        outcome: str = "failed",
    ) -> None:
        parent_id = parent["id"]
        tenant_id = parent["tenant_id"]
        err = (reason or "join failed")[:2000]
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                await_children_outcome = %s,
                await_children_resolved_at = %s,
                await_children_deadline_at = NULL,
                last_error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s
              AND status = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.CANCELLED.value,
                outcome,
                now,
                err,
                now,
                now,
                parent_id,
                JobStatus.AWAITING_CHILDREN.value,
            ),
        ).fetchone()
        if updated is None:
            return
        self._append_job_audit(
            conn,
            job_row=updated,
            event_type="children_join_failed",
            from_status=JobStatus.AWAITING_CHILDREN.value,
            actor="system",
            detail={"reason": err, "outcome": outcome},
            now=now,
        )
        # Stop remaining siblings / dependents.
        self._cascade_cancel_dependents(
            conn,
            parent_id=parent_id,
            tenant_id=tenant_id,
            reason=f"parent job {parent_id} join failed: {err}",
            now=now,
        )

    def _expire_await_children_timeouts_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """Cancel awaiting_children rows past await_children_deadline_at."""
        if tenant_id:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = %s
                  AND tenant_id = %s
                  AND await_children_deadline_at IS NOT NULL
                  AND await_children_deadline_at <= %s
                ORDER BY await_children_deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    JobStatus.AWAITING_CHILDREN.value,
                    tenant_id,
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
                  AND await_children_deadline_at IS NOT NULL
                  AND await_children_deadline_at <= %s
                ORDER BY await_children_deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    JobStatus.AWAITING_CHILDREN.value,
                    now,
                    limit,
                ),
            ).fetchall()
        out: list[Job] = []
        for row in rows:
            self._fail_parent_join_locked(
                conn,
                parent=row,
                now=now,
                reason="await children timed out",
                outcome="timeout",
            )
            refreshed = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (row["id"],),
            ).fetchone()
            if refreshed is not None:
                out.append(_row_to_job(refreshed))
        return out
