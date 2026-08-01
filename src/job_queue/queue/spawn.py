"""JobQueue spawn/join domain (SpawnMixin): spawn children and parent join."""
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


class SpawnMixin:
    def spawn_children(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        children: Sequence[dict[str, Any]],
        timeout_seconds: float | int | None = None,
        progress: Any = None,
        wait: bool = True,
    ) -> SpawnChildrenResult:
        """Enqueue subtasks for a leased job; optionally park until they finish.

        Each entry in *children* is a dict with at least ``payload`` (dict).
        Optional per-child keys: ``priority``, ``queue_name``, ``max_attempts``,
        ``visibility_timeout_seconds``, ``max_runtime_seconds``, ``dedupe_key``,
        ``estimated_duration_seconds``, ``preemptible``.

        When *wait* is True (default):
          * children are inserted with ``parent_job_id`` pointing at *job_id*
          * parent is parked as ``awaiting_children`` (lease cleared; concurrency
            slot freed; checkpoint kept / optionally updated via *progress*)
          * when every child ``completed``, parent becomes ``pending`` again
          * if any child ends failed/dead/cancelled or the join times out, the
            parent is cancelled with an error and remaining siblings are stopped

        When *wait* is False, children are enqueued and the parent keeps its
        lease (fire-and-forget spawn).

        Workers that park should raise / catch :class:`ChildrenAwaited` and
        must not ack or nack afterward.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not children:
            raise ValueError("children must be non-empty")
        if timeout_seconds is not None and float(timeout_seconds) <= 0:
            raise ValueError("timeout_seconds must be > 0 when set")

        specs: list[dict[str, Any]] = []
        for i, raw in enumerate(children):
            if not isinstance(raw, dict):
                raise ValueError(f"children[{i}] must be a dict")
            payload = raw.get("payload")
            if not isinstance(payload, dict):
                raise ValueError(f"children[{i}].payload must be a dict")
            specs.append(dict(raw))

        now = _utcnow()
        deadline = (
            now + timedelta(seconds=float(timeout_seconds))
            if timeout_seconds is not None
            else None
        )

        with transaction(self._pool) as conn:
            self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id
            )
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)

            # Idempotent re-entry if already parked for children.
            if row["status"] == JobStatus.AWAITING_CHILDREN.value:
                if row["tenant_id"] != tenant_id:
                    raise SpawnError(
                        f"job {job_id} not found for tenant {tenant_id!r}"
                    )
                parent = _row_to_job(row)
                kids = self._list_children_locked(
                    conn, parent_id=job_id, tenant_id=tenant_id
                )
                return SpawnChildrenResult(parent=parent, children=tuple(kids))

            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            parent_queue = row["queue_name"]
            parent_labels = list(row.get("labels") or [])
            child_jobs: list[Job] = []
            child_ids: list[UUID] = []

            # Serialize pending-cap with other enqueues for this tenant.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise SpawnError(
                    f"tenant {tenant_id!r} is offboarding; spawn rejected",
                    job=_row_to_job(row),
                )

            for i, spec in enumerate(specs):
                self._assert_pending_capacity(conn, tenant_id)
                # Spawn under parent lease: still consumes enqueue rate so a
                # fan-out storm cannot bypass tenant throttles.
                self._consume_enqueue_rate(conn, tenant_id, now=now)

                payload = spec["payload"]
                qname = str(spec.get("queue_name") or parent_queue)
                priority = int(spec.get("priority") or row["priority"])
                max_attempts = int(spec.get("max_attempts") or row["max_attempts"])
                vt = int(
                    spec.get("visibility_timeout_seconds")
                    or row["visibility_timeout_seconds"]
                )
                max_rt = int(
                    spec.get("max_runtime_seconds") or row["max_runtime_seconds"]
                )
                dedupe_key = spec.get("dedupe_key")
                if dedupe_key is not None and dedupe_key == "":
                    raise ValueError(
                        f"children[{i}].dedupe_key must be non-empty when set"
                    )
                est = spec.get("estimated_duration_seconds")
                if est is not None:
                    est = int(est)
                    if est < 1:
                        raise ValueError(
                            f"children[{i}].estimated_duration_seconds must be >= 1"
                        )
                preemptible = (
                    bool(spec["preemptible"])
                    if "preemptible" in spec
                    else True
                )
                # Labels: inherit parent, then merge any child-specific labels.
                if "labels" in spec:
                    child_labels = _normalize_labels(
                        list(parent_labels) + list(spec.get("labels") or [])
                    )
                else:
                    child_labels = list(parent_labels)
                cid = uuid4()
                if dedupe_key is not None:
                    self._lock_dedupe_key(
                        conn,
                        tenant_id=tenant_id,
                        queue_name=qname,
                        dedupe_key=str(dedupe_key),
                    )
                    existing = self._find_active_dedupe(
                        conn,
                        tenant_id=tenant_id,
                        queue_name=qname,
                        dedupe_key=str(dedupe_key),
                    )
                    if existing is not None:
                        raise SpawnError(
                            f"children[{i}] dedupe_key {dedupe_key!r} already active",
                            job=_row_to_job(row),
                        )

                crow = conn.execute(
                    f"""
                    INSERT INTO jobs (
                        id, tenant_id, queue_name, payload, status, priority,
                        attempts, max_attempts, visibility_timeout_seconds,
                        max_runtime_seconds,
                        retry_backoff_base_seconds, retry_backoff_max_seconds,
                        available_at, dedupe_key, schedule_id, depends_on,
                        parent_job_id, waiting_on, labels,
                        deadline_at, estimated_duration_seconds, preemptible,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, NULL, '{{}}'::uuid[],
                        %s, '{{}}'::uuid[], %s,
                        NULL, %s, %s,
                        %s, %s
                    )
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        cid,
                        tenant_id,
                        qname,
                        Jsonb(payload),
                        JobStatus.PENDING.value,
                        priority,
                        max_attempts,
                        vt,
                        max_rt,
                        int(row.get("retry_backoff_base_seconds") or 1),
                        int(row.get("retry_backoff_max_seconds") or 300),
                        now,
                        dedupe_key,
                        job_id,
                        child_labels,
                        est,
                        preemptible,
                        now,
                        now,
                    ),
                ).fetchone()
                assert crow is not None
                child = _row_to_job(crow)
                child_jobs.append(child)
                child_ids.append(child.id)
                self._append_job_audit(
                    conn,
                    job_row=crow,
                    event_type="enqueued",
                    from_status=None,
                    actor=worker_id,
                    detail={
                        "parent_job_id": str(job_id),
                        "spawned": True,
                        "queue_name": qname,
                    },
                    now=now,
                )

            if not wait:
                return SpawnChildrenResult(
                    parent=_row_to_job(row),
                    children=tuple(child_jobs),
                )

            # Park parent: free lease / concurrency, keep checkpoint.
            cp_sql = ""
            cp_params: list[Any] = []
            if progress is not None:
                # Merge child ids into checkpoint for resume handlers.
                progress_out = progress
                if isinstance(progress, dict):
                    progress_out = {
                        **progress,
                        "waiting_on": [str(x) for x in child_ids],
                        "phase": "awaiting_children",
                    }
                cp_sql = ", checkpoint = %s, checkpointed_at = %s"
                cp_params = [Jsonb(progress_out), now]
            else:
                # Still stamp a minimal join checkpoint if none provided.
                existing_cp = row.get("checkpoint")
                if isinstance(existing_cp, dict):
                    progress_out = {
                        **existing_cp,
                        "waiting_on": [str(x) for x in child_ids],
                        "phase": "awaiting_children",
                    }
                else:
                    progress_out = {
                        "waiting_on": [str(x) for x in child_ids],
                        "phase": "awaiting_children",
                    }
                cp_sql = ", checkpoint = %s, checkpointed_at = %s"
                cp_params = [Jsonb(progress_out), now]

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    waiting_on = %s,
                    await_children_started_at = %s,
                    await_children_deadline_at = %s,
                    await_children_outcome = NULL,
                    await_children_resolved_at = NULL,
                    last_error = NULL,
                    updated_at = %s
                    {cp_sql}
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.AWAITING_CHILDREN.value,
                    child_ids,
                    now,
                    deadline,
                    now,
                    *cp_params,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during spawn_children")
            parent = _row_to_job(updated)
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="children_awaited",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={
                    "child_ids": [str(x) for x in child_ids],
                    "timeout_seconds": (
                        float(timeout_seconds)
                        if timeout_seconds is not None
                        else None
                    ),
                },
                now=now,
            )
            # Children may already be terminal if something weird happens; wake.
            self._maybe_wake_parent_on_join_locked(
                conn, parent_id=job_id, tenant_id=tenant_id, now=now
            )
            parent_row = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (job_id,),
            ).fetchone()
            assert parent_row is not None
            return SpawnChildrenResult(
                parent=_row_to_job(parent_row),
                children=tuple(child_jobs),
            )

    def list_children(
        self,
        parent_id: UUID,
        *,
        tenant_id: str,
        limit: int = 500,
    ) -> list[Job]:
        """List jobs spawned with ``parent_job_id = parent_id`` for *tenant_id*."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with transaction(self._pool) as conn:
            # Authorize: parent must belong to tenant (or any child).
            parent = conn.execute(
                "SELECT id FROM jobs WHERE id = %s AND tenant_id = %s",
                (parent_id, tenant_id),
            ).fetchone()
            if parent is None:
                # Still allow if children exist under tenant (parent deleted).
                kids = self._list_children_locked(
                    conn, parent_id=parent_id, tenant_id=tenant_id, limit=limit
                )
                if not kids:
                    raise ValueError(
                        f"job {parent_id} not found for tenant {tenant_id!r}"
                    )
                return kids
            return self._list_children_locked(
                conn, parent_id=parent_id, tenant_id=tenant_id, limit=limit
            )

    def process_await_children_timeouts(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """Cancel parents parked past ``await_children_deadline_at``.

        Safe to call from a scheduler loop. The normal ``lease`` /
        ``lease_batch`` path also reaps these opportunistically (same as
        approval timeouts), so an idle fleet that still leases work will
        expire timed-out joins without a dedicated ticker.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            return self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id, limit=limit
            )

    def list_awaiting_children(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List parents parked waiting for subtasks (deadline ascending)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        now = _utcnow()
        with transaction(self._pool) as conn:
            self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id
            )
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                ORDER BY await_children_deadline_at ASC NULLS LAST, created_at ASC
                LIMIT %s OFFSET %s
                """,
                (
                    tenant_id,
                    JobStatus.AWAITING_CHILDREN.value,
                    limit,
                    offset,
                ),
            ).fetchall()
            return [_row_to_job(r) for r in rows]

    def _list_children_locked(
        self,
        conn: Any,
        *,
        parent_id: UUID,
        tenant_id: str,
        limit: int = 500,
    ) -> list[Job]:
        rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND parent_job_id = %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (tenant_id, parent_id, limit),
        ).fetchall()
        return [_row_to_job(r) for r in rows]

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
