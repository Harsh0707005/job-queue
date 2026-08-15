"""JobQueue enqueue helpers: dedupe and deps."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _DEDUPE_LOCK_CLASS,
    _JOB_COLUMNS,
    DependencyError,
)


class JobsEnqueueHelpersMixin:
    @staticmethod
    def _lock_dedupe_key(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> None:
        """Transaction-scoped lock shared by enqueue and replay for one key."""
        # Two-arg form avoids colliding with tenant concurrency locks.
        # Use unit separators (not NUL — psycopg/Postgres reject 0x00 in text).
        lock_key = f"{tenant_id}\x1f{queue_name}\x1f{dedupe_key}"
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_DEDUPE_LOCK_CLASS, lock_key),
        )

    @staticmethod
    def _normalize_depends_on(
        depends_on: UUID | Sequence[UUID] | None,
    ) -> list[UUID]:
        if depends_on is None:
            return []
        if isinstance(depends_on, UUID):
            return [depends_on]
        ids = list(depends_on)
        for item in ids:
            if not isinstance(item, UUID):
                raise TypeError("depends_on entries must be UUID instances")
        # Preserve order, drop duplicates.
        seen: set[UUID] = set()
        unique: list[UUID] = []
        for item in ids:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return unique

    def _validate_dependencies(
        self,
        conn: Any,
        *,
        tenant_id: str,
        depends_on: list[UUID],
    ) -> None:
        """Ensure parents exist, are runnable, and stay that way until commit.

        Parents are locked ``FOR UPDATE`` (stable id order) so a concurrent
        cancel / dead-letter cannot commit between this check and the child
        insert. Without the lock, cascade would miss the not-yet-visible
        children and leave them pending forever.
        """
        terminal_fail = {
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
            JobStatus.QUARANTINED.value,
        }
        # Lock order must be stable vs other multi-parent enqueue/cancel paths.
        for parent_id in sorted(depends_on, key=lambda u: u.int):
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (parent_id, tenant_id),
            ).fetchone()
            if row is None:
                raise DependencyError(
                    f"dependency {parent_id} not found for tenant {tenant_id!r}"
                )
            if row["status"] in terminal_fail:
                raise DependencyError(
                    f"dependency {parent_id} is already {row['status']}; "
                    "refusing to enqueue a child that can never run"
                )

    @staticmethod
    def _find_active_dedupe(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> dict[str, Any] | None:
        return conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND dedupe_key = %s
              AND status IN (%s, %s, %s, %s, %s, %s, %s)
            LIMIT 1
            """,
            (
                tenant_id,
                queue_name,
                dedupe_key,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
                JobStatus.AWAITING_APPROVAL.value,
                JobStatus.AWAITING_CHILDREN.value,
                JobStatus.AWAITING_SKILLS.value,
                JobStatus.QUARANTINED.value,
                JobStatus.FROZEN.value,
            ),
        ).fetchone()

    def _enqueue_pending_on_conn(
        self,
        conn: Any,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        queue_name: str,
        priority: int,
        max_attempts: int,
        visibility_timeout_seconds: int,
        max_runtime_seconds: int,
        retry_backoff_base_seconds: int,
        retry_backoff_max_seconds: int,
        run_at: datetime,
        dedupe_key: str | None,
        dep_ids: list[UUID],
        label_list: list[str],
        skill_list: list[str],
        deadline: datetime | None,
        estimated_duration_seconds: int | None,
        preemptible: bool,
        now: datetime,
        jid: UUID,
        expires: datetime | None,
        exp_action: str,
        prefer_lw: bool,
        payload_kind: str,
        cloned_from_job_id: UUID | None,
        max_worker_hops: int | None,
        quarantine_action: str | None,
    ) -> Job:
        """Insert one pending job on an open connection.

        Tenant advisory lock must already be held. An active *dedupe_key*
        match is returned as-is (no insert, no rate charge).
        """
        if dedupe_key is not None:
            self._lock_dedupe_key(
                conn,
                tenant_id=tenant_id,
                queue_name=queue_name,
                dedupe_key=dedupe_key,
            )
            existing = self._find_active_dedupe(
                conn,
                tenant_id=tenant_id,
                queue_name=queue_name,
                dedupe_key=dedupe_key,
            )
            if existing is not None:
                return self._job(existing, conn=conn)

        if hasattr(self, "_assert_queue_accepts_enqueue"):
            self._assert_queue_accepts_enqueue(conn, tenant_id, queue_name)
        self._assert_pending_capacity(conn, tenant_id)
        self._consume_enqueue_rate(conn, tenant_id, now=now)
        if hasattr(self, "_consume_spend_enqueues"):
            self._consume_spend_enqueues(conn, tenant_id, extra=1, now=now)

        part_id = self._compute_partition_id(
            conn, tenant_id=tenant_id, job_id=jid
        )
        sealed, payload_key_id = self._seal_job_payload(
            conn, tenant_id=tenant_id, job_id=jid, payload=payload
        )
        status = self._enqueue_status_for_skills(conn, skill_list)
        cloned = (
            str(cloned_from_job_id) if cloned_from_job_id is not None else None
        )
        if dedupe_key is not None:
            row = conn.execute(
                f"""
                INSERT INTO jobs (
                    id, tenant_id, queue_name, payload, status, priority,
                    attempts, max_attempts, visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds, retry_backoff_max_seconds,
                    available_at, dedupe_key, schedule_id, depends_on,
                    labels, required_skills,
                    deadline_at, estimated_duration_seconds, preemptible,
                    created_at, updated_at, partition_id, payload_key_id,
                    expires_at, expiry_action,
                    prefer_last_worker, payload_kind, cloned_from_job_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s,
                    %s, %s,
                    %s, %s, NULL, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (tenant_id, queue_name, dedupe_key)
                    WHERE dedupe_key IS NOT NULL
                      AND status IN (
                          'pending', 'leased',
                          'awaiting_approval', 'awaiting_children',
                          'awaiting_skills', 'quarantined'
                      )
                DO NOTHING
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    jid,
                    tenant_id,
                    queue_name,
                    Jsonb(sealed),
                    status,
                    priority,
                    max_attempts,
                    visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds,
                    retry_backoff_max_seconds,
                    run_at,
                    dedupe_key,
                    dep_ids,
                    label_list,
                    skill_list,
                    deadline,
                    estimated_duration_seconds,
                    bool(preemptible),
                    now,
                    now,
                    part_id,
                    payload_key_id,
                    expires,
                    exp_action,
                    prefer_lw,
                    payload_kind,
                    cloned_from_job_id,
                ),
            ).fetchone()
            if row is None:
                existing = self._find_active_dedupe(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    dedupe_key=dedupe_key,
                )
                if existing is None:
                    raise RuntimeError(
                        f"dedupe race for key {dedupe_key!r}: "
                        "conflict without an active row"
                    )
                return self._job(existing, conn=conn)
        else:
            row = conn.execute(
                f"""
                INSERT INTO jobs (
                    id, tenant_id, queue_name, payload, status, priority,
                    attempts, max_attempts, visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds, retry_backoff_max_seconds,
                    available_at, dedupe_key, schedule_id, depends_on,
                    labels, required_skills,
                    deadline_at, estimated_duration_seconds, preemptible,
                    created_at, updated_at, partition_id, payload_key_id,
                    expires_at, expiry_action,
                    prefer_last_worker, payload_kind, cloned_from_job_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s,
                    %s, %s,
                    %s, NULL, NULL, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    jid,
                    tenant_id,
                    queue_name,
                    Jsonb(sealed),
                    status,
                    priority,
                    max_attempts,
                    visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds,
                    retry_backoff_max_seconds,
                    run_at,
                    dep_ids,
                    label_list,
                    skill_list,
                    deadline,
                    estimated_duration_seconds,
                    bool(preemptible),
                    now,
                    now,
                    part_id,
                    payload_key_id,
                    expires,
                    exp_action,
                    prefer_lw,
                    payload_kind,
                    cloned_from_job_id,
                ),
            ).fetchone()
            assert row is not None

        if hasattr(self, "_recheck_awaiting_skills_row_locked"):
            row = self._recheck_awaiting_skills_row_locked(conn, row, now=now)
        job = self._job(row, conn=conn)
        self._append_job_audit(
            conn,
            job_row=row,
            event_type="enqueued",
            from_status=None,
            actor=None,
            detail={
                "queue_name": queue_name,
                "priority": priority,
                "dedupe_key": dedupe_key,
                "labels": label_list,
                "required_skills": skill_list,
                "status": job.status.value,
                "cloned_from_job_id": cloned,
            },
            now=now,
        )
        if hasattr(self, "_record_usage"):
            self._record_usage(conn, tenant_id=tenant_id, now=now, enqueues=1)
        if hasattr(self, "_stamp_job_quarantine"):
            self._stamp_job_quarantine(
                conn,
                job_id=job.id,
                tenant_id=tenant_id,
                max_worker_hops=max_worker_hops,
                quarantine_action=quarantine_action,
            )
            stamped = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (job.id,),
            ).fetchone()
            if stamped is not None:
                job = self._job(stamped, conn=conn)
        return job
