"""JobQueue spawn_children."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
    SpawnChildrenResult,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    LeaseError,
    QueueDrainError,
    SpawnError,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
)


class SpawnChildrenMixin:
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

        result: SpawnChildrenResult | None = None
        with transaction(self._pool) as conn:
            self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id
            )
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)

            # Idempotent re-entry if already parked for children.
            if row["status"] == JobStatus.AWAITING_CHILDREN.value:
                if row["tenant_id"] != tenant_id:
                    raise SpawnError(f"job {job_id} not found for tenant {tenant_id!r}")
                parent = self._job(row)
                kids = self._list_children_locked(
                    conn, parent_id=job_id, tenant_id=tenant_id
                )
                result = SpawnChildrenResult(parent=parent, children=tuple(kids))

            if result is None:
                self._assert_lease_tenant(row, tenant_id)
                self._assert_active_lease(row, worker_id, lease_token, now)

                parent_queue = row["queue_name"]
                parent_labels = list(row.get("labels") or [])
                parent_skills = list(row.get("required_skills") or [])
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
                        job=self._job(row),
                    )
                self._assert_tenant_mutable(
                    conn, tenant_id, action="spawn", error=SpawnError
                )

                for i, spec in enumerate(specs):
                    qcheck = str(spec.get("queue_name") or parent_queue)
                    if hasattr(self, "_assert_queue_accepts_enqueue"):
                        try:
                            self._assert_queue_accepts_enqueue(
                                conn, tenant_id, qcheck
                            )
                        except QueueDrainError as exc:
                            raise SpawnError(str(exc), job=self._job(row)) from exc
                    self._assert_pending_capacity(conn, tenant_id)
                    # Spawn under parent lease: still consumes enqueue rate so a
                    # fan-out storm cannot bypass tenant throttles.
                    self._consume_enqueue_rate(conn, tenant_id, now=now)
                    if hasattr(self, "_consume_spend_enqueues"):
                        self._consume_spend_enqueues(
                            conn, tenant_id, extra=1, now=now
                        )

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
                        bool(spec["preemptible"]) if "preemptible" in spec else True
                    )
                    # Labels: inherit parent, then merge any child-specific labels.
                    if "labels" in spec:
                        child_labels = _normalize_labels(
                            list(parent_labels) + list(spec.get("labels") or [])
                        )
                    else:
                        child_labels = list(parent_labels)
                    if "required_skills" in spec:
                        child_skills = _normalize_skills(spec.get("required_skills"))
                    else:
                        child_skills = list(parent_skills)
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
                                job=self._job(row),
                            )

                    part_id = (
                        self._compute_partition_id(conn, tenant_id=tenant_id, job_id=cid)
                        if hasattr(self, "_compute_partition_id")
                        else 0
                    )
                    sealed, payload_key_id = self._seal_job_payload(
                        conn, tenant_id=tenant_id, job_id=cid, payload=payload
                    )
                    child_status = (
                        self._enqueue_status_for_skills(conn, child_skills)
                        if hasattr(self, "_enqueue_status_for_skills")
                        else JobStatus.PENDING.value
                    )
                    crow = conn.execute(
                        f"""
                        INSERT INTO jobs (
                            id, tenant_id, queue_name, payload, status, priority,
                            attempts, max_attempts, visibility_timeout_seconds,
                            max_runtime_seconds,
                            retry_backoff_base_seconds, retry_backoff_max_seconds,
                            available_at, dedupe_key, schedule_id, depends_on,
                            parent_job_id, waiting_on, labels, required_skills,
                            deadline_at, estimated_duration_seconds, preemptible,
                            created_at, updated_at, partition_id, payload_key_id,
                            expires_at, expiry_action
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            0, %s, %s,
                            %s,
                            %s, %s,
                            %s, %s, NULL, '{{}}'::uuid[],
                            %s, '{{}}'::uuid[], %s, %s,
                            NULL, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s
                        )
                        RETURNING {_JOB_COLUMNS}
                        """,
                        (
                            cid,
                            tenant_id,
                            qname,
                            Jsonb(sealed),
                            child_status,
                            priority,
                            max_attempts,
                            vt,
                            max_rt,
                            (
                                1
                                if row.get("retry_backoff_base_seconds") is None
                                else int(row["retry_backoff_base_seconds"])
                            ),
                            (
                                300
                                if row.get("retry_backoff_max_seconds") is None
                                else int(row["retry_backoff_max_seconds"])
                            ),
                            now,
                            dedupe_key,
                            job_id,
                            child_labels,
                            child_skills,
                            est,
                            preemptible,
                            now,
                            now,
                            part_id,
                            payload_key_id,
                            row.get("expires_at"),
                            str(row.get("expiry_action") or "dead"),
                        ),
                    ).fetchone()
                    assert crow is not None
                    if hasattr(self, "_recheck_awaiting_skills_row_locked"):
                        crow = self._recheck_awaiting_skills_row_locked(
                            conn, crow, now=now
                        )
                    child = self._job(crow)
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
                    if hasattr(self, "_record_usage"):
                        self._record_usage(
                            conn,
                            tenant_id=tenant_id,
                            now=now,
                            enqueues=1,
                            spawn_children=1,
                        )
                    if hasattr(self, "_stamp_job_quarantine"):
                        self._stamp_job_quarantine(
                            conn,
                            job_id=crow["id"],
                            tenant_id=tenant_id,
                            max_worker_hops=spec.get("max_worker_hops"),
                            quarantine_action=spec.get("quarantine_action"),
                        )
                        stamped = conn.execute(
                            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                            (crow["id"],),
                        ).fetchone()
                        if stamped is not None:
                            crow = stamped
                            child = self._job(crow)
                    child_jobs.append(child)

                if not wait:
                    result = SpawnChildrenResult(
                        parent=self._job(row),
                        children=tuple(child_jobs),
                    )
                else:
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
                        raise LeaseError(
                            f"job {job_id} lease lost during spawn_children"
                        )
                    parent = self._job(updated)
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
                    result = SpawnChildrenResult(
                        parent=self._job(parent_row),
                        children=tuple(child_jobs),
                    )

        assert result is not None
        if hasattr(self, "_rescan_awaiting_skills_after_commit"):
            if any(c.required_skills for c in result.children):
                self._rescan_awaiting_skills_after_commit()
        return result
