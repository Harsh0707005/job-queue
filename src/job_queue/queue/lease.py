"""JobQueue lease domain (LeaseMixin)."""
from __future__ import annotations

import json

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import DeadlockDetected, UniqueViolation
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

class LeaseMixin:
    def set_lease_rate_limit(
        self,
        tenant_id: str,
        per_second: float | None,
        *,
        burst: int | None = None,
    ) -> None:
        """Cap how fast *tenant_id* jobs may be claimed (token bucket).

        Applies to new pending claims (scoped and fair multi-tenant). Expired
        lease reclaim is not rate-limited. Fair lease skips a rate-limited
        tenant and rotates LRS so others keep getting work. Pass ``None`` for
        *per_second* to clear.
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
                    SET lease_rate_per_sec = NULL,
                        lease_burst = NULL,
                        lease_tokens = NULL,
                        lease_tokens_at = NULL,
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
                    lease_rate_per_sec, lease_burst,
                    lease_tokens, lease_tokens_at,
                    updated_at
                )
                VALUES (
                    %s, NULL, NULL, FALSE, FALSE,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET lease_rate_per_sec = EXCLUDED.lease_rate_per_sec,
                    lease_burst = EXCLUDED.lease_burst,
                    lease_tokens = EXCLUDED.lease_tokens,
                    lease_tokens_at = EXCLUDED.lease_tokens_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, rate, burst_n, float(burst_n), now, now),
            )


    def get_lease_rate_limit(
        self, tenant_id: str
    ) -> tuple[float | None, int | None]:
        """Return ``(per_second, burst)`` or ``(None, None)`` if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT lease_rate_per_sec, lease_burst
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None or row["lease_rate_per_sec"] is None:
                return None, None
            return float(row["lease_rate_per_sec"]), (
                int(row["lease_burst"])
                if row["lease_burst"] is not None
                else None
            )


    def lease(
        self,
        *,
        worker_id: str,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> Job | None:
        """Claim the next available job (batch size 1).

        See :meth:`lease_batch` for multi-job pulls and full semantics.
        """
        jobs = self.lease_batch(
            worker_id=worker_id,
            limit=1,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        return jobs[0] if jobs else None


    def lease_batch(
        self,
        *,
        worker_id: str,
        limit: int = 1,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> list[Job]:
        """Claim up to *limit* available jobs in one pull.

        Pass *tenant_id* to lease only that tenant's work. Omit *tenant_id* for
        **fair multi-tenant** leasing with **priority lanes**: higher priority
        first; within a lane tenants rotate by least-recently-served so one
        noisy tenant cannot monopolize the batch.

        Available means:
          * status = pending AND available_at <= now() AND all ``depends_on``
            parents are ``completed``, OR
          * status = leased AND lease_expires_at < now()  (orphaned lease)
            AND attempts < max_attempts

        Delayed pending jobs, unmet dependencies, and per-tenant concurrency
        caps are always enforced. Paused tenants yield no new leases (in-flight
        work may still complete). Expired leases that already used all
        attempts are dead-lettered first (dependents cascade-cancelled).

        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never claim the
        same row. Each job gets a fresh ``lease_token`` for fencing. Returns
        fewer than *limit* jobs when the queue is short (possibly empty list).
        """
        if not worker_id:
            raise ValueError("worker_id is required")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if limit > 500:
            raise ValueError("limit must be <= 500")

        # Fair multi-tenant leasing takes per-tenant advisory locks; under
        # high concurrency lock order can deadlock. Retry instead of 500ing
        # the whole batch so workers keep draining.
        last_exc: BaseException | None = None
        for _attempt in range(5):
            try:
                if tenant_id is None:
                    return self._lease_fair_batch(
                        worker_id=worker_id,
                        queue_name=queue_name,
                        visibility_timeout_seconds=visibility_timeout_seconds,
                        limit=limit,
                    )
                return self._lease_for_tenant_batch(
                    tenant_id=tenant_id,
                    worker_id=worker_id,
                    queue_name=queue_name,
                    visibility_timeout_seconds=visibility_timeout_seconds,
                    limit=limit,
                )
            except DeadlockDetected as exc:
                last_exc = exc
                continue
        assert last_exc is not None
        raise last_exc


    def _lease_for_tenant_batch(
        self,
        *,
        tenant_id: str,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        limit: int,
    ) -> list[Job]:
        now = _utcnow()
        claimed: list[Job] = []

        with transaction(self._pool) as conn:
            # Serialize lease decisions for this tenant so concurrency caps
            # cannot be exceeded by racing workers.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )

            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=tenant_id)
            # Offboarding: cancel abandoned expired leases so the tenant can empty.
            self._reap_offboarding_expired_leases(
                conn, now=now, tenant_id=tenant_id
            )
            # Parked-job deadlines (approval + await-children join).
            self._expire_parked_timeouts_locked(
                conn, now=now, tenant_id=tenant_id
            )

            # Paused: no new claims (pending or reclaim). In-flight holders
            # still use heartbeat/ack/nack outside this path.
            if self._is_tenant_paused(conn, tenant_id):
                return []

            conc_limit = self._read_concurrency_limit(conn, tenant_id)
            leased_count = self._count_leased(conn, tenant_id)

            # Deadline preemption: free a slot when an urgent pending job
            # cannot wait for less-urgent holders to finish.
            if conc_limit is not None and leased_count >= conc_limit:
                if self._try_preempt_for_tenant(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    now=now,
                ):
                    leased_count = self._count_leased(conn, tenant_id)

            if conc_limit is not None and leased_count >= conc_limit:
                # Only reclaim expired leases (does not increase concurrent
                # holders beyond the cap in steady state).
                candidates = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      AND status = %s
                      AND lease_expires_at < %s
                      AND attempts < max_attempts
                    ORDER BY priority DESC,
                      (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                        - COALESCE(estimated_duration_seconds, 60)) ASC,
                      created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.LEASED.value,
                        now,
                        now,
                        now,
                        limit,
                    ),
                ).fetchall()
            else:
                take = limit
                if conc_limit is not None:
                    take = min(limit, conc_limit - leased_count)
                    take = max(take, 0)
                if take < 1:
                    return []
                candidates = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      AND (
                            (
                                status = %s
                                AND available_at <= %s
                                AND {_DEPS_SATISFIED_SQL}
                            )
                         OR (
                                status = %s
                            AND lease_expires_at < %s
                            AND attempts < max_attempts
                         )
                      )
                    ORDER BY priority DESC,
                      CASE WHEN deadline_at IS NULL THEN 1 ELSE 0 END ASC,
                      (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                        - COALESCE(estimated_duration_seconds, 60)) ASC,
                      created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.PENDING.value,
                        now,
                        JobStatus.LEASED.value,
                        now,
                        now,
                        now,
                        take,
                    ),
                ).fetchall()

            for candidate in candidates:
                # Rate-limit new pending claims only; reclaim is free so
                # recovery is not blocked by a noisy tenant's throttle.
                if candidate["status"] == JobStatus.PENDING.value:
                    if not self._try_consume_lease_rate(
                        conn, tenant_id, now=now
                    ):
                        break
                claimed.append(
                    self._claim_locked_job(
                        conn,
                        candidate,
                        worker_id=worker_id,
                        token=uuid4(),
                        visibility_timeout_seconds=visibility_timeout_seconds,
                        now=now,
                    )
                )
        return claimed


    def _lease_fair_batch(
        self,
        *,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        limit: int,
        _max_skips: int | None = None,
    ) -> list[Job]:
        """Lease up to *limit* jobs fairly across tenants for *queue_name*.

        Each claim updates ``tenant_fairness.last_lease_at`` so subsequent
        jobs in the same batch rotate tenants within a priority lane.
        """
        now = _utcnow()
        claimed: list[Job] = []
        # Allow several skip attempts per desired job (paused / at-cap tenants).
        max_skips = _max_skips if _max_skips is not None else max(64, limit * 16)
        skips = 0
        # Tenants that hit lease rate limit this batch are excluded so a
        # high-priority noisy tenant cannot burn the skip budget.
        rate_limited_tenants: set[str] = set()

        with transaction(self._pool) as conn:
            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=None)
            self._reap_offboarding_expired_leases(conn, now=now, tenant_id=None)
            # Parked-job deadlines (approval + await-children join) for all tenants.
            self._expire_parked_timeouts_locked(conn, now=now, tenant_id=None)
            # Free slots for deadline-critical pending work before claiming.
            self._preempt_blocked_urgent(
                conn, queue_name=queue_name, now=now, limit=max(4, limit)
            )

            while len(claimed) < limit and skips < max_skips:
                try:
                    # Savepoint so a rejected candidate releases its row lock.
                    with conn.transaction():
                        job = self._try_claim_fair_candidate(
                            conn,
                            worker_id=worker_id,
                            queue_name=queue_name,
                            visibility_timeout_seconds=visibility_timeout_seconds,
                            now=now,
                            exclude_tenants=rate_limited_tenants,
                        )
                        if job is None:
                            break
                        claimed.append(job)
                except _SkipLeaseCandidate as skip:
                    skips += 1
                    if (
                        skip.tenant_id
                        and skip.reason == "tenant lease rate limited"
                    ):
                        rate_limited_tenants.add(skip.tenant_id)
                    continue
        return claimed


    def _try_claim_fair_candidate(
        self,
        conn: Any,
        *,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        now: datetime,
        exclude_tenants: set[str] | None = None,
    ) -> Job | None:
        token = uuid4()
        # Priority lanes first (higher priority always before lower), then
        # least-recently-served tenant within that lane (never-served =
        # -infinity). A noisy tenant's high-priority flood still rotates with
        # other tenants at the same priority; it cannot leapfrog higher-priority
        # work of quieter tenants, and low-priority noise cannot starve high.
        excluded = sorted(exclude_tenants or ())
        exclude_sql = ""
        params: list[Any] = [queue_name]
        if excluded:
            exclude_sql = "AND jobs.tenant_id <> ALL(%s)"
            params.append(excluded)
        params.extend(
            [
                JobStatus.PENDING.value,
                now,
                JobStatus.LEASED.value,
                JobStatus.LEASED.value,
                now,
                now,
                now,
            ]
        )
        candidate = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE queue_name = %s
              {exclude_sql}
              AND NOT EXISTS (
                    SELECT 1
                    FROM tenant_settings ts
                    WHERE ts.tenant_id = jobs.tenant_id
                      AND ts.paused = TRUE
              )
              AND (
                    (
                        status = %s
                        AND available_at <= %s
                        AND {_DEPS_SATISFIED_SQL}
                        AND (
                            NOT EXISTS (
                                SELECT 1
                                FROM tenant_settings ts
                                WHERE ts.tenant_id = jobs.tenant_id
                                  AND ts.max_concurrent_leases IS NOT NULL
                            )
                            OR (
                                SELECT COUNT(*)::int
                                FROM jobs j2
                                WHERE j2.tenant_id = jobs.tenant_id
                                  AND j2.status = %s
                            ) < (
                                SELECT ts.max_concurrent_leases
                                FROM tenant_settings ts
                                WHERE ts.tenant_id = jobs.tenant_id
                            )
                        )
                    )
                 OR (
                        status = %s
                    AND lease_expires_at < %s
                    AND attempts < max_attempts
                 )
              )
            ORDER BY
              priority DESC,
              CASE WHEN deadline_at IS NULL THEN 1 ELSE 0 END ASC,
              (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                - COALESCE(estimated_duration_seconds, 60)) ASC,
              COALESCE(
                (
                    SELECT tf.last_lease_at
                    FROM tenant_fairness tf
                    WHERE tf.tenant_id = jobs.tenant_id
                ),
                TIMESTAMPTZ '-infinity'
              ) ASC,
              created_at ASC
            FOR UPDATE OF jobs SKIP LOCKED
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()

        if candidate is None:
            return None

        # Re-check pause + concurrency under tenant lock (closes races).
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (candidate["tenant_id"],),
        )
        if self._is_tenant_paused(conn, candidate["tenant_id"]):
            raise _SkipLeaseCandidate(
                "tenant paused", tenant_id=candidate["tenant_id"]
            )
        if candidate["status"] == JobStatus.PENDING.value:
            limit = self._read_concurrency_limit(conn, candidate["tenant_id"])
            leased_count = self._count_leased(conn, candidate["tenant_id"])
            if limit is not None and leased_count >= limit:
                # Try deadline preemption once under the tenant lock.
                if self._try_preempt_for_tenant(
                    conn,
                    tenant_id=candidate["tenant_id"],
                    queue_name=queue_name,
                    now=now,
                    urgent_hint=candidate,
                ):
                    leased_count = self._count_leased(
                        conn, candidate["tenant_id"]
                    )
                if leased_count >= limit:
                    raise _SkipLeaseCandidate(
                        "tenant at concurrency limit",
                        tenant_id=candidate["tenant_id"],
                    )
            # Token-bucket lease rate: skip + rotate LRS so other tenants
            # keep getting work while this tenant waits for tokens.
            if not self._try_consume_lease_rate(
                conn, candidate["tenant_id"], now=now
            ):
                conn.execute(
                    """
                    INSERT INTO tenant_fairness (tenant_id, last_lease_at)
                    VALUES (%s, clock_timestamp())
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET last_lease_at = clock_timestamp()
                    """,
                    (candidate["tenant_id"],),
                )
                raise _SkipLeaseCandidate(
                    "tenant lease rate limited",
                    tenant_id=candidate["tenant_id"],
                )

        claimed = self._claim_locked_job(
            conn,
            candidate,
            worker_id=worker_id,
            token=token,
            visibility_timeout_seconds=visibility_timeout_seconds,
            now=now,
        )
        # Record that this tenant was just served (drives next fair pick).
        # Use clock_timestamp() so successive claims in the same batch get
        # distinct last_lease_at values and rotate tenants correctly.
        conn.execute(
            """
            INSERT INTO tenant_fairness (tenant_id, last_lease_at)
            VALUES (%s, clock_timestamp())
            ON CONFLICT (tenant_id) DO UPDATE
            SET last_lease_at = clock_timestamp()
            """,
            (claimed.tenant_id,),
        )
        return claimed


    def _claim_locked_job(
        self,
        conn: Any,
        candidate: dict[str, Any],
        *,
        worker_id: str,
        token: UUID,
        visibility_timeout_seconds: int | None,
        now: datetime,
    ) -> Job:
        new_attempts = candidate["attempts"] + 1
        vt = visibility_timeout_seconds or candidate["visibility_timeout_seconds"]
        max_rt = int(candidate.get("max_runtime_seconds") or 3600)
        lease_expires = now + timedelta(seconds=vt)
        hard = now + timedelta(seconds=max_rt)
        if lease_expires > hard:
            lease_expires = hard

        from_status = candidate["status"]
        row = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = %s,
                lease_token = %s,
                lease_tenant_id = %s,
                lease_expires_at = %s,
                lease_started_at = %s,
                attempts = %s,
                visibility_timeout_seconds = %s,
                updated_at = %s,
                last_error = NULL
            WHERE id = %s
              AND tenant_id = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.LEASED.value,
                worker_id,
                token,
                candidate["tenant_id"],
                lease_expires,
                now,
                new_attempts,
                vt,
                now,
                candidate["id"],
                candidate["tenant_id"],
            ),
        ).fetchone()
        assert row is not None
        job = _row_to_job(row)
        self._append_job_audit(
            conn,
            job_row=row,
            event_type="leased",
            from_status=from_status,
            actor=worker_id,
            detail={
                "attempts": new_attempts,
                "reclaim": from_status == JobStatus.LEASED.value,
            },
            now=now,
        )
        return job


    def _hard_deadline(self, row: dict[str, Any]) -> datetime | None:
        started = row.get("lease_started_at")
        if started is None:
            return None
        max_rt = row.get("max_runtime_seconds")
        if max_rt is None:
            max_rt = 3600
        return _as_utc(started) + timedelta(seconds=int(max_rt))


    def _release_max_runtime_exceeded(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        """Force-release a stuck lease that exceeded max_runtime_seconds."""
        attempts = int(row["attempts"])
        max_attempts = int(row["max_attempts"])
        error = "max runtime exceeded"
        tenant_id = row["tenant_id"]
        # Offboarding tenants must not regain pending work after a force-release.
        force_terminal = self._is_tenant_offboarding(conn, tenant_id)
        if attempts >= max_attempts or force_terminal:
            terminal = (
                JobStatus.CANCELLED.value
                if force_terminal and attempts < max_attempts
                else JobStatus.DEAD.value
            )
            if force_terminal and attempts < max_attempts:
                error = "max runtime exceeded during tenant offboard"
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    completed_at = CASE WHEN %s = %s THEN %s ELSE completed_at END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    terminal,
                    error,
                    terminal,
                    JobStatus.DEAD.value,
                    now,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type=(
                            "cancelled"
                            if terminal == JobStatus.CANCELLED.value
                            else "dead_lettered"
                        ),
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
                    )
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tenant_id,
                reason=f"parent job {row['id']} ended as {terminal} (max runtime)",
                now=now,
            )
            dead_row = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (row["id"],),
            ).fetchone()
            if dead_row is not None and terminal == JobStatus.DEAD.value:
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(dead_row),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
            self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
        else:
            base = row.get("retry_backoff_base_seconds")
            cap = row.get("retry_backoff_max_seconds")
            backoff = compute_retry_backoff_seconds(
                attempts,
                base_seconds=1 if base is None else int(base),
                max_seconds=300 if cap is None else int(cap),
            )
            next_available = now + timedelta(seconds=backoff)
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    available_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    JobStatus.PENDING.value,
                    error,
                    next_available,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type="requeued",
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
                    )


    def _dead_letter_exhausted_leases(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None,
    ) -> None:
        """Mark expired leases that exhausted attempts as dead; cascade children."""
        if tenant_id is not None:
            dead_rows = conn.execute(
                """
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
                    updated_at = %s
                WHERE tenant_id = %s
                  AND status = %s
                  AND lease_expires_at < %s
                  AND attempts >= max_attempts
                RETURNING id, tenant_id
                """,
                (
                    JobStatus.DEAD.value,
                    now,
                    tenant_id,
                    JobStatus.LEASED.value,
                    now,
                ),
            ).fetchall()
        else:
            dead_rows = conn.execute(
                """
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
                    updated_at = %s
                WHERE status = %s
                  AND lease_expires_at < %s
                  AND attempts >= max_attempts
                RETURNING id, tenant_id
                """,
                (
                    JobStatus.DEAD.value,
                    now,
                    JobStatus.LEASED.value,
                    now,
                ),
            ).fetchall()

        tenants: set[str] = set()
        for dead in dead_rows:
            tid = dead["tenant_id"]
            tenants.add(tid)
            full = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (dead["id"],),
            ).fetchone()
            if full is not None:
                self._append_job_audit(
                    conn,
                    job_row=full,
                    event_type="dead_lettered",
                    from_status=JobStatus.LEASED.value,
                    actor="system",
                    detail={
                        "reason": "exceeded max_attempts after lease expiry"
                    },
                    now=now,
                )
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(full),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
            else:
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)


    def heartbeat(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        extend_by_seconds: int | None = None,
    ) -> Job:
        """Extend the lease held by *worker_id* under *lease_token*.

        Requires matching worker, fencing token, and tenant. Tenant matches
        when *tenant_id* equals the job's current owner **or**
        ``lease_tenant_id`` (frozen at claim; allows mid-flight merge). An
        unrelated tenant is rejected even with a valid token. Raises
        ``LeaseError`` if the lease was reclaimed, expired, completed, or the
        hard ``max_runtime_seconds`` deadline has been exceeded (the job is
        force-released so a stuck worker cannot heartbeat forever).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_lease_holder(row, worker_id, lease_token)
            hard = self._hard_deadline(row)
            if hard is not None and now >= hard:
                # Commit force-release, then signal the worker (do not raise
                # inside the transaction or the release would roll back).
                self._release_max_runtime_exceeded(conn, row, now=now)
                runtime_exceeded = True
            else:
                runtime_exceeded = False
                self._assert_lease_not_expired(row, now)

                extend = extend_by_seconds or row["visibility_timeout_seconds"]
                new_expiry = now + timedelta(seconds=extend)
                if hard is not None and new_expiry > hard:
                    new_expiry = hard

                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET lease_expires_at = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        new_expiry,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise LeaseError(f"job {job_id} lease lost during heartbeat")
                return _row_to_job(updated)

        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded "
                f"(limit={row.get('max_runtime_seconds')}s)"
            )
        raise LeaseError(f"job {job_id} lease lost during heartbeat")


    def ack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        result: Any = None,
    ) -> Job:
        """Mark a leased job completed. Only the current lease holder may ack.

        Optional *result* is stored as JSON on the job and can be read later
        via ``get`` / ``get_result``. *tenant_id* must match the current owner
        or ``lease_tenant_id`` (pre-merge claim tenant).
        """
        now = _utcnow()
        result_json = Jsonb(result) if result is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_lease_holder(row, worker_id, lease_token)
            hard = self._hard_deadline(row)
            if hard is not None and now >= hard:
                self._release_max_runtime_exceeded(conn, row, now=now)
                runtime_exceeded = True
            else:
                runtime_exceeded = False
                self._assert_lease_not_expired(row, now)

                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        completed_at = %s,
                        result = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.COMPLETED.value,
                        now,
                        result_json,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise LeaseError(f"job {job_id} lease lost during ack")
                job = _row_to_job(updated)
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="completed",
                    from_status=JobStatus.LEASED.value,
                    actor=worker_id,
                    detail={"has_result": result is not None},
                    now=now,
                )
                self._record_job_duration(conn, job=job, now=now)
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_COMPLETED, now=now
                )
                # Wake parked parent if this was a spawned child.
                if updated.get("parent_job_id") is not None:
                    self._maybe_wake_parent_on_join_locked(
                        conn,
                        parent_id=updated["parent_job_id"],
                        tenant_id=updated["tenant_id"],
                        now=now,
                    )
                self._maybe_finalize_offboard(
                    conn, tenant_id=job.tenant_id, now=now
                )
                return job

        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded; refusing ack"
            )
        raise LeaseError(f"job {job_id} lease lost during ack")


    def release(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        reason: str | None = None,
    ) -> Job:
        """Return a leased job to ``pending`` immediately (no failure count).

        Clears the lease so another worker can claim without waiting for
        visibility timeout. Does **not** change ``attempts`` (the claim already
        counted). ``available_at`` is set to now. Intended for graceful drain
        on worker shutdown/deploy.
        """
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    available_at = %s,
                    last_error = COALESCE(%s, last_error),
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.PENDING.value,
                    now,
                    reason,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during release")
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="released",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"reason": reason or "drain"},
                now=now,
            )
            self._maybe_finalize_offboard(
                conn, tenant_id=updated["tenant_id"], now=now
            )
            return _row_to_job(updated)


    def nack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        error: str | None = None,
        requeue: bool = True,
    ) -> Job:
        """Release a lease after a processing failure.

        If *requeue* is True and attempts < max_attempts, the job returns to
        ``pending`` with ``available_at`` pushed forward by exponential
        backoff (base * 2^(attempts-1), capped by the job's max backoff) so
        it is not immediately leaseable again. When attempts are exhausted
        (or requeue is False), the job is ``dead`` / ``failed`` and dependents
        are cascade-cancelled. *tenant_id* must match the current owner or
        ``lease_tenant_id`` (pre-merge claim tenant).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            attempts = row["attempts"]
            max_attempts = row["max_attempts"]
            owner_tenant = row["tenant_id"]

            if requeue and attempts < max_attempts:
                new_status = JobStatus.PENDING.value
                base = row.get("retry_backoff_base_seconds")
                cap = row.get("retry_backoff_max_seconds")
                backoff = compute_retry_backoff_seconds(
                    attempts,
                    base_seconds=1 if base is None else int(base),
                    max_seconds=300 if cap is None else int(cap),
                )
                next_available = now + timedelta(seconds=backoff)
            elif attempts >= max_attempts:
                new_status = JobStatus.DEAD.value
                next_available = row["available_at"]
            else:
                new_status = JobStatus.FAILED.value
                next_available = row["available_at"]

            # Stamp completed_at for terminal outcomes so retention can age them.
            terminal_at = (
                now
                if new_status
                in (
                    JobStatus.DEAD.value,
                    JobStatus.FAILED.value,
                )
                else None
            )
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    available_at = %s,
                    last_error = %s,
                    completed_at = COALESCE(%s, completed_at),
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    new_status,
                    next_available,
                    error,
                    terminal_at,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during nack")

            event_type = (
                "requeued"
                if new_status == JobStatus.PENDING.value
                else (
                    "dead_lettered"
                    if new_status == JobStatus.DEAD.value
                    else "failed"
                )
            )
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type=event_type,
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"error": error, "requeue": requeue},
                now=now,
            )

            if new_status in (JobStatus.DEAD.value, JobStatus.FAILED.value):
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=job_id,
                    tenant_id=owner_tenant,
                    reason=(
                        f"parent job {job_id} ended as {new_status}"
                        + (f": {error}" if error else "")
                    ),
                    now=now,
                )
            job = _row_to_job(updated)
            if new_status == JobStatus.DEAD.value:
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_DEAD, now=now
                )
            elif new_status == JobStatus.FAILED.value:
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_FAILED, now=now
                )
            # Child terminal fail must wake/fail a parked parent join.
            if (
                updated.get("parent_job_id") is not None
                and new_status
                in (
                    JobStatus.DEAD.value,
                    JobStatus.FAILED.value,
                )
            ):
                self._maybe_wake_parent_on_join_locked(
                    conn,
                    parent_id=updated["parent_job_id"],
                    tenant_id=owner_tenant,
                    now=now,
                )
            # Offboarding: requeued pending is purged when no leases remain;
            # terminal nacks also trigger purge when this was the last holder.
            self._maybe_finalize_offboard(
                conn, tenant_id=job.tenant_id, now=now
            )
            return job


    def ensure_lease(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
    ) -> Job:
        """Return the job if this worker still holds a live lease; else raise.

        After ``merge_tenant``, *tenant_id* may be the pre-merge claim tenant
        (``lease_tenant_id``); an unrelated tenant is still rejected.

        If ``max_runtime_seconds`` has elapsed, force-releases the lease (same
        as :meth:`heartbeat`) so cooperative handlers cannot keep a stuck
        lease alive by only calling ``ensure_held`` without heartbeating.
        """
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_lease_holder(row, worker_id, lease_token)
            hard = self._hard_deadline(row)
            if hard is not None and now >= hard:
                self._release_max_runtime_exceeded(conn, row, now=now)
                runtime_exceeded = True
            else:
                runtime_exceeded = False
                self._assert_lease_not_expired(row, now)
                return _row_to_job(row)
        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded "
                f"(limit={row.get('max_runtime_seconds')}s)"
            )
        raise LeaseError(f"job {job_id} lease lost during ensure_lease")


    def checkpoint(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        progress: Any,
    ) -> Job:
        """Persist progress JSON for a leased job (survives reclaim).

        Only the current lease holder may write. *progress* is stored as JSON
        on the job (``checkpoint`` / ``checkpointed_at``) and returned on
        subsequent ``lease`` / ``get`` so a replacement worker can resume.
        Does not clear on nack requeue or lease expiry; cancel clears it.
        """
        now = _utcnow()
        progress_json = Jsonb(progress) if progress is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET checkpoint = %s,
                    checkpointed_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    progress_json,
                    now,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during checkpoint")
            job = _row_to_job(updated)
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="checkpointed",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"checkpoint": progress},
                now=now,
            )
            return job


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


    def _record_job_duration(
        self,
        conn: Any,
        *,
        job: Job,
        now: datetime,
    ) -> None:
        """Update EWMA of observed runtimes for future remaining estimates."""
        started = job.lease_started_at or job.created_at
        if started is None:
            return
        duration = max(0.1, (now - _as_utc(started)).total_seconds())
        # Cap outliers so one stuck lease does not poison the estimate.
        duration = min(duration, float(job.max_runtime_seconds or 3600))
        kind = self._payload_kind(job.payload)
        existing = conn.execute(
            """
            SELECT ewma_seconds, samples
            FROM duration_estimates
            WHERE tenant_id = %s AND queue_name = %s AND kind = %s
            FOR UPDATE
            """,
            (job.tenant_id, job.queue_name, kind),
        ).fetchone()
        if existing is None:
            ewma = duration
            samples = 1
        else:
            prev = float(existing["ewma_seconds"])
            samples = int(existing["samples"]) + 1
            ewma = _EWMA_ALPHA * duration + (1.0 - _EWMA_ALPHA) * prev
        conn.execute(
            """
            INSERT INTO duration_estimates (
                tenant_id, queue_name, kind, ewma_seconds, samples, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, queue_name, kind) DO UPDATE
            SET ewma_seconds = EXCLUDED.ewma_seconds,
                samples = EXCLUDED.samples,
                updated_at = EXCLUDED.updated_at
            """,
            (job.tenant_id, job.queue_name, kind, ewma, samples, now),
        )


    def _base_estimate_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
    ) -> float:
        if row.get("estimated_duration_seconds") is not None:
            return float(row["estimated_duration_seconds"])
        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        kind = self._payload_kind(payload)
        ewma = self._lookup_ewma_seconds(
            conn,
            tenant_id=row["tenant_id"],
            queue_name=row["queue_name"],
            kind=kind,
        )
        if ewma is not None and ewma > 0:
            return float(ewma)
        # Payload steps heuristic for demo workers.
        if isinstance(payload, dict) and payload.get("steps") is not None:
            try:
                steps = max(1, int(payload["steps"]))
                step_ms = int(payload.get("step_ms") or 200)
                return max(1.0, steps * step_ms / 1000.0)
            except (TypeError, ValueError):
                pass
        return _DEFAULT_ESTIMATE_SECONDS


    def _remaining_estimate_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> float:
        base = self._base_estimate_seconds(conn, row)
        cp = row.get("checkpoint")
        if isinstance(cp, str):
            try:
                cp = json.loads(cp)
            except json.JSONDecodeError:
                cp = None
        if isinstance(cp, dict):
            step = cp.get("step")
            total = cp.get("total")
            try:
                if step is not None and total is not None and int(total) > 0:
                    frac = min(1.0, max(0.0, int(step) / int(total)))
                    return max(0.05 * base, base * (1.0 - frac))
                if step is not None and row.get("payload"):
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(payload, dict) and payload.get("steps"):
                        total_s = max(1, int(payload["steps"]))
                        frac = min(1.0, max(0.0, int(step) / total_s))
                        return max(0.05 * base, base * (1.0 - frac))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        if row.get("status") == JobStatus.LEASED.value and row.get(
            "lease_started_at"
        ):
            elapsed = (now - _as_utc(row["lease_started_at"])).total_seconds()
            return max(0.1 * base, base - elapsed)
        return base


    def _slack_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> float:
        """Seconds of schedule margin: deadline − now − remaining work.

        ``+inf`` when the job has no deadline (never deadline-urgent).
        """
        deadline = row.get("deadline_at")
        if deadline is None:
            return float("inf")
        rem = self._remaining_estimate_seconds(conn, row, now=now)
        return (_as_utc(deadline) - now).total_seconds() - rem


    def _preempt_leased_job(
        self,
        conn: Any,
        victim: dict[str, Any],
        *,
        now: datetime,
        reason: str,
    ) -> dict[str, Any] | None:
        """Park a leased job as pending; clear fence; keep checkpoint.

        Does **not** burn an attempt. Returns updated row or None if lost race.
        """
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                available_at = %s,
                preempt_count = preempt_count + 1,
                last_preempted_at = %s,
                last_error = %s,
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND lease_token IS NOT NULL
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.PENDING.value,
                now,
                now,
                reason[:500],
                now,
                victim["id"],
                JobStatus.LEASED.value,
            ),
        ).fetchone()
        if updated is not None:
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="preempted",
                from_status=JobStatus.LEASED.value,
                actor="system",
                detail={"reason": reason[:500]},
                now=now,
            )
        return updated


    def _try_preempt_for_tenant(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        now: datetime,
        urgent_hint: dict[str, Any] | None = None,
    ) -> PreemptResult | None | bool:
        """If an urgent pending job needs a slot, preempt one less-urgent lease.

        Returns :class:`PreemptResult` on success, ``None``/``False`` otherwise.
        When used as a bool (lease paths), truthy means a slot was freed.
        """
        conc = self._read_concurrency_limit(conn, tenant_id)
        if conc is None:
            return None
        leased_n = self._count_leased(conn, tenant_id)
        if leased_n < conc:
            return None

        # Claimable deadline pending ordered by urgency (low slack first).
        pending_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
              AND available_at <= %s
              AND deadline_at IS NOT NULL
              AND {_DEPS_SATISFIED_SQL}
            ORDER BY priority DESC,
              (EXTRACT(EPOCH FROM (deadline_at - %s))
                - COALESCE(estimated_duration_seconds, 60)) ASC,
              created_at ASC
            LIMIT 16
            """,
            (
                tenant_id,
                queue_name,
                JobStatus.PENDING.value,
                now,
                now,
            ),
        ).fetchall()
        if not pending_rows and urgent_hint is not None:
            if (
                urgent_hint.get("tenant_id") == tenant_id
                and urgent_hint.get("status") == JobStatus.PENDING.value
                and urgent_hint.get("deadline_at") is not None
            ):
                pending_rows = [urgent_hint]
        if not pending_rows:
            return None

        leased_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
            FOR UPDATE
            """,
            (tenant_id, queue_name, JobStatus.LEASED.value),
        ).fetchall()
        if not leased_rows:
            return None

        # Score pending candidates.
        scored_pending: list[tuple[tuple, dict[str, Any], float]] = []
        for prow in pending_rows:
            slack = self._slack_seconds(conn, prow, now=now)
            if slack == float("inf"):
                continue
            scored_pending.append(
                (self._urgency_tuple(conn, prow, now=now), prow, slack)
            )
        if not scored_pending:
            return None
        scored_pending.sort(key=lambda x: x[0])
        urgent_key, urgent, urgent_slack = scored_pending[0]

        # Victims: preemptible, under max preempts, strictly less urgent.
        candidates: list[tuple[tuple, dict[str, Any], float]] = []
        for vrow in leased_rows:
            if not bool(vrow.get("preemptible", True)):
                continue
            if int(vrow.get("preempt_count") or 0) >= _MAX_PREEMPTS_PER_JOB:
                continue
            v_key = self._urgency_tuple(conn, vrow, now=now)
            # Strictly less urgent than the pending job.
            if v_key <= urgent_key:
                continue
            v_slack = self._slack_seconds(conn, vrow, now=now)
            rem = self._remaining_estimate_seconds(conn, vrow, now=now)
            candidates.append((v_key, vrow, rem if rem > 0 else 0.0))

        if not candidates:
            return None

        # Can we wait for the soonest less-urgent holder to finish?
        min_wait = min(c[2] for c in candidates)
        # Only preempt if waiting would risk the deadline (slack < wait).
        # Small buffer (0.5s) avoids thrash on equal estimates.
        if urgent_slack >= min_wait - 0.5:
            return None

        # Prefer victim that is least urgent, then longest remaining (free
        # more useful work later), then lowest preempt_count (anti thrash).
        def victim_rank(item: tuple) -> tuple:
            v_key, vrow, rem = item
            return (
                v_key[0],
                -v_key[1] if v_key[1] != float("inf") else -1e18,
                v_key[2],
                -rem,
                int(vrow.get("preempt_count") or 0),
            )

        # Actually sort by least urgent first: largest urgency tuple.
        candidates.sort(key=lambda x: x[0], reverse=True)
        # Among equal urgency, pick higher remaining then lower preempt_count.
        best = candidates[0]
        for c in candidates[1:]:
            if c[0] == best[0]:
                if c[2] > best[2] or (
                    c[2] == best[2]
                    and int(c[1].get("preempt_count") or 0)
                    < int(best[1].get("preempt_count") or 0)
                ):
                    best = c
            else:
                break
        _, victim, _ = best
        victim_slack = self._slack_seconds(conn, victim, now=now)

        updated = self._preempt_leased_job(
            conn,
            victim,
            now=now,
            reason=(
                f"preempted for deadline job {urgent['id']} "
                f"(urgent_slack={urgent_slack:.1f}s)"
            ),
        )
        if updated is None:
            return None
        return PreemptResult(
            tenant_id=tenant_id,
            queue_name=queue_name,
            urgent_job_id=urgent["id"],
            victim_job_id=victim["id"],
            urgent_slack_seconds=float(urgent_slack),
            victim_slack_seconds=(
                float(victim_slack)
                if victim_slack != float("inf")
                else float("inf")
            ),
        )


    def _preempt_blocked_urgent(
        self,
        conn: Any,
        *,
        queue_name: str,
        now: datetime,
        limit: int = 8,
    ) -> int:
        """Run preemption for tenants with deadline pending blocked by caps."""
        rows = conn.execute(
            """
            SELECT DISTINCT j.tenant_id
            FROM jobs j
            JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
            WHERE j.queue_name = %s
              AND j.status = %s
              AND j.deadline_at IS NOT NULL
              AND j.available_at <= %s
              AND ts.max_concurrent_leases IS NOT NULL
              AND ts.paused = FALSE
            LIMIT %s
            """,
            (
                queue_name,
                JobStatus.PENDING.value,
                now,
                max(limit * 2, 8),
            ),
        ).fetchall()
        n = 0
        for r in rows:
            if n >= limit:
                break
            tid = r["tenant_id"]
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tid,),
            )
            if self._try_preempt_for_tenant(
                conn, tenant_id=tid, queue_name=queue_name, now=now
            ):
                n += 1
        return n


    def _lock_job_for_tenant(
        self,
        conn: Any,
        job_id: UUID,
        tenant_id: str,
    ) -> dict[str, Any]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        row = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (job_id, tenant_id),
        ).fetchone()
        if row is None:
            raise LeaseError(f"job {job_id} not found for tenant {tenant_id!r}")
        return row


    def _lock_job_for_lease(
        self,
        conn: Any,
        job_id: UUID,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Lock a job for heartbeat / ack / nack / ensure_lease by id.

        Tenant authorization is applied separately via
        :meth:`_assert_lease_tenant` (current owner **or** frozen
        ``lease_tenant_id``). Locking by id allows the row to be found after
        ``merge_tenant`` moved ``tenant_id`` while the worker still holds the
        pre-merge tenant string.
        """
        row = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            if tenant_id is not None:
                raise LeaseError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            raise LeaseError(f"job {job_id} not found")
        return row


    @staticmethod
    def _assert_lease_tenant(row: dict[str, Any], tenant_id: str) -> None:
        """Reject lease ops from tenants that neither own nor claimed the job.

        Accepts:
        * current ``tenant_id`` (owner after merge or normal path)
        * ``lease_tenant_id`` frozen at claim (pre-merge worker after transfer)

        Rejects any other tenant even when the lease token is valid — that is
        the cross-tenant safety boundary.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        current = row["tenant_id"]
        claim_tenant = row.get("lease_tenant_id")
        if tenant_id == current:
            return
        if claim_tenant is not None and tenant_id == claim_tenant:
            return
        raise LeaseError(
            f"job {row['id']} not found for tenant {tenant_id!r}"
        )


    def _try_consume_lease_rate(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> bool:
        """Consume one lease token; False if rate-limited (no exception)."""
        ok, _ = self._try_consume_rate_tokens(
            conn,
            tenant_id,
            kind="lease",
            now=now,
            cost=1.0,
        )
        return ok


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
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tid,
                reason=(
                    f"parent job {row['id']} cancelled "
                    f"(lease expired during offboard)"
                ),
                now=now,
            )

        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)


    @staticmethod
    def _count_leased(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM jobs
            WHERE tenant_id = %s AND status = %s
            """,
            (tenant_id, JobStatus.LEASED.value),
        ).fetchone()
        assert row is not None
        return int(row["n"])


    @staticmethod
    def _assert_lease_holder(
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
    ) -> None:
        if row["status"] != JobStatus.LEASED.value:
            raise LeaseError(
                f"job {row['id']} is not leased (status={row['status']})"
            )
        if row["leased_by"] != worker_id:
            raise LeaseError(
                f"job {row['id']} is leased by {row['leased_by']!r}, "
                f"not {worker_id!r}"
            )
        if row["lease_token"] != lease_token:
            raise LeaseError(
                f"job {row['id']} lease_token mismatch "
                f"(fence invalidated; lease was reclaimed)"
            )


    @staticmethod
    def _assert_lease_not_expired(row: dict[str, Any], now: datetime) -> None:
        expires = row["lease_expires_at"]
        if expires is not None and expires < now:
            raise LeaseError(f"job {row['id']} lease has expired")


    @classmethod
    def _assert_active_lease(
        cls,
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
        now: datetime,
    ) -> None:
        cls._assert_lease_holder(row, worker_id, lease_token)
        cls._assert_lease_not_expired(row, now)


