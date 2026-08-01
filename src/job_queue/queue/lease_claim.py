"""JobQueue lease claim domain (LeaseClaimMixin): rate limits, lease, batch/fair claim."""
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


class LeaseClaimMixin:
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

        # Maintenance in its own short transaction (not held across claims).
        with transaction(self._pool) as conn:
            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=None)
            self._reap_offboarding_expired_leases(conn, now=now, tenant_id=None)
            # Parked-job deadlines (approval + await-children join) for all tenants.
            self._expire_parked_timeouts_locked(conn, now=now, tenant_id=None)
            # Free slots for deadline-critical pending work before claiming.
            self._preempt_blocked_urgent(
                conn, queue_name=queue_name, now=now, limit=max(4, limit)
            )

        # One transaction per claim so per-tenant advisory locks cannot invert
        # across tenants inside a multi-claim batch (was a multi-worker deadlock
        # under fair lease: txn A holds tenant1 wants tenant2, B the reverse).
        while len(claimed) < limit and skips < max_skips:
            try:
                with transaction(self._pool) as conn:
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
