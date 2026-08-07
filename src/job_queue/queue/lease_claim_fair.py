"""JobQueue fair multi-tenant lease claim."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _DEPS_SATISFIED_SQL,
    _JOB_COLUMNS,
    LeaseError,
    _SkipLeaseCandidate,
    _utcnow,
)


class LeaseClaimFairMixin:
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
        # Saga compensate redrive (own txn; must not hold locks across claims).
        if hasattr(self, "process_workflow_compensation"):
            try:
                self.process_workflow_compensation(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass

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
                if skip.tenant_id and skip.reason == "tenant lease rate limited":
                    rate_limited_tenants.add(skip.tenant_id)
                continue
            except LeaseError:
                # Claim predicate race under load — try another candidate.
                skips += 1
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
            raise _SkipLeaseCandidate("tenant paused", tenant_id=candidate["tenant_id"])
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
                    leased_count = self._count_leased(conn, candidate["tenant_id"])
                if leased_count >= limit:
                    raise _SkipLeaseCandidate(
                        "tenant at concurrency limit",
                        tenant_id=candidate["tenant_id"],
                    )
            # Token-bucket lease rate: skip + rotate LRS so other tenants
            # keep getting work while this tenant waits for tokens.
            if not self._try_consume_lease_rate(conn, candidate["tenant_id"], now=now):
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
