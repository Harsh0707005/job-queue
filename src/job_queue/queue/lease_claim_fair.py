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
    _NOT_TTL_EXPIRED_SQL,
    LeaseError,
    UnreadablePayloadError,
    _SkipLeaseCandidate,
    _failed_worker_avoid_sql,
    _failed_worker_prefer_sql,
    _job_fits_resources,
    _last_success_worker_prefer_sql,
    _resource_claim_sql,
    _resource_map_from_row,
    _row_has_required_skills,
    _skill_claim_sql,
    _utcnow,
)
from job_queue.queue.tenants_breaker import _TENANT_CLAIMS_BLOCKED_SQL

# Returned (not raised) so the claim transaction commits the dead-letter.
_UNREADABLE_PAYLOAD = object()


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

        Each claim advances ``tenant_fairness.fair_virtual`` by ``1/weight``
        (and ``last_lease_at``) so subsequent jobs rotate tenants within a
        priority lane. Higher-weight tenants increment slower and get more
        turns; lower-weight tenants still become the min score and cannot
        starve.
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
        if hasattr(self, "process_workflow_signal_timeouts"):
            try:
                self.process_workflow_signal_timeouts(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_workflow_timers"):
            try:
                self.process_workflow_timers(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_workflow_activities"):
            try:
                self.process_workflow_activities(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_workflow_approval_timeouts"):
            try:
                self.process_workflow_approval_timeouts(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_workflow_run_deadlines"):
            try:
                self.process_workflow_run_deadlines(limit=max(8, limit))
            except Exception:  # noqa: BLE001
                pass
        # Expire dead workers and rebalance partition ownership.
        if hasattr(self, "process_worker_timeouts"):
            try:
                self.process_worker_timeouts()
            except Exception:  # noqa: BLE001
                pass

        # Resolve partition filter once ([] = no unskilled claim; None = open).
        # Active workers with no shards can still claim skilled jobs.
        # Snapshot generations for the rebalance fence inside each claim txn.
        owned_parts: list[int] | None = None
        part_gens: dict[int, int] | None = None
        if hasattr(self, "_worker_partition_filter"):
            with self._pool.connection() as conn:
                owned_parts = self._worker_partition_filter(conn, worker_id)
                if owned_parts is not None and owned_parts:
                    if hasattr(self, "_owned_partition_generations_locked"):
                        part_gens = self._owned_partition_generations_locked(
                            conn, worker_id
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
                        partitions=owned_parts,
                        partition_generations=part_gens,
                    )
                    if job is _UNREADABLE_PAYLOAD:
                        skips += 1
                        continue
                    if job is None:
                        break
                    claimed.append(job)
            except _SkipLeaseCandidate as skip:
                skips += 1
                if skip.tenant_id and skip.reason in (
                    "tenant lease rate limited",
                    "tenant paused",
                    "tenant breaker open",
                ):
                    rate_limited_tenants.add(skip.tenant_id)
                continue
            except LeaseError:
                # Claim predicate race under load / rebalance fence — try again.
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
        partitions: list[int] | None = None,
        partition_generations: dict[int, int] | None = None,
    ) -> Job | None:
        token = uuid4()
        # Worker inflight cap: lock the registry row before the tenant lock
        # (same order as the scoped path). At cap, stop the batch.
        if hasattr(self, "_worker_lease_remaining_locked"):
            remaining = self._worker_lease_remaining_locked(
                conn, worker_id, now=now
            )
            if remaining is not None and remaining <= 0:
                return None
        capacity: dict[str, int] = {}
        free: dict[str, int] = {}
        if hasattr(self, "_worker_capacity_state_locked"):
            capacity, free = self._worker_capacity_state_locked(
                conn, worker_id, now=now
            )
        # Priority lanes first (higher priority always before lower), then
        # weighted virtual time within that lane (never-served = 0). A noisy
        # tenant's high-priority flood still rotates with other tenants at
        # the same priority; it cannot leapfrog higher-priority work of
        # quieter tenants, and low-priority noise cannot starve high.
        # Higher lease_weight increments virtual time slower (more turns)
        # without starving weight-1 tenants.
        excluded = sorted(exclude_tenants or ())
        exclude_sql = ""
        part_sql = ""
        part_params: list[Any] = []
        params: list[Any] = [queue_name]
        if excluded:
            exclude_sql = "AND jobs.tenant_id <> ALL(%s)"
            params.append(excluded)
        if hasattr(self, "_partition_claim_predicate"):
            part_sql, part_params = self._partition_claim_predicate(
                worker_id=worker_id,
                owned_parts=partitions,
                jobs_alias="jobs",
            )
            params.extend(part_params)
        elif partitions is not None:
            part_sql = "AND jobs.partition_id = ANY(%s)"
            params.append(list(partitions))
        skill_sql = _skill_claim_sql("jobs")
        params.append(worker_id)
        avoid_sql = _failed_worker_avoid_sql("jobs")
        params.extend([worker_id, worker_id, worker_id])
        res_sql, res_params = _resource_claim_sql(
            "jobs", capacity=capacity, free=free
        )
        params.extend(res_params)
        prefer_sql = _failed_worker_prefer_sql("jobs")
        affinity_sql = _last_success_worker_prefer_sql("jobs")
        params.extend(
            [
                JobStatus.PENDING.value,
                now,
                JobStatus.LEASED.value,
                JobStatus.LEASED.value,
                now,
                now,
            ]
        )
        params.append(worker_id)
        params.append(worker_id)
        params.extend([now, now])
        candidate = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE queue_name = %s
              {exclude_sql}
              {part_sql}
              {skill_sql}
              {avoid_sql}
              {res_sql}
              AND NOT EXISTS (
                    SELECT 1
                    FROM tenant_settings ts
                    WHERE ts.tenant_id = jobs.tenant_id
                      AND {_TENANT_CLAIMS_BLOCKED_SQL}
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
              {_NOT_TTL_EXPIRED_SQL}
            ORDER BY
              {prefer_sql},
              priority DESC,
              {affinity_sql},
              CASE WHEN deadline_at IS NULL THEN 1 ELSE 0 END ASC,
              (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                - COALESCE(estimated_duration_seconds, 60)) ASC,
              COALESCE(
                (
                    SELECT tf.fair_virtual
                    FROM tenant_fairness tf
                    WHERE tf.tenant_id = jobs.tenant_id
                ),
                0
              ) ASC,
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

        # Rebalance fence: unskilled jobs stay on owned shards. Skilled jobs
        # are allowed to cross partitions so they cannot stick on a worker
        # that cannot run them.
        cost = _resource_map_from_row(candidate.get("resource_cost"))
        if not _job_fits_resources(cost, capacity=capacity, free=free):
            raise _SkipLeaseCandidate("worker at capacity")
        hop_open = bool(candidate.get("failed_workers")) or (
            candidate["status"] == JobStatus.LEASED.value
        )
        affinity_hop = hasattr(self, "_job_allows_affinity_hop") and (
            self._job_allows_affinity_hop(conn, candidate, worker_id)
        )
        if (
            partitions is not None
            and not _row_has_required_skills(candidate)
            and not hop_open
            and not affinity_hop
            and hasattr(self, "_guard_partition_claim")
        ):
            exp = None
            if partition_generations is not None:
                exp = partition_generations.get(int(candidate["partition_id"]))
            self._guard_partition_claim(
                conn,
                worker_id=worker_id,
                partition_id=int(candidate["partition_id"]),
                expected_generation=exp,
            )

        # Re-check pause / breaker + concurrency under tenant lock (closes races).
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (candidate["tenant_id"],),
        )
        if self._tenant_claims_blocked(conn, candidate["tenant_id"], now=now):
            reason = (
                "tenant paused"
                if self._is_tenant_paused(conn, candidate["tenant_id"])
                else "tenant breaker open"
            )
            raise _SkipLeaseCandidate(reason, tenant_id=candidate["tenant_id"])
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
                self._touch_tenant_fairness(conn, candidate["tenant_id"])
                raise _SkipLeaseCandidate(
                    "tenant lease rate limited",
                    tenant_id=candidate["tenant_id"],
                )

        try:
            claimed = self._claim_locked_job(
                conn,
                candidate,
                worker_id=worker_id,
                token=token,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
        except UnreadablePayloadError:
            # Commit the dead-letter; caller skips to the next candidate.
            return _UNREADABLE_PAYLOAD
        # Weighted virtual time + LRS tie-break (drives the next fair pick).
        self._touch_tenant_fairness(conn, claimed.tenant_id)
        return claimed
