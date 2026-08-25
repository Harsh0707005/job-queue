"""JobQueue per-tenant lease claim helpers and locks."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

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
    _deduct_resources,
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


class LeaseClaimTenantMixin:
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

        # Maintenance in a short transaction so reclaim/dead-letter/timeouts
        # do not hold the per-tenant claim lock across the whole batch (fair
        # lease already does this — scoped path was a multi-worker bottleneck).
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=tenant_id)
            self._reap_offboarding_expired_leases(conn, now=now, tenant_id=tenant_id)
            self._expire_parked_timeouts_locked(conn, now=now, tenant_id=tenant_id)
        if hasattr(self, "process_workflow_compensation"):
            try:
                self.process_workflow_compensation(
                    tenant_id=tenant_id, limit=max(8, limit)
                )
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_workflow_signal_timeouts"):
            try:
                self.process_workflow_signal_timeouts(
                    tenant_id=tenant_id, limit=max(8, limit)
                )
            except Exception:  # noqa: BLE001
                pass
        if hasattr(self, "process_worker_timeouts"):
            try:
                self.process_worker_timeouts()
            except Exception:  # noqa: BLE001
                pass

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
        with transaction(self._pool) as conn:
            # Worker cap lock first, then tenant lock — same order as fair
            # claim so a capped worker cannot deadlock with a tenant lock.
            worker_remaining: int | None = None
            if hasattr(self, "_worker_lease_remaining_locked"):
                worker_remaining = self._worker_lease_remaining_locked(
                    conn, worker_id, now=now
                )
                if worker_remaining is not None and worker_remaining <= 0:
                    return []
            # Serialize lease decisions for this tenant so concurrency caps
            # cannot be exceeded by racing workers.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )

            # Paused or breaker open: no new claims (pending or reclaim).
            # In-flight holders still use heartbeat/ack/nack outside this path.
            if self._tenant_claims_blocked(conn, tenant_id, now=now):
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

            part_sql = ""
            part_params: list[Any] = []
            if hasattr(self, "_partition_claim_predicate"):
                part_sql, part_params = self._partition_claim_predicate(
                    worker_id=worker_id,
                    owned_parts=owned_parts,
                    jobs_alias="jobs",
                )
            elif owned_parts is not None:
                part_sql = "AND partition_id = ANY(%s)"
                part_params = [list(owned_parts)]

            skill_sql = _skill_claim_sql("jobs")
            skill_params: list[Any] = [worker_id]
            avoid_sql = _failed_worker_avoid_sql("jobs")
            avoid_params: list[Any] = [worker_id, worker_id, worker_id]
            capacity: dict[str, int] = {}
            free: dict[str, int] = {}
            if hasattr(self, "_worker_capacity_state_locked"):
                capacity, free = self._worker_capacity_state_locked(
                    conn, worker_id, now=now
                )
            res_sql, res_params = _resource_claim_sql(
                "jobs", capacity=capacity, free=dict(free)
            )
            prefer_sql = _failed_worker_prefer_sql("jobs")
            affinity_sql = _last_success_worker_prefer_sql("jobs")

            claim_passes = 0
            while claim_passes < 4:
                claim_passes += 1
                fail_closed = 0
                if conc_limit is not None and leased_count >= conc_limit:
                    # Only reclaim expired leases (does not increase concurrent
                    # holders beyond the cap in steady state). Still honor the
                    # worker inflight cap so a full worker cannot grow.
                    reclaim_take = limit
                    if worker_remaining is not None:
                        reclaim_take = min(reclaim_take, worker_remaining)
                    if reclaim_take < 1:
                        return claimed
                    candidates = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      {part_sql}
                      {skill_sql}
                      {avoid_sql}
                      {res_sql}
                      AND status = %s
                      AND lease_expires_at < %s
                      AND attempts < max_attempts
                      {_NOT_TTL_EXPIRED_SQL}
                    ORDER BY {prefer_sql},
                      priority DESC,
                      {affinity_sql},
                      (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                        - COALESCE(estimated_duration_seconds, 60)) ASC,
                      created_at ASC
                    FOR UPDATE OF jobs SKIP LOCKED
                    LIMIT %s
                    """,
                    tuple(
                        [tenant_id, queue_name]
                        + part_params
                        + skill_params
                        + avoid_params
                        + res_params
                        + [
                            JobStatus.LEASED.value,
                            now,
                            now,
                            worker_id,
                            worker_id,
                            now,
                            now,
                            reclaim_take,
                        ]
                    ),
                ).fetchall()
                else:
                    take = limit
                    if conc_limit is not None:
                        take = min(take, conc_limit - leased_count)
                        take = max(take, 0)
                    if worker_remaining is not None:
                        take = min(take, worker_remaining)
                        take = max(take, 0)
                    if take < 1:
                        return claimed
                    candidates = conn.execute(
                        f"""
                        SELECT {_JOB_COLUMNS}
                        FROM jobs
                        WHERE tenant_id = %s
                          AND queue_name = %s
                          {part_sql}
                          {skill_sql}
                          {avoid_sql}
                          {res_sql}
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
                          {_NOT_TTL_EXPIRED_SQL}
                        ORDER BY {prefer_sql},
                          priority DESC,
                          {affinity_sql},
                          CASE WHEN deadline_at IS NULL THEN 1 ELSE 0 END ASC,
                          (EXTRACT(EPOCH FROM (COALESCE(deadline_at, %s + INTERVAL '100 years') - %s))
                            - COALESCE(estimated_duration_seconds, 60)) ASC,
                          created_at ASC
                        FOR UPDATE OF jobs SKIP LOCKED
                        LIMIT %s
                        """,
                        tuple(
                            [tenant_id, queue_name]
                            + part_params
                            + skill_params
                            + avoid_params
                            + res_params
                            + [
                                JobStatus.PENDING.value,
                                now,
                                JobStatus.LEASED.value,
                                now,
                                now,
                                worker_id,
                                worker_id,
                                now,
                                now,
                                take,
                            ]
                        ),
                    ).fetchall()

                for candidate in candidates:
                    # Rebalance fence for unskilled work only. Skilled jobs may
                    # cross shards so they do not stall on an incapable owner.
                    hop_open = bool(candidate.get("failed_workers")) or (
                        candidate["status"] == JobStatus.LEASED.value
                    )
                    affinity_hop = hasattr(self, "_job_allows_affinity_hop") and (
                        self._job_allows_affinity_hop(conn, candidate, worker_id)
                    )
                    if (
                        owned_parts is not None
                        and not _row_has_required_skills(candidate)
                        and not hop_open
                        and not affinity_hop
                        and hasattr(self, "_guard_partition_claim")
                    ):
                        exp = None
                        if part_gens is not None:
                            exp = part_gens.get(int(candidate["partition_id"]))
                        try:
                            self._guard_partition_claim(
                                conn,
                                worker_id=worker_id,
                                partition_id=int(candidate["partition_id"]),
                                expected_generation=exp,
                            )
                        except LeaseError:
                            continue
                    # Rate-limit new pending claims only; reclaim is free so
                    # recovery is not blocked by a noisy tenant's throttle.
                    if candidate["status"] == JobStatus.PENDING.value:
                        if not self._try_consume_lease_rate(conn, tenant_id, now=now):
                            break
                    cost = _resource_map_from_row(candidate.get("resource_cost"))
                    if not _job_fits_resources(
                        cost, capacity=capacity, free=free
                    ):
                        continue
                    try:
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
                        _deduct_resources(free, cost)
                        leased_count += 1
                    except UnreadablePayloadError:
                        # Missing/corrupt blob: job is now dead. Refetch so a
                        # sibling can fill this pull instead of returning empty.
                        fail_closed += 1
                        continue
                    except LeaseError:
                        # Row no longer claimable (e.g. concurrent max-runtime release).
                        continue
                    if len(claimed) >= limit:
                        break
                if len(claimed) >= limit or fail_closed == 0:
                    break
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
        from job_queue.queue.lease_quarantine import _failed_workers_plus

        workers = list(candidate.get("failed_workers") or [])
        if (
            candidate["status"] == JobStatus.LEASED.value
            and candidate.get("leased_by")
            and hasattr(self, "_quarantine_job_if_due")
        ):
            workers = _failed_workers_plus(workers, candidate["leased_by"])
            qrow = self._quarantine_job_if_due(
                conn,
                candidate,
                workers=workers,
                worker_id=str(candidate["leased_by"]),
                reason="lease expired",
                now=now,
            )
            if qrow is not None:
                raise LeaseError(
                    f"job {candidate['id']} quarantined after worker hops"
                )

        new_attempts = candidate["attempts"] + 1
        vt = visibility_timeout_seconds or candidate["visibility_timeout_seconds"]
        max_rt = int(candidate.get("max_runtime_seconds") or 3600)
        lease_expires = now + timedelta(seconds=vt)
        hard = now + timedelta(seconds=max_rt)
        if lease_expires > hard:
            lease_expires = hard

        from_status = candidate["status"]
        if from_status == JobStatus.LEASED.value and hasattr(
            self, "_record_lease_end_usage"
        ):
            # Previous expired lease: bill its runtime before this reclaim.
            self._record_lease_end_usage(conn, candidate, now=now)
        # Optimistic predicates: never claim a completed/cancelled/pending-not-ready
        # row even if a caller held a stale snapshot (defense in depth under load).
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
                failed_workers = %s,
                updated_at = %s,
                last_error = NULL
            WHERE id = %s
              AND tenant_id = %s
              AND (
                    (
                        status = %s
                        AND available_at <= %s
                    )
                 OR (
                        status = %s
                        AND lease_expires_at IS NOT NULL
                        AND lease_expires_at < %s
                        AND attempts < max_attempts
                    )
              )
              {_NOT_TTL_EXPIRED_SQL}
              {_skill_claim_sql("jobs")}
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
                workers,
                now,
                candidate["id"],
                candidate["tenant_id"],
                JobStatus.PENDING.value,
                now,
                JobStatus.LEASED.value,
                now,
                now,
                worker_id,
            ),
        ).fetchone()
        if row is None:
            raise LeaseError(
                f"job {candidate['id']} claim race "
                f"(status changed under lock for tenant {candidate['tenant_id']!r})"
            )
        if hasattr(self, "_fail_closed_unreadable_payload"):
            self._fail_closed_unreadable_payload(
                conn, row, worker_id=worker_id, now=now
            )
        job = self._job(row, conn=conn)
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
        # Durable throughput counter (same txn as claim).
        wait_ms = 0.0
        wait_count = 0
        if from_status == JobStatus.PENDING.value:
            avail = candidate.get("available_at") or candidate.get("created_at")
            if avail is not None:
                if getattr(avail, "tzinfo", None) is None:
                    from datetime import timezone

                    avail = avail.replace(tzinfo=timezone.utc)
                wait_ms = max(0.0, (now - avail).total_seconds() * 1000.0)
                wait_count = 1
        self._record_metrics_batch(
            conn,
            tenant_id=str(candidate["tenant_id"]),
            increments={"lease": 1},
            wait_sum_ms=wait_ms,
            wait_count=wait_count,
            now=now,
        )
        if hasattr(self, "_mark_workflow_step_leased"):
            self._mark_workflow_step_leased(conn, row, now=now)
        return job

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
                raise LeaseError(f"job {job_id} not found for tenant {tenant_id!r}")
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
        raise LeaseError(f"job {row['id']} not found for tenant {tenant_id!r}")

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
