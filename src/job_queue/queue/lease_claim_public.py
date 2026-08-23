"""JobQueue public lease/lease_batch entrypoints."""

from __future__ import annotations

from job_queue.models import (
    Job,
)


class LeaseClaimPublicMixin:
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

        # Durable maintenance: refuse new claims even when partition
        # registration is off (unit tests) or the worker still holds shards.
        if hasattr(self, "worker_is_draining") and self.worker_is_draining(
            worker_id
        ):
            return []

        jobs = self._lease_batch_once(
            worker_id=worker_id,
            limit=limit,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        # Empty pull: revive any awaiting_skills that landed after the
        # in-txn park/revive pass (enqueue vs register TOCTOU) and retry.
        if jobs or not hasattr(self, "process_awaiting_skills"):
            return jobs
        try:
            totals = self.process_awaiting_skills(
                limit=max(50, limit * 8), max_passes=8
            )
        except Exception:  # noqa: BLE001
            return jobs
        if not totals.get("revived"):
            return jobs
        return self._lease_batch_once(
            worker_id=worker_id,
            limit=limit,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )

    def _lease_batch_once(
        self,
        *,
        worker_id: str,
        limit: int,
        tenant_id: str | None,
        queue_name: str,
        visibility_timeout_seconds: int | None,
    ) -> list[Job]:
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
