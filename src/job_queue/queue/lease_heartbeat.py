"""JobQueue lease heartbeat and holder assertions."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    LeaseError,
    _row_to_job,
    _utcnow,
)


class LeaseHeartbeatMixin:
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

    @staticmethod
    def _assert_lease_holder(
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
    ) -> None:
        if row["status"] != JobStatus.LEASED.value:
            raise LeaseError(f"job {row['id']} is not leased (status={row['status']})")
        if row["leased_by"] != worker_id:
            raise LeaseError(
                f"job {row['id']} is leased by {row['leased_by']!r}, not {worker_id!r}"
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
