"""JobQueue checkpoint."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

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


class LeaseCheckpointMixin:
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
