"""JobQueue list_orphaned_audit_jobs."""

from __future__ import annotations

from uuid import UUID


class AuditOrphansMixin:
    def list_orphaned_audit_jobs(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 1000,
    ) -> list[UUID]:
        """Job ids that have audit history but no ``jobs`` row (rebuild candidates)."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._pool.connection() as conn:
            if tenant_id is None:
                rows = conn.execute(
                    """
                    SELECT DISTINCT e.job_id
                    FROM job_audit_events e
                    WHERE NOT EXISTS (
                        SELECT 1 FROM jobs j WHERE j.id = e.job_id
                    )
                    ORDER BY e.job_id
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT e.job_id
                    FROM job_audit_events e
                    INNER JOIN (
                        SELECT job_id, MAX(seq) AS max_seq
                        FROM job_audit_events
                        GROUP BY job_id
                    ) last ON last.job_id = e.job_id AND last.max_seq = e.seq
                    WHERE e.tenant_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM jobs j WHERE j.id = e.job_id
                      )
                    ORDER BY e.job_id
                    LIMIT %s
                    """,
                    (tenant_id, limit),
                ).fetchall()
            return [r["job_id"] for r in rows]
