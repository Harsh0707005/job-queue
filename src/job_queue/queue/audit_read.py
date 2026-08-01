"""JobQueue audit read/verify domain: list and consistency checks."""

from __future__ import annotations

import json

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.models import (
    AuditConsistencyReport,
    JobAuditEvent,
)


class AuditReadMixin:
    @staticmethod
    def _row_to_audit(row: dict[str, Any]) -> JobAuditEvent:
        detail = row.get("detail") or {}
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except json.JSONDecodeError:
                detail = {}
        if not isinstance(detail, dict):
            detail = {"value": detail}
        return JobAuditEvent(
            event_id=row["event_id"],
            tenant_id=row["tenant_id"],
            job_id=row["job_id"],
            seq=int(row["seq"]),
            event_type=row["event_type"],
            from_status=row.get("from_status"),
            to_status=row.get("to_status"),
            actor=row.get("actor"),
            detail=detail,
            created_at=row["created_at"],
        )

    def list_job_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        after_seq: int = 0,
        limit: int = 1000,
    ) -> list[JobAuditEvent]:
        """Replay-ordered audit events for one job (tenant-authorized).

        Authorization: the job's **current** owner must be *tenant_id*. Events
        themselves keep the tenant id at write time (immutable), so after
        ``merge_tenant`` pre-merge rows still store the source tenant. This
        method returns the **full** per-job ``seq`` stream (all tenants on
        the event rows) so history is continuous across ownership transfer.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if after_seq < 0:
            raise ValueError("after_seq must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._pool.connection() as conn:
            if not self._tenant_can_read_job_audit(
                conn, job_id=job_id, tenant_id=tenant_id
            ):
                return []
            rows = conn.execute(
                """
                SELECT
                    event_id, tenant_id, job_id, seq, event_type,
                    from_status, to_status, actor, detail, created_at
                FROM job_audit_events
                WHERE job_id = %s
                  AND seq > %s
                ORDER BY seq ASC
                LIMIT %s
                """,
                (job_id, after_seq, limit),
            ).fetchall()
            return [self._row_to_audit(r) for r in rows]

    def list_tenant_audit(
        self,
        tenant_id: str,
        *,
        after_created_at: datetime | None = None,
        after_event_id: UUID | None = None,
        limit: int = 500,
    ) -> list[JobAuditEvent]:
        """Tenant-wide audit stream ordered by time (for catch-up consumers).

        Includes:
        * events originally written under *tenant_id*, and
        * events for jobs **currently** owned by *tenant_id* (so after a
          merge, the target tenant can catch up pre-merge history of jobs
          they now own without mutating immutable event rows).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        # Events visible to this tenant: written under them, or belonging to
        # a job they currently own (covers merge without rewriting history).
        ownership = """
            (
                e.tenant_id = %s
                OR EXISTS (
                    SELECT 1 FROM jobs j
                    WHERE j.id = e.job_id AND j.tenant_id = %s
                )
            )
        """
        with self._pool.connection() as conn:
            if after_created_at is None:
                rows = conn.execute(
                    f"""
                    SELECT
                        e.event_id, e.tenant_id, e.job_id, e.seq, e.event_type,
                        e.from_status, e.to_status, e.actor, e.detail, e.created_at
                    FROM job_audit_events e
                    WHERE {ownership}
                    ORDER BY e.created_at ASC, e.event_id ASC
                    LIMIT %s
                    """,
                    (tenant_id, tenant_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                        e.event_id, e.tenant_id, e.job_id, e.seq, e.event_type,
                        e.from_status, e.to_status, e.actor, e.detail, e.created_at
                    FROM job_audit_events e
                    WHERE {ownership}
                      AND (
                            e.created_at > %s
                            OR (
                                e.created_at = %s
                                AND e.event_id > %s
                            )
                          )
                    ORDER BY e.created_at ASC, e.event_id ASC
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        tenant_id,
                        after_created_at,
                        after_created_at,
                        after_event_id or UUID(int=0),
                        limit,
                    ),
                ).fetchall()
            return [self._row_to_audit(r) for r in rows]

    def verify_job_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
    ) -> AuditConsistencyReport:
        """Replay full audit for *job_id* and compare final status to the job row.

        Uses the complete per-job ``seq`` stream (including pre-merge events
        still stamped with a prior tenant). Access requires current ownership.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            job_row = conn.execute(
                "SELECT id, tenant_id, status FROM jobs WHERE id = %s AND tenant_id = %s",
                (job_id, tenant_id),
            ).fetchone()
            if job_row is None:
                return AuditConsistencyReport(
                    job_id=job_id,
                    tenant_id=tenant_id,
                    ok=False,
                    job_status=None,
                    audit_final_status=None,
                    event_count=0,
                    gaps=(),
                    error="job not found for tenant",
                )
            # Full job history by job_id (not filtered by event.tenant_id).
            events = conn.execute(
                """
                SELECT seq, to_status, event_type, tenant_id
                FROM job_audit_events
                WHERE job_id = %s
                ORDER BY seq ASC
                """,
                (job_id,),
            ).fetchall()

        if not events:
            return AuditConsistencyReport(
                job_id=job_id,
                tenant_id=tenant_id,
                ok=False,
                job_status=job_row["status"],
                audit_final_status=None,
                event_count=0,
                gaps=(),
                error="no audit events for job",
            )

        gaps: list[int] = []
        expected = 1
        last_status: str | None = None
        for ev in events:
            seq = int(ev["seq"])
            if seq != expected:
                gaps.append(seq)
            expected = seq + 1
            if ev["to_status"] is not None:
                last_status = ev["to_status"]

        job_status = job_row["status"]
        ok = not gaps and last_status == job_status
        err = None
        if gaps:
            err = f"seq gaps near {gaps[:5]!r}"
        elif last_status != job_status:
            err = f"audit final status {last_status!r} != job status {job_status!r}"
        return AuditConsistencyReport(
            job_id=job_id,
            tenant_id=tenant_id,
            ok=ok,
            job_status=job_status,
            audit_final_status=last_status,
            event_count=len(events),
            gaps=tuple(gaps),
            error=err,
        )

    @staticmethod
    def _tenant_can_read_job_audit(
        conn: Any,
        *,
        job_id: UUID,
        tenant_id: str,
    ) -> bool:
        """True if *tenant_id* currently owns the job (post-merge target)."""
        row = conn.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
            (job_id, tenant_id),
        ).fetchone()
        return row is not None

    def verify_tenant_audit(
        self,
        tenant_id: str,
        *,
        limit: int = 10_000,
    ) -> list[AuditConsistencyReport]:
        """Verify audit consistency for up to *limit* jobs of a tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id FROM jobs
                WHERE tenant_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (tenant_id, limit),
            ).fetchall()
        return [self.verify_job_audit(r["id"], tenant_id=tenant_id) for r in rows]
