"""JobQueue audit read/verify domain: list and consistency checks."""

from __future__ import annotations

import json

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.crypto import audit_event_aad, is_envelope
from job_queue.models import (
    AuditConsistencyReport,
    JobAuditEvent,
)


class AuditReadMixin:
    def _open_audit_detail(
        self,
        detail: Any,
        *,
        event_id: Any,
        event_tenant: str,
        reader_tenant: str | None,
        conn: Any | None,
    ) -> dict[str, Any]:
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except json.JSONDecodeError:
                return {}
        if not isinstance(detail, dict):
            return {"value": detail}
        if not is_envelope(detail) or not hasattr(self, "_open_dict"):
            return detail
        tried: set[str] = set()
        for tid in (reader_tenant, event_tenant):
            if not tid or tid in tried:
                continue
            tried.add(tid)
            try:
                opened = self._open_dict(
                    detail,
                    tenant_id=tid,
                    aad=audit_event_aad(event_id),
                    conn=conn,
                )
            except Exception:  # noqa: BLE001
                continue
            if isinstance(opened, dict):
                return opened
        return {}

    def _row_to_audit(
        self,
        row: dict[str, Any],
        *,
        conn: Any | None = None,
        reader_tenant: str | None = None,
    ) -> JobAuditEvent:
        detail = self._open_audit_detail(
            row.get("detail") or {},
            event_id=row["event_id"],
            event_tenant=str(row["tenant_id"]),
            reader_tenant=reader_tenant,
            conn=conn,
        )
        tenant_seq = row.get("tenant_seq")
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
            tenant_seq=(None if tenant_seq is None else int(tenant_seq)),
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
                    from_status, to_status, actor, detail, created_at,
                    tenant_seq, payload_key_id
                FROM job_audit_events
                WHERE job_id = %s
                  AND seq > %s
                ORDER BY seq ASC
                LIMIT %s
                """,
                (job_id, after_seq, limit),
            ).fetchall()
            return [
                self._row_to_audit(r, conn=conn, reader_tenant=tenant_id)
                for r in rows
            ]

    def list_tenant_audit(
        self,
        tenant_id: str,
        *,
        after_seq: int | None = None,
        after_created_at: datetime | None = None,
        after_event_id: UUID | None = None,
        limit: int = 500,
    ) -> list[JobAuditEvent]:
        """Tenant-wide audit stream.

        *after_seq* is the durable per-tenant cursor (``tenant_seq``). It
        returns only rows written under *tenant_id* so consumers cannot
        see another tenant's log. Omit it for the older time-ordered
        catch-up that also includes events of jobs this tenant now owns
        (merge).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if after_seq is not None and after_seq < 0:
            raise ValueError("after_seq must be >= 0")
        if after_seq is not None:
            return self.list_tenant_events(
                tenant_id, after_seq=after_seq, limit=limit
            )
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
                        e.from_status, e.to_status, e.actor, e.detail,
                        e.created_at, e.tenant_seq, e.payload_key_id
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
                        e.from_status, e.to_status, e.actor, e.detail,
                        e.created_at, e.tenant_seq, e.payload_key_id
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
            return [
                self._row_to_audit(r, conn=conn, reader_tenant=tenant_id)
                for r in rows
            ]

    def list_tenant_events(
        self,
        tenant_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> list[JobAuditEvent]:
        """Follow this tenant's sealed event log from *after_seq*.

        ``tenant_seq`` is gap-free for writes under *tenant_id*. Other
        tenants' rows are never returned.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if after_seq < 0:
            raise ValueError("after_seq must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_id, tenant_id, job_id, seq, event_type,
                    from_status, to_status, actor, detail, created_at,
                    tenant_seq, payload_key_id
                FROM job_audit_events
                WHERE tenant_id = %s
                  AND tenant_seq > %s
                ORDER BY tenant_seq ASC
                LIMIT %s
                """,
                (tenant_id, int(after_seq), limit),
            ).fetchall()
            return [
                self._row_to_audit(r, conn=conn, reader_tenant=tenant_id)
                for r in rows
            ]

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
