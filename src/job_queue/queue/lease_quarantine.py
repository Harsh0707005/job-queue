"""Quarantine jobs that keep dying across distinct workers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.models import JobStatus, WebhookEvent
from job_queue.queue._common import _JOB_COLUMNS


QUARANTINE_DEAD = "dead"
QUARANTINE_RELEASE = "release"
QUARANTINE_ACTIONS = frozenset({QUARANTINE_DEAD, QUARANTINE_RELEASE})


def normalize_quarantine_action(action: str | None) -> str:
    if action is None or str(action).strip() == "":
        return QUARANTINE_DEAD
    raw = str(action).strip().lower().replace("-", "_")
    if raw in ("dead", "dead_letter", "deadletter", "dlq"):
        return QUARANTINE_DEAD
    if raw in ("release", "quarantine", "park", "hold"):
        return QUARANTINE_RELEASE
    raise ValueError("quarantine_action must be 'dead' or 'release'")


def _failed_workers_plus(raw: Any, worker_id: str | None) -> list[str]:
    """Append this dying worker (duplicates count — same worker retry is a hop)."""
    out: list[str] = []
    for item in raw or []:
        s = str(item).strip()
        if s:
            out.append(s)
    wid = str(worker_id or "").strip()
    if wid:
        out.append(wid)
    return out


class LeaseQuarantineMixin:
    def _resolve_job_quarantine(
        self,
        conn: Any,
        *,
        tenant_id: str,
        max_worker_hops: int | None,
        quarantine_action: str | None,
    ) -> tuple[int | None, str]:
        hops = None if max_worker_hops is None else int(max_worker_hops)
        if hops is not None and hops < 1:
            raise ValueError("max_worker_hops must be >= 1 or None")
        action = (
            None
            if quarantine_action is None
            else normalize_quarantine_action(quarantine_action)
        )
        if hops is None or action is None:
            row = conn.execute(
                """
                SELECT default_max_worker_hops, default_quarantine_action
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if hops is None and row and row.get("default_max_worker_hops") is not None:
                hops = int(row["default_max_worker_hops"])
            if action is None and row and row.get("default_quarantine_action"):
                action = normalize_quarantine_action(row["default_quarantine_action"])
        if hops is None:
            return None, QUARANTINE_DEAD
        return hops, (action or QUARANTINE_DEAD)

    def _stamp_job_quarantine(
        self,
        conn: Any,
        *,
        job_id: Any,
        tenant_id: str,
        max_worker_hops: int | None = None,
        quarantine_action: str | None = None,
    ) -> None:
        hops, action = self._resolve_job_quarantine(
            conn,
            tenant_id=tenant_id,
            max_worker_hops=max_worker_hops,
            quarantine_action=quarantine_action,
        )
        conn.execute(
            """
            UPDATE jobs
            SET max_worker_hops = %s,
                quarantine_action = %s
            WHERE id = %s
            """,
            (hops, action, job_id),
        )

    def _quarantine_job_if_due(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        workers: list[str],
        worker_id: str | None,
        reason: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        """If distinct failing workers hit the hop cap, park or dead-letter.

        Returns the updated row when quarantined, else None.
        """
        limit = row.get("max_worker_hops")
        if limit is None or len(workers) < int(limit):
            return None
        action = normalize_quarantine_action(row.get("quarantine_action"))
        if action == QUARANTINE_RELEASE:
            new_status = JobStatus.QUARANTINED.value
            event = "quarantined"
            err = (
                f"quarantined after {len(workers)} worker hops "
                f"({', '.join(workers)}): {reason}"
            )
        else:
            new_status = JobStatus.DEAD.value
            event = "dead_lettered"
            err = (
                f"dead-lettered after {len(workers)} worker hops "
                f"({', '.join(workers)}): {reason}"
            )
        tenant_id = str(row["tenant_id"])
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                failed_workers = %s,
                last_error = %s,
                completed_at = CASE WHEN %s = %s THEN COALESCE(completed_at, %s)
                                    ELSE completed_at END,
                updated_at = %s
            WHERE id = %s
              AND tenant_id = %s
              AND status IN (%s, %s)
            RETURNING {_JOB_COLUMNS}
            """,
            (
                new_status,
                workers,
                err[:2000],
                new_status,
                JobStatus.DEAD.value,
                now,
                now,
                row["id"],
                tenant_id,
                JobStatus.LEASED.value,
                JobStatus.PENDING.value,
            ),
        ).fetchone()
        if updated is None:
            return None
        if hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type=event,
                from_status=row.get("status"),
                actor=worker_id,
                detail={
                    "reason": reason,
                    "failed_workers": workers,
                    "max_worker_hops": int(limit),
                    "quarantine_action": action,
                },
                now=now,
            )
        if hasattr(self, "_record_lease_end_usage") and row.get("status") == JobStatus.LEASED.value:
            extra = {"dead_letters": 1} if new_status == JobStatus.DEAD.value else {}
            self._record_lease_end_usage(conn, row, now=now, **extra)
        if new_status == JobStatus.DEAD.value:
            if hasattr(self, "_record_metric"):
                self._record_metric(
                    conn, tenant_id=tenant_id, metric="dead", count=1, now=now
                )
            job = self._job(updated)
            if hasattr(self, "_enqueue_job_webhooks"):
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_DEAD, now=now
                )
        # Both actions are terminal for dependents: a quarantined DAG parent
        # must not leave children pending, and a quarantined spawn child
        # must fail the parked parent join.
        if hasattr(self, "_cascade_cancel_dependents"):
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tenant_id,
                reason=err,
                now=now,
            )
        if updated.get("parent_job_id") is not None and hasattr(
            self, "_maybe_wake_parent_on_join_locked"
        ):
            self._maybe_wake_parent_on_join_locked(
                conn,
                parent_id=updated["parent_job_id"],
                tenant_id=tenant_id,
                now=now,
            )
        if hasattr(self, "_on_workflow_step_terminal"):
            self._on_workflow_step_terminal(
                conn, updated, success=False, error=err, now=now
            )
        return updated
