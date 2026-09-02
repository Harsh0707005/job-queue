"""Expire unfinished jobs past TTL / expires_at (cancel or dead-letter)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from job_queue.db import transaction
from job_queue.models import Job, JobStatus, WebhookEvent
from job_queue.queue._common import (
    _CANCELABLE_STATUSES,
    _JOB_COLUMNS,
    _as_utc,
    _utcnow,
)


EXPIRY_CANCEL = "cancel"
EXPIRY_DEAD = "dead"
EXPIRY_ACTIONS = frozenset({EXPIRY_CANCEL, EXPIRY_DEAD})

# Frozen jobs are an operator hold — TTL must not lift them.
_EXPIRE_STATUSES = tuple(
    s.value for s in _CANCELABLE_STATUSES if s is not JobStatus.FROZEN
)

_TTL_MSG = "job ttl expired"


def normalize_expiry_action(action: str | None) -> str:
    if action is None or str(action).strip() == "":
        return EXPIRY_DEAD
    raw = str(action).strip().lower().replace("-", "_")
    if raw in ("cancel", "cancelled", "canceled"):
        return EXPIRY_CANCEL
    if raw in ("dead", "dead_letter", "deadletter", "dlq"):
        return EXPIRY_DEAD
    raise ValueError("expiry_action must be 'cancel' or 'dead'")


class JobsExpireMixin:
    def process_expired_jobs(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 200,
        max_passes: int = 16,
    ) -> dict[str, int]:
        """Cancel or dead-letter unfinished jobs past ``expires_at``.

        Safe under load (``FOR UPDATE SKIP LOCKED``). Lease, cleaner, and
        explicit callers invoke this so parked / unmatched work cannot sit
        forever when a TTL is set.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if max_passes < 1:
            raise ValueError("max_passes must be >= 1")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")
        totals = {"cancelled": 0, "dead": 0}
        for _ in range(int(max_passes)):
            now = _utcnow()
            with transaction(self._pool) as conn:
                batch = self._expire_unfinished_jobs_locked(
                    conn, now=now, tenant_id=tenant_id, limit=limit
                )
            for job in batch:
                if job.status == JobStatus.CANCELLED:
                    totals["cancelled"] += 1
                elif job.status == JobStatus.DEAD:
                    totals["dead"] += 1
            if not batch:
                break
        return totals

    def _resolve_job_expiry(
        self,
        conn: Any,
        *,
        tenant_id: str,
        now: datetime,
        ttl_seconds: float | int | None,
        expires_at: datetime | None,
        expiry_action: str | None,
    ) -> tuple[datetime | None, str]:
        if ttl_seconds is not None and expires_at is not None:
            raise ValueError("pass only one of ttl_seconds or expires_at")
        if ttl_seconds is not None:
            if float(ttl_seconds) < 1:
                raise ValueError("ttl_seconds must be >= 1")
        settings = conn.execute(
            """
            SELECT default_job_ttl_seconds, default_expiry_action
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        default_ttl = None
        default_action = None
        if settings is not None:
            if settings.get("default_job_ttl_seconds") is not None:
                default_ttl = int(settings["default_job_ttl_seconds"])
            if settings.get("default_expiry_action"):
                default_action = str(settings["default_expiry_action"])
        if expires_at is not None:
            exp = _as_utc(expires_at)
        elif ttl_seconds is not None:
            exp = now + timedelta(seconds=float(ttl_seconds))
        elif default_ttl is not None:
            exp = now + timedelta(seconds=int(default_ttl))
        else:
            exp = None
        action = normalize_expiry_action(expiry_action or default_action)
        return exp, action

    def _replay_refresh_expires_at(
        self,
        conn: Any,
        *,
        row: dict[str, Any],
        tenant_id: str,
        now: datetime,
        run_at: datetime,
    ) -> datetime | None:
        """Keep a still-valid TTL; give another full window if it already elapsed.

        Replay must not restore ``pending`` with ``expires_at`` in the past or
        the cleaner / lease path immediately dead-letters the set again.
        """
        raw = row.get("expires_at")
        if raw is None:
            return None
        exp = _as_utc(raw)
        if exp > now and run_at < exp:
            return exp
        created = row.get("created_at")
        created_at = _as_utc(created) if created is not None else now
        window = (exp - created_at).total_seconds()
        if window < 1:
            refreshed, _ = self._resolve_job_expiry(
                conn,
                tenant_id=tenant_id,
                now=now,
                ttl_seconds=None,
                expires_at=None,
                expiry_action=None,
            )
            if refreshed is None:
                window = 1.0
                new_exp = now + timedelta(seconds=window)
            else:
                new_exp = refreshed
        else:
            new_exp = now + timedelta(seconds=window)
        if run_at >= new_exp:
            new_exp = run_at + timedelta(seconds=max(window, 1.0))
        return new_exp

    def _expire_unfinished_jobs_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> list[Job]:
        statuses = list(_EXPIRE_STATUSES)
        if tenant_id is not None:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = ANY(%s)
                  AND expires_at IS NOT NULL
                  AND expires_at < %s
                  {self._sql_skip_relocating("jobs")}
                ORDER BY expires_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (tenant_id, statuses, now, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = ANY(%s)
                  AND expires_at IS NOT NULL
                  AND expires_at < %s
                  {self._sql_skip_relocating("jobs")}
                ORDER BY expires_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (statuses, now, int(limit)),
            ).fetchall()
        out: list[Job] = []
        for row in rows:
            job = self._expire_one_job_locked(conn, row, now=now)
            if job is not None:
                out.append(job)
        return out

    def _expire_one_job_locked(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> Job | None:
        from_status = str(row.get("status") or "")
        if from_status not in _EXPIRE_STATUSES:
            return None
        action = normalize_expiry_action(row.get("expiry_action"))
        new_status = (
            JobStatus.CANCELLED.value
            if action == EXPIRY_CANCEL
            else JobStatus.DEAD.value
        )
        skills = list(row.get("required_skills") or [])
        err = _TTL_MSG
        if skills:
            err = f"{_TTL_MSG}: waiting for skills {','.join(skills)}"
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                last_error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s
              AND status = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                new_status,
                err[:2000],
                now,
                now,
                row["id"],
                from_status,
            ),
        ).fetchone()
        if updated is None:
            return None
        event = "cancelled" if action == EXPIRY_CANCEL else "dead_lettered"
        if hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type=event,
                from_status=from_status,
                actor="system",
                detail={"reason": "ttl", "expiry_action": action},
                now=now,
            )
        if hasattr(self, "_record_lease_end_usage"):
            extra = (
                {"cancels": 1}
                if action == EXPIRY_CANCEL
                else {}
            )
            self._record_lease_end_usage(conn, row, now=now, **extra)
        if action == EXPIRY_DEAD and hasattr(self, "_record_metric"):
            self._record_metric(
                conn,
                tenant_id=str(updated["tenant_id"]),
                metric="dead",
                count=1,
                now=now,
            )
        job = self._job(updated)
        if action == EXPIRY_DEAD and hasattr(self, "_enqueue_job_webhooks"):
            self._enqueue_job_webhooks(
                conn, job=job, event=WebhookEvent.JOB_DEAD, now=now
            )
        if hasattr(self, "_cascade_cancel_dependents"):
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=str(updated["tenant_id"]),
                reason=f"parent job {row['id']} expired ({action})",
                now=now,
            )
        if updated.get("parent_job_id") is not None and hasattr(
            self, "_maybe_wake_parent_on_join_locked"
        ):
            self._maybe_wake_parent_on_join_locked(
                conn,
                parent_id=updated["parent_job_id"],
                tenant_id=updated["tenant_id"],
                now=now,
            )
        if hasattr(self, "_on_workflow_step_terminal"):
            # Cancel *or* dead — a cancelled step job must still fail the
            # saga so the run cannot sit RUNNING forever.
            self._on_workflow_step_terminal(
                conn,
                updated,
                success=False,
                error=err,
                now=now,
            )
        if hasattr(self, "_maybe_finalize_offboard"):
            self._maybe_finalize_offboard(
                conn, tenant_id=str(updated["tenant_id"]), now=now
            )
        return job
