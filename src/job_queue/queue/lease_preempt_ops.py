"""JobQueue preemption operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.models import (
    JobStatus,
    PreemptResult,
)
from job_queue.queue._common import (
    _DEPS_SATISFIED_SQL,
    _JOB_COLUMNS,
    _MAX_PREEMPTS_PER_JOB,
)


class LeasePreemptOpsMixin:
    def _preempt_leased_job(
        self,
        conn: Any,
        victim: dict[str, Any],
        *,
        now: datetime,
        reason: str,
    ) -> dict[str, Any] | None:
        """Park a leased job as pending; clear fence; keep checkpoint.

        Does **not** burn an attempt. Returns updated row or None if lost race.
        """
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                available_at = %s,
                preempt_count = preempt_count + 1,
                last_preempted_at = %s,
                last_error = %s,
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND lease_token IS NOT NULL
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.PENDING.value,
                now,
                now,
                reason[:500],
                now,
                victim["id"],
                JobStatus.LEASED.value,
            ),
        ).fetchone()
        if updated is not None:
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="preempted",
                from_status=JobStatus.LEASED.value,
                actor="system",
                detail={"reason": reason[:500]},
                now=now,
            )
        return updated

    def _try_preempt_for_tenant(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        now: datetime,
        urgent_hint: dict[str, Any] | None = None,
    ) -> PreemptResult | None | bool:
        """If an urgent pending job needs a slot, preempt one less-urgent lease.

        Returns :class:`PreemptResult` on success, ``None``/``False`` otherwise.
        When used as a bool (lease paths), truthy means a slot was freed.
        """
        conc = self._read_concurrency_limit(conn, tenant_id)
        if conc is None:
            return None
        leased_n = self._count_leased(conn, tenant_id)
        if leased_n < conc:
            return None

        # Claimable deadline pending ordered by urgency (low slack first).
        pending_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
              AND available_at <= %s
              AND deadline_at IS NOT NULL
              AND {_DEPS_SATISFIED_SQL}
            ORDER BY priority DESC,
              (EXTRACT(EPOCH FROM (deadline_at - %s))
                - COALESCE(estimated_duration_seconds, 60)) ASC,
              created_at ASC
            LIMIT 16
            """,
            (
                tenant_id,
                queue_name,
                JobStatus.PENDING.value,
                now,
                now,
            ),
        ).fetchall()
        if not pending_rows and urgent_hint is not None:
            if (
                urgent_hint.get("tenant_id") == tenant_id
                and urgent_hint.get("status") == JobStatus.PENDING.value
                and urgent_hint.get("deadline_at") is not None
            ):
                pending_rows = [urgent_hint]
        if not pending_rows:
            return None

        leased_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
            FOR UPDATE
            """,
            (tenant_id, queue_name, JobStatus.LEASED.value),
        ).fetchall()
        if not leased_rows:
            return None

        # Score pending candidates.
        scored_pending: list[tuple[tuple, dict[str, Any], float]] = []
        for prow in pending_rows:
            slack = self._slack_seconds(conn, prow, now=now)
            if slack == float("inf"):
                continue
            scored_pending.append(
                (self._urgency_tuple(conn, prow, now=now), prow, slack)
            )
        if not scored_pending:
            return None
        scored_pending.sort(key=lambda x: x[0])
        urgent_key, urgent, urgent_slack = scored_pending[0]

        # Victims: preemptible, under max preempts, strictly less urgent.
        candidates: list[tuple[tuple, dict[str, Any], float]] = []
        for vrow in leased_rows:
            if not bool(vrow.get("preemptible", True)):
                continue
            if int(vrow.get("preempt_count") or 0) >= _MAX_PREEMPTS_PER_JOB:
                continue
            v_key = self._urgency_tuple(conn, vrow, now=now)
            # Strictly less urgent than the pending job.
            if v_key <= urgent_key:
                continue
            rem = self._remaining_estimate_seconds(conn, vrow, now=now)
            candidates.append((v_key, vrow, rem if rem > 0 else 0.0))

        if not candidates:
            return None

        # Can we wait for the soonest less-urgent holder to finish?
        min_wait = min(c[2] for c in candidates)
        # Only preempt if waiting would risk the deadline (slack < wait).
        # Small buffer (0.5s) avoids thrash on equal estimates.
        if urgent_slack >= min_wait - 0.5:
            return None

        # Prefer victim that is least urgent, then longest remaining (free
        # more useful work later), then lowest preempt_count (anti thrash).
        def victim_rank(item: tuple) -> tuple:
            v_key, vrow, rem = item
            return (
                v_key[0],
                -v_key[1] if v_key[1] != float("inf") else -1e18,
                v_key[2],
                -rem,
                int(vrow.get("preempt_count") or 0),
            )

        # Actually sort by least urgent first: largest urgency tuple.
        candidates.sort(key=lambda x: x[0], reverse=True)
        # Among equal urgency, pick higher remaining then lower preempt_count.
        best = candidates[0]
        for c in candidates[1:]:
            if c[0] == best[0]:
                if c[2] > best[2] or (
                    c[2] == best[2]
                    and int(c[1].get("preempt_count") or 0)
                    < int(best[1].get("preempt_count") or 0)
                ):
                    best = c
            else:
                break
        _, victim, _ = best
        victim_slack = self._slack_seconds(conn, victim, now=now)

        updated = self._preempt_leased_job(
            conn,
            victim,
            now=now,
            reason=(
                f"preempted for deadline job {urgent['id']} "
                f"(urgent_slack={urgent_slack:.1f}s)"
            ),
        )
        if updated is None:
            return None
        return PreemptResult(
            tenant_id=tenant_id,
            queue_name=queue_name,
            urgent_job_id=urgent["id"],
            victim_job_id=victim["id"],
            urgent_slack_seconds=float(urgent_slack),
            victim_slack_seconds=(
                float(victim_slack) if victim_slack != float("inf") else float("inf")
            ),
        )

    def _preempt_blocked_urgent(
        self,
        conn: Any,
        *,
        queue_name: str,
        now: datetime,
        limit: int = 8,
    ) -> int:
        """Run preemption for tenants with deadline pending blocked by caps."""
        rows = conn.execute(
            """
            SELECT DISTINCT j.tenant_id
            FROM jobs j
            JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
            WHERE j.queue_name = %s
              AND j.status = %s
              AND j.deadline_at IS NOT NULL
              AND j.available_at <= %s
              AND ts.max_concurrent_leases IS NOT NULL
              AND ts.paused = FALSE
            LIMIT %s
            """,
            (
                queue_name,
                JobStatus.PENDING.value,
                now,
                max(limit * 2, 8),
            ),
        ).fetchall()
        n = 0
        for r in rows:
            if n >= limit:
                break
            tid = r["tenant_id"]
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tid,),
            )
            if self._try_preempt_for_tenant(
                conn, tenant_id=tid, queue_name=queue_name, now=now
            ):
                n += 1
        return n
