"""JobQueue schedule CRUD: create/get/list/pause/resume/delete."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    Schedule,
)
from job_queue.queue._common import (
    _SCHEDULE_COLUMNS,
    ScheduleError,
    _as_utc,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
)

MISFIRE_SKIP = "skip"
MISFIRE_CATCH_UP = "catch_up"
MISFIRE_CATCH_UP_ONE = "catch_up_one"
MISFIRE_POLICIES = frozenset(
    {MISFIRE_SKIP, MISFIRE_CATCH_UP, MISFIRE_CATCH_UP_ONE}
)
_CATCH_UP_MAX_DEFAULT = 8
_CATCH_UP_MAX_LIMIT = 100
_MAX_MISFIRE_WALK = 10_000


def normalize_misfire_policy(raw: str | None) -> str:
    if raw is None or str(raw).strip() == "":
        return MISFIRE_CATCH_UP_ONE
    key = str(raw).strip().lower().replace("-", "_")
    if key in ("skip", "miss", "skip_missed", "skip_misses"):
        return MISFIRE_SKIP
    if key in ("catch_up", "catchup", "catch_up_all"):
        return MISFIRE_CATCH_UP
    if key in ("catch_up_one", "one", "latest", "catchup_one"):
        return MISFIRE_CATCH_UP_ONE
    raise ValueError(
        "misfire_policy must be 'skip', 'catch_up', or 'catch_up_one'"
    )


def normalize_catch_up_max(raw: int | None) -> int:
    if raw is None:
        return _CATCH_UP_MAX_DEFAULT
    n = int(raw)
    if n < 1 or n > _CATCH_UP_MAX_LIMIT:
        raise ValueError(
            f"catch_up_max must be between 1 and {_CATCH_UP_MAX_LIMIT}"
        )
    return n


_MAX_SCHEDULE_DAG_JOBS = 200


def normalize_schedule_dag_jobs(
    raw: Sequence[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Validate a schedule DAG template. None / empty → no DAG (single job)."""
    if raw is None:
        return None
    if not isinstance(raw, (list, tuple)):
        raise ValueError("dag_jobs must be a list")
    if len(raw) == 0:
        return None
    if len(raw) > _MAX_SCHEDULE_DAG_JOBS:
        raise ValueError(
            f"dag_jobs must have at most {_MAX_SCHEDULE_DAG_JOBS} nodes"
        )
    seen: set[str] = set()
    nodes: list[dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"dag_jobs[{i}] must be a dict")
        ref = str(item.get("ref") or "").strip()
        if not ref:
            raise ValueError(f"dag_jobs[{i}].ref is required")
        if ref in seen:
            raise ValueError(f"duplicate dag ref {ref!r}")
        seen.add(ref)
        payload = item.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError(f"dag_jobs[{i}].payload must be a dict")
        deps = item.get("depends_on") or []
        if isinstance(deps, str):
            deps = [deps]
        if not isinstance(deps, (list, tuple)):
            raise ValueError(f"dag_jobs[{i}].depends_on must be a list")
        node = dict(item)
        node["ref"] = ref
        node["depends_on"] = [str(d).strip() for d in deps if str(d).strip()]
        nodes.append(node)
    refs = {n["ref"] for n in nodes}
    incoming = {n["ref"]: 0 for n in nodes}
    adj: dict[str, list[str]] = {n["ref"]: [] for n in nodes}
    for n in nodes:
        for dep in n["depends_on"]:
            if dep not in refs:
                raise ValueError(
                    f"dag_jobs ref {n['ref']!r} depends_on unknown ref {dep!r}"
                )
            adj[dep].append(n["ref"])
            incoming[n["ref"]] += 1
    ready = [r for r, c in incoming.items() if c == 0]
    seen_n = 0
    while ready:
        node = ready.pop()
        seen_n += 1
        for child in adj[node]:
            incoming[child] -= 1
            if incoming[child] == 0:
                ready.append(child)
    if seen_n != len(nodes):
        raise ValueError("dag_jobs contains a cycle")
    return nodes


class SchedulesCrudMixin:
    def create_schedule(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        cron: str | None = None,
        interval_seconds: int | None = None,
        timezone: str = "UTC",
        queue_name: str = "default",
        name: str | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        start_at: datetime | None = None,
        enabled: bool = True,
        schedule_id: UUID | None = None,
        labels: Sequence[str] | str | None = None,
        required_skills: Sequence[str] | str | None = None,
        misfire_policy: str | None = None,
        catch_up_max: int | None = None,
        dag_jobs: Sequence[dict[str, Any]] | None = None,
    ) -> Schedule:
        """Register a recurring schedule (cron and/or fixed interval).

        Prefer *cron* (standard crontab). Examples::

            \"0 9 * * 1-5\"     # weekdays at 09:00 in *timezone*
            \"0 0 1 * *\"       # midnight on the 1st of each month
            \"*/5 * * * * *\"   # every 5 seconds (6-field)

        *interval_seconds* is still supported for fixed delays. Provide at
        least one of *cron* or *interval_seconds*.

        Call ``process_due_schedules()`` (or ``ScheduleRunner``) to materialize
        due ticks. Schedule fires **bypass** tenant pending caps and enqueue
        rate limits.

        Parameters
        ----------
        start_at:
            Base time for the first ``next_run_at``. For cron, the first fire
            is the next occurrence **after** this instant (default: now).
            For interval-only, defaults to now (due on the next tick).
        name:
            Optional unique name per tenant for lookup / idempotent registration.
        timezone:
            IANA timezone for cron evaluation (default ``UTC``).
        misfire_policy:
            ``skip`` jumps to the next future tick and records misses.
            ``catch_up_one`` (default) enqueues one pickup then jumps.
            ``catch_up`` drains up to ``catch_up_max`` missed ticks one
            at a time after the prior job finishes.
        catch_up_max:
            Bound for ``catch_up`` (1–100, default 8). Extra missed
            ticks are counted as skipped, not enqueued.
        dag_jobs:
            Optional DAG template. When set, each tick enqueues this
            whole graph in one transaction instead of a single job.
        """
        from job_queue.cronutil import next_cron_fire, validate_cron, validate_timezone

        if not tenant_id:
            raise ValueError("tenant_id is required")
        if cron is None and interval_seconds is None:
            raise ValueError("provide cron and/or interval_seconds")
        if interval_seconds is not None and interval_seconds < 1:
            raise ValueError("interval_seconds must be >= 1 or None")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if name is not None and name == "":
            raise ValueError("name must be non-empty when provided")

        cron_expr = validate_cron(cron) if cron is not None else None
        tz_name = validate_timezone(timezone)
        label_list = _normalize_labels(labels)
        skill_list = _normalize_skills(required_skills)
        policy = normalize_misfire_policy(misfire_policy)
        catch_max = normalize_catch_up_max(catch_up_max)
        dag_list = normalize_schedule_dag_jobs(dag_jobs)
        now = _utcnow()
        base = _as_utc(start_at) if start_at is not None else now
        if cron_expr is not None:
            # First fire = next occurrence after base (not base itself unless
            # it already matches a boundary and caller wants that — croniter
            # get_next is strict-after).
            first_run = next_cron_fire(cron_expr, base, timezone_name=tz_name)
        else:
            first_run = base
        sid = schedule_id or uuid4()

        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; "
                    "schedule registration is rejected"
                )
            try:
                sealed, payload_key_id = self._seal_schedule_payload(
                    conn,
                    tenant_id=tenant_id,
                    schedule_id=sid,
                    payload=payload,
                )
                row = conn.execute(
                    f"""
                    INSERT INTO schedules (
                        id, tenant_id, name, queue_name, payload,
                        interval_seconds, cron_expr, timezone,
                        priority, max_attempts,
                        visibility_timeout_seconds, enabled, next_run_at,
                        labels, created_at, updated_at, payload_key_id,
                        required_skills,
                        misfire_policy, catch_up_max, dag_jobs
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        sid,
                        tenant_id,
                        name,
                        queue_name,
                        Jsonb(sealed),
                        interval_seconds,
                        cron_expr,
                        tz_name,
                        priority,
                        max_attempts,
                        visibility_timeout_seconds,
                        enabled,
                        first_run,
                        label_list,
                        now,
                        now,
                        payload_key_id,
                        skill_list,
                        policy,
                        catch_max,
                        Jsonb(dag_list) if dag_list else None,
                    ),
                ).fetchone()
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule name {name!r} already exists for tenant {tenant_id!r}"
                ) from exc
            assert row is not None
            return self._schedule(row)

    def get_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule | None:
        """Fetch a schedule by id for *tenant_id* (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return self._schedule(row) if row else None

    def list_schedules(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Schedule]:
        """List schedules for a tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if enabled_only:
            clauses.append("enabled = TRUE")
        where = " AND ".join(clauses)
        params.extend([limit, offset])
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [self._schedule(r) for r in rows]

    def pause_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule:
        """Stop a schedule from firing (keeps the row; use resume to re-enable)."""
        return self._set_schedule_enabled(
            schedule_id, tenant_id=tenant_id, enabled=False
        )

    def resume_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        next_run_at: datetime | None = None,
    ) -> Schedule:
        """Re-enable a paused schedule.

        If *next_run_at* is omitted, the next fire is set to now so it is due
        on the following ``process_due_schedules`` call.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        run_at = _as_utc(next_run_at) if next_run_at is not None else now
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = TRUE,
                    next_run_at = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (run_at, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return self._schedule(row)

    def delete_schedule(self, schedule_id: UUID, *, tenant_id: str) -> bool:
        """Permanently remove a schedule. Returns True if a row was deleted."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM schedules
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return row is not None

    def _set_schedule_enabled(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> Schedule:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (enabled, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return self._schedule(row)

    def set_schedule_misfire(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        misfire_policy: str | None = None,
        catch_up_max: int | None = None,
    ) -> Schedule:
        """Change miss / catch-up policy on an existing schedule."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if misfire_policy is None and catch_up_max is None:
            raise ValueError("pass misfire_policy and/or catch_up_max")
        policy = (
            normalize_misfire_policy(misfire_policy)
            if misfire_policy is not None
            else None
        )
        catch_max = (
            normalize_catch_up_max(catch_up_max)
            if catch_up_max is not None
            else None
        )
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET misfire_policy = COALESCE(%s, misfire_policy),
                    catch_up_max = COALESCE(%s, catch_up_max),
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (policy, catch_max, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return self._schedule(row)

    @staticmethod
    def _schedule_tick_after(sched_row: dict[str, Any], after: datetime) -> datetime:
        """Next occurrence strictly after *after*."""
        cron = sched_row.get("cron_expr")
        if cron:
            from job_queue.cronutil import next_cron_fire

            tz = str(sched_row.get("timezone") or "UTC")
            return next_cron_fire(str(cron), after, timezone_name=tz)
        interval = sched_row.get("interval_seconds")
        if interval is None:
            raise ValueError("schedule has neither cron_expr nor interval_seconds")
        return after + timedelta(seconds=int(interval))

    def _misfire_walk(
        self,
        sched_row: dict[str, Any],
        *,
        now: datetime,
        fire_cap: int,
    ) -> tuple[list[datetime], int, datetime]:
        """Walk missed ticks from ``next_run_at``.

        Returns ``(slots_to_consider, skipped_count, first_future)``.
        ``slots_to_consider`` has at most *fire_cap* past-due times.
        """
        t = _as_utc(sched_row["next_run_at"])
        slots: list[datetime] = []
        skipped = 0
        for _ in range(_MAX_MISFIRE_WALK):
            if t > now:
                return slots, skipped, t
            if fire_cap > 0 and len(slots) < fire_cap:
                slots.append(t)
            else:
                skipped += 1
            t = self._schedule_tick_after(sched_row, t)
        future = self._schedule_next_run(sched_row, now=now)
        return slots, skipped + 1, future

    @staticmethod
    def _schedule_next_run(sched_row: dict[str, Any], *, now: datetime) -> datetime:
        """Compute the next fire time after *now* for a schedule row."""
        cron = sched_row.get("cron_expr")
        if cron:
            from job_queue.cronutil import next_cron_fire

            tz = str(sched_row.get("timezone") or "UTC")
            return next_cron_fire(str(cron), now, timezone_name=tz)
        interval = sched_row.get("interval_seconds")
        if interval is None:
            raise ValueError("schedule has neither cron_expr nor interval_seconds")
        return now + timedelta(seconds=int(interval))
