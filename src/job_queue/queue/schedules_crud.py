"""JobQueue schedule CRUD: create/get/list/pause/resume/delete."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    Schedule,
)
from job_queue.queue._common import (
    _SCHED_MUTEX_LOCK_CLASS,
    _SCHEDULE_COLUMNS,
    _SCHEDULE_INFLIGHT_STATUSES,
    _SCHEDULE_TICK_FAILED_STATUSES,
    _UNSET,
    ScheduleError,
    _as_utc,
    _dag_jobs_from_row,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
    _workflow_from_row,
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
_MAX_MUTEX_GROUP_LEN = 128
_MAX_WAITS_ON = 32
_MAX_JOINS_ON = 32


def normalize_waits_on(
    raw: Sequence[UUID | str] | UUID | str | None,
) -> list[UUID]:
    """Same-tenant upstream schedule ids. Empty / None → []."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, (UUID, str)):
        items: Sequence[UUID | str] = [raw]
    else:
        items = raw
    out: list[UUID] = []
    seen: set[UUID] = set()
    for item in items:
        if item is None or item == "":
            continue
        uid = item if isinstance(item, UUID) else UUID(str(item))
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    if len(out) > _MAX_WAITS_ON:
        raise ValueError(f"waits_on can list at most {_MAX_WAITS_ON} schedules")
    return out


def normalize_joins_on(
    raw: Sequence[UUID | str] | UUID | str | None,
) -> list[UUID]:
    """Same-tenant waiter schedule ids a join waits for. Empty / None → []."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, (UUID, str)):
        items: Sequence[UUID | str] = [raw]
    else:
        items = raw
    out: list[UUID] = []
    seen: set[UUID] = set()
    for item in items:
        if item is None or item == "":
            continue
        uid = item if isinstance(item, UUID) else UUID(str(item))
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    if len(out) > _MAX_JOINS_ON:
        raise ValueError(f"joins_on can list at most {_MAX_JOINS_ON} schedules")
    return out


def normalize_mutex_group(raw: str | None) -> str | None:
    """Empty / None → ungrouped. Same tenant + name share one mutex."""
    if raw is None:
        return None
    name = str(raw).strip()
    if name == "":
        return None
    if len(name) > _MAX_MUTEX_GROUP_LEN:
        raise ValueError(
            f"mutex_group longer than {_MAX_MUTEX_GROUP_LEN} characters"
        )
    return name


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


def normalize_schedule_workflow(
    raw: Any,
) -> dict[str, Any] | None:
    """Validate a schedule workflow template. None / empty → no workflow."""
    if raw is None or raw == {} or raw == ():
        return None
    if not isinstance(raw, dict):
        raise ValueError("workflow must be a dict")
    steps = raw.get("steps")
    if not steps or not isinstance(steps, list):
        raise ValueError("workflow.steps must be a non-empty list")
    from job_queue.queue.workflows import WorkflowsMixin

    WorkflowsMixin._expand_and_normalize_steps(steps)
    out: dict[str, Any] = {"steps": [dict(s) if isinstance(s, dict) else s for s in steps]}
    name = raw.get("name")
    if name is not None and str(name).strip():
        out["name"] = str(name).strip()
    return out


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
        mutex_group: str | None = None,
        waits_on: Sequence[UUID | str] | UUID | str | None = None,
        joins_on: Sequence[UUID | str] | UUID | str | None = None,
        workflow: dict[str, Any] | None = None,
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
        workflow:
            Optional multi-step saga template ``{"steps": [...]}``.
            Each tick starts one workflow run. Mutually exclusive with
            ``dag_jobs``. Step jobs inherit ``schedule_id`` and
            ``tick_iteration``.
        mutex_group:
            Optional per-tenant exclusive group name. At most one
            member may have a tick or DAG in flight. Missed ticks
            while the group is busy follow ``misfire_policy``.
        waits_on:
            Upstream schedule ids on this tenant. This schedule will
            not fire while any of those has a live tick, is paused, or
            has a paused / inflight mutex-group peer. Missed ticks
            while waiting follow ``misfire_policy``.
        joins_on:
            Waiter schedule ids on this tenant. This schedule fires
            one tick per generation after every waiter has completed
            that same iteration. A paused waiter or one stuck behind
            a mutex peer keeps the join blocked. Missed ticks follow
            ``misfire_policy``.
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
        wf = normalize_schedule_workflow(workflow)
        if dag_list and wf:
            raise ValueError("workflow is mutually exclusive with dag_jobs")
        group = normalize_mutex_group(mutex_group)
        wait_ids = normalize_waits_on(waits_on)
        join_ids = normalize_joins_on(joins_on)
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
            self._assert_waits_on_ok(
                conn, tenant_id=tenant_id, schedule_id=sid, waits_on=wait_ids
            )
            self._assert_joins_on_ok(
                conn, tenant_id=tenant_id, schedule_id=sid, joins_on=join_ids
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
                        misfire_policy, catch_up_max, dag_jobs, workflow,
                        mutex_group, waits_on, joins_on
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
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
                        Jsonb(wf) if wf else None,
                        group,
                        wait_ids,
                        join_ids,
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
        """Stop this schedule from enqueueing. Other schedules on the tenant keep ticking.

        In-flight jobs (or a whole DAG tick) finish. ``next_run_at`` is left
        as-is so resume can apply misfire policy instead of snapping to now.
        """
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

        Overdue ``next_run_at`` is left in the past so misfire policy
        (skip / catch_up_one / catch_up), blackout, inflight overlap, and
        spend caps run on the next ``process_due_schedules``. Pass
        *next_run_at* only to override that. Does not pause or resume the tenant.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is offboarding; resume is refused"
                )
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            run_at = (
                _as_utc(next_run_at)
                if next_run_at is not None
                else row["next_run_at"]
            )
            updated = conn.execute(
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
            assert updated is not None
            return self._schedule(updated)

    def delete_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
    ) -> bool:
        """Permanently remove a schedule.

        Returns True if a row was deleted, False if it was already gone.

        Fails closed with ``ScheduleError`` when any job for this schedule
        is still in flight (pending, leased, awaiting approval / children
        / skills, or frozen). Completed, cancelled, and dead-letter jobs
        keep their ``schedule_id`` as history — there is no FK, so delete
        does not cascade into the job table. Schedule blackouts CASCADE.
        Other schedules on the tenant are untouched.

        Serializes with ``process_due_schedules`` and ``fire_schedule``
        via ``FOR UPDATE`` on the schedule row: a tick cannot enqueue
        after delete has claimed the row, and delete cannot drop a row
        a tick is currently materializing.

        Set *wait* to disable the schedule (so no new tick can start)
        and poll until in-flight work finishes, then delete. On timeout
        the schedule stays disabled and ``ScheduleError`` is raised.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        deadline: float | None = None
        if wait:
            timeout = (
                30.0
                if wait_timeout_seconds is None
                else float(wait_timeout_seconds)
            )
            if timeout < 0:
                raise ValueError("wait_timeout_seconds must be >= 0")
            deadline = time.monotonic() + timeout

        while True:
            with transaction(self._pool) as conn:
                row = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE id = %s AND tenant_id = %s
                    FOR UPDATE
                    """,
                    (schedule_id, tenant_id),
                ).fetchone()
                if row is None:
                    return False
                active = conn.execute(
                    """
                    SELECT id, status
                    FROM jobs
                    WHERE schedule_id = %s
                      AND status = ANY(%s)
                    LIMIT 1
                    """,
                    (schedule_id, list(_SCHEDULE_INFLIGHT_STATUSES)),
                ).fetchone()
                if active is None and not self._schedule_workflow_tick_open(
                    conn, [schedule_id]
                ):
                    conn.execute(
                        """
                        DELETE FROM schedules
                        WHERE id = %s AND tenant_id = %s
                        """,
                        (schedule_id, tenant_id),
                    )
                    return True
                if not wait:
                    detail = (
                        f"job {active['id']} is {active['status']}"
                        if active is not None
                        else "a workflow run is still live"
                    )
                    raise ScheduleError(
                        f"schedule {schedule_id} has in-flight work "
                        f"({detail}); "
                        "delete is refused until the tick finishes"
                    )
                if bool(row["enabled"]):
                    conn.execute(
                        """
                        UPDATE schedules
                        SET enabled = FALSE,
                            updated_at = %s
                        WHERE id = %s AND tenant_id = %s
                        """,
                        (_utcnow(), schedule_id, tenant_id),
                    )
            if deadline is not None and time.monotonic() >= deadline:
                raise ScheduleError(
                    f"schedule {schedule_id} still has in-flight work "
                    "after waiting; schedule is disabled so it will "
                    "not enqueue more ticks"
                )
            time.sleep(0.05)

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
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            updated = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (enabled, now, schedule_id, tenant_id),
            ).fetchone()
            assert updated is not None
            return self._schedule(updated)

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

    def update_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        cron: Any = _UNSET,
        interval_seconds: Any = _UNSET,
        timezone: Any = _UNSET,
        payload: Any = _UNSET,
        dag_jobs: Any = _UNSET,
        workflow: Any = _UNSET,
        queue_name: Any = _UNSET,
        name: Any = _UNSET,
        priority: Any = _UNSET,
        max_attempts: Any = _UNSET,
        visibility_timeout_seconds: Any = _UNSET,
        labels: Any = _UNSET,
        required_skills: Any = _UNSET,
        misfire_policy: Any = _UNSET,
        catch_up_max: Any = _UNSET,
        mutex_group: Any = _UNSET,
        waits_on: Any = _UNSET,
        joins_on: Any = _UNSET,
    ) -> Schedule:
        """Edit a live schedule without deleting it.

        Cadence changes (cron, interval, timezone) recompute ``next_run_at``
        as the next occurrence **after now** and clear ``catch_up_remaining``
        so a faster cron cannot backfill missed ticks. The in-flight tick
        (one job or a whole DAG) is left alone; the new DAG template /
        payload applies on the next fire. ``process_due`` and this update
        both ``FOR UPDATE`` the schedule row, so a tick cannot mix old and
        new templates.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        fields = (
            cron,
            interval_seconds,
            timezone,
            payload,
            dag_jobs,
            workflow,
            queue_name,
            name,
            priority,
            max_attempts,
            visibility_timeout_seconds,
            labels,
            required_skills,
            misfire_policy,
            catch_up_max,
            mutex_group,
            waits_on,
            joins_on,
        )
        if all(f is _UNSET for f in fields):
            raise ValueError("pass at least one field to update")

        from job_queue.cronutil import validate_cron, validate_timezone

        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is offboarding; update is refused"
                )

            cron_expr = row.get("cron_expr")
            interval = row.get("interval_seconds")
            tz_name = str(row.get("timezone") or "UTC")
            cadence_changed = False
            if cron is not _UNSET:
                if cron is None or str(cron).strip() == "":
                    cron_expr = None
                else:
                    cron_expr = validate_cron(str(cron))
                cadence_changed = True
            if interval_seconds is not _UNSET:
                if interval_seconds is None:
                    interval = None
                else:
                    interval = int(interval_seconds)
                    if interval < 1:
                        raise ValueError("interval_seconds must be >= 1 or None")
                cadence_changed = True
            if timezone is not _UNSET:
                tz_name = validate_timezone(str(timezone))
                cadence_changed = True
            if cron_expr is None and interval is None:
                raise ValueError("provide cron and/or interval_seconds")

            qname = str(row["queue_name"])
            if queue_name is not _UNSET:
                qname = str(queue_name).strip()
                if not qname:
                    raise ValueError("queue_name is required")
            new_name = row.get("name")
            if name is not _UNSET:
                if name is None or str(name).strip() == "":
                    new_name = None
                else:
                    new_name = str(name).strip()
            pri = int(row["priority"])
            if priority is not _UNSET:
                pri = int(priority)
            attempts = int(row["max_attempts"])
            if max_attempts is not _UNSET:
                attempts = int(max_attempts)
                if attempts < 1:
                    raise ValueError("max_attempts must be >= 1")
            vis = int(row["visibility_timeout_seconds"])
            if visibility_timeout_seconds is not _UNSET:
                vis = int(visibility_timeout_seconds)
                if vis < 1:
                    raise ValueError("visibility_timeout_seconds must be >= 1")
            labs = list(row.get("labels") or [])
            if labels is not _UNSET:
                labs = _normalize_labels(labels)
            skills = list(row.get("required_skills") or [])
            if required_skills is not _UNSET:
                skills = _normalize_skills(required_skills)
            policy = str(row.get("misfire_policy") or "catch_up_one")
            if misfire_policy is not _UNSET:
                policy = normalize_misfire_policy(misfire_policy)
            catch_max = int(row.get("catch_up_max") or 8)
            if catch_up_max is not _UNSET:
                catch_max = normalize_catch_up_max(catch_up_max)
            group = row.get("mutex_group")
            if mutex_group is not _UNSET:
                group = normalize_mutex_group(mutex_group)
            wait_ids = list(row.get("waits_on") or [])
            if waits_on is not _UNSET:
                wait_ids = normalize_waits_on(waits_on)
                self._assert_waits_on_ok(
                    conn,
                    tenant_id=tenant_id,
                    schedule_id=schedule_id,
                    waits_on=wait_ids,
                )
            join_ids = list(row.get("joins_on") or [])
            if joins_on is not _UNSET:
                join_ids = normalize_joins_on(joins_on)
                self._assert_joins_on_ok(
                    conn,
                    tenant_id=tenant_id,
                    schedule_id=schedule_id,
                    joins_on=join_ids,
                )

            dag_val = row.get("dag_jobs")
            if dag_jobs is not _UNSET:
                dag_list = normalize_schedule_dag_jobs(dag_jobs)
                dag_val = Jsonb(dag_list) if dag_list else None
            wf_val = row.get("workflow")
            if workflow is not _UNSET:
                wf_norm = normalize_schedule_workflow(workflow)
                wf_val = Jsonb(wf_norm) if wf_norm else None
            dag_present = (
                normalize_schedule_dag_jobs(dag_jobs) is not None
                if dag_jobs is not _UNSET
                else bool(_dag_jobs_from_row(row.get("dag_jobs")))
            )
            wf_present = (
                normalize_schedule_workflow(workflow) is not None
                if workflow is not _UNSET
                else bool(_workflow_from_row(row.get("workflow")))
            )
            if dag_present and wf_present:
                raise ValueError("workflow is mutually exclusive with dag_jobs")

            sealed = row["payload"]
            payload_key_id = row.get("payload_key_id")
            if payload is not _UNSET:
                if not isinstance(payload, dict):
                    raise ValueError("payload must be a dict")
                sealed, payload_key_id = self._seal_schedule_payload(
                    conn,
                    tenant_id=tenant_id,
                    schedule_id=schedule_id,
                    payload=dict(payload),
                )

            next_run = row["next_run_at"]
            catch_remaining = row.get("catch_up_remaining")
            if cadence_changed:
                merged = {
                    "cron_expr": cron_expr,
                    "interval_seconds": interval,
                    "timezone": tz_name,
                    "next_run_at": next_run,
                }
                next_run = self._schedule_next_run(merged, now=now)
                catch_remaining = None

            try:
                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET cron_expr = %s,
                        interval_seconds = %s,
                        timezone = %s,
                        payload = %s,
                        payload_key_id = %s,
                        dag_jobs = %s,
                        workflow = %s,
                        queue_name = %s,
                        name = %s,
                        priority = %s,
                        max_attempts = %s,
                        visibility_timeout_seconds = %s,
                        labels = %s,
                        required_skills = %s,
                        misfire_policy = %s,
                        catch_up_max = %s,
                        mutex_group = %s,
                        waits_on = %s,
                        joins_on = %s,
                        next_run_at = %s,
                        catch_up_remaining = %s,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        cron_expr,
                        interval,
                        tz_name,
                        Jsonb(sealed) if isinstance(sealed, dict) else sealed,
                        payload_key_id,
                        dag_val,
                        wf_val,
                        qname,
                        new_name,
                        pri,
                        attempts,
                        vis,
                        labs,
                        skills,
                        policy,
                        catch_max,
                        group,
                        wait_ids,
                        join_ids,
                        next_run,
                        catch_remaining,
                        now,
                        schedule_id,
                        tenant_id,
                    ),
                ).fetchone()
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule name {new_name!r} already exists for "
                    f"tenant {tenant_id!r}"
                ) from exc
            assert updated is not None
            return self._schedule(updated)

    @staticmethod
    def _mutex_lock_key(tenant_id: str, mutex_group: str) -> str:
        return f"{tenant_id}\x1f{mutex_group}"

    @staticmethod
    def _try_lock_schedule_mutex_group(
        conn: Any, tenant_id: str, mutex_group: str
    ) -> bool:
        """Non-blocking per-tenant group lock.

        A blocking lock here would stall a global ``process_due`` that also
        holds other tenants' schedule rows (``SKIP LOCKED`` then waits).
        """
        row = conn.execute(
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s)) AS locked",
            (
                _SCHED_MUTEX_LOCK_CLASS,
                SchedulesCrudMixin._mutex_lock_key(tenant_id, mutex_group),
            ),
        ).fetchone()
        return bool(row and row["locked"])

    @staticmethod
    def _assert_waits_on_ok(
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        waits_on: Sequence[UUID],
    ) -> None:
        if not waits_on:
            return
        if schedule_id in waits_on:
            raise ValueError("schedule cannot wait on itself")
        rows = conn.execute(
            """
            SELECT id, waits_on
            FROM schedules
            WHERE tenant_id = %s AND id = ANY(%s)
            ORDER BY id
            FOR UPDATE
            """,
            (tenant_id, list(waits_on)),
        ).fetchall()
        found = {row["id"] for row in rows}
        missing = [uid for uid in waits_on if uid not in found]
        if missing:
            raise ValueError(
                f"waits_on schedule {missing[0]} not found for tenant "
                f"{tenant_id!r}"
            )
        graph_rows = conn.execute(
            """
            SELECT id, waits_on
            FROM schedules
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchall()
        graph: dict[UUID, list[UUID]] = {}
        for row in graph_rows:
            graph[row["id"]] = list(row.get("waits_on") or [])
        graph[schedule_id] = list(waits_on)
        seen: set[UUID] = set()
        stack: set[UUID] = set()

        def visit(node: UUID) -> None:
            if node in stack:
                raise ValueError("waits_on contains a cycle")
            if node in seen:
                return
            stack.add(node)
            for nxt in graph.get(node) or []:
                visit(nxt)
            stack.remove(node)
            seen.add(node)

        visit(schedule_id)

    @staticmethod
    def _assert_joins_on_ok(
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        joins_on: Sequence[UUID],
    ) -> None:
        if not joins_on:
            return
        if schedule_id in joins_on:
            raise ValueError("schedule cannot join on itself")
        rows = conn.execute(
            """
            SELECT id
            FROM schedules
            WHERE tenant_id = %s AND id = ANY(%s)
            ORDER BY id
            FOR UPDATE
            """,
            (tenant_id, list(joins_on)),
        ).fetchall()
        found = {row["id"] for row in rows}
        missing = [uid for uid in joins_on if uid not in found]
        if missing:
            raise ValueError(
                f"joins_on schedule {missing[0]} not found for tenant "
                f"{tenant_id!r}"
            )

    @staticmethod
    def _resolve_tick_iteration(conn: Any, sched_row: Any) -> int:
        """Next generation for this fire: inherit waits_on, else increment."""
        current = int(sched_row.get("tick_iteration") or 0)
        joins = list(sched_row.get("joins_on") or [])
        if joins:
            return current + 1
        waits = list(sched_row.get("waits_on") or [])
        if waits:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(tick_iteration), 0) AS n
                FROM schedules
                WHERE id = ANY(%s)
                """,
                (waits,),
            ).fetchone()
            inherited = int((row or {}).get("n") or 0)
            # 0 when every upstream is still on generation 0 (never ticked).
            # Do not invent a waiter-only iteration — that fires empty.
            return inherited if inherited > 0 else current
        return current + 1

    def _maybe_advance_schedule_completed(
        self,
        conn: Any,
        *,
        schedule_id: UUID | None,
        tenant_id: str | None = None,
    ) -> None:
        """Record last_completed_iteration when a generation fully succeeds."""
        if schedule_id is None:
            return
        inflight = list(_SCHEDULE_INFLIGHT_STATUSES)
        failed = list(_SCHEDULE_TICK_FAILED_STATUSES)
        live = conn.execute(
            """
            SELECT 1
            FROM jobs
            WHERE schedule_id = %s
              AND status = ANY(%s)
            LIMIT 1
            """,
            (schedule_id, inflight),
        ).fetchone()
        if live is not None:
            return
        if self._schedule_workflow_tick_open(conn, [schedule_id]):
            return
        failed_row = conn.execute(
            """
            SELECT 1
            FROM (
                SELECT MAX(created_at) FILTER (
                           WHERE status = %s
                       ) AS last_ok,
                       MAX(created_at) FILTER (
                           WHERE status = ANY(%s)
                       ) AS last_bad
                FROM jobs
                WHERE schedule_id = %s
            ) t
            WHERE t.last_bad IS NOT NULL
              AND (t.last_ok IS NULL OR t.last_bad >= t.last_ok)
            """,
            (JobStatus.COMPLETED.value, failed, schedule_id),
        ).fetchone()
        if failed_row is not None:
            return
        conn.execute(
            """
            UPDATE schedules
            SET last_completed_iteration = tick_iteration,
                updated_at = NOW()
            WHERE id = %s
              AND tick_iteration > 0
              AND (
                last_completed_iteration IS NULL
                OR tick_iteration > last_completed_iteration
              )
            """,
            (schedule_id,),
        )
        if tenant_id and hasattr(self, "_complete_schedule_generation"):
            self._complete_schedule_generation(
                conn, schedule_id=schedule_id, tenant_id=tenant_id
            )

    @staticmethod
    def _join_ready_watermark(
        conn: Any,
        joins_on: Sequence[UUID] | None,
    ) -> int:
        """Lowest last_completed_iteration across the join set (0 if empty)."""
        ids = list(joins_on or [])
        if not ids:
            return 0
        row = conn.execute(
            """
            SELECT COALESCE(MIN(last_completed_iteration), 0) AS n
            FROM schedules
            WHERE id = ANY(%s)
            """,
            (ids,),
        ).fetchone()
        return int((row or {}).get("n") or 0)

    @staticmethod
    def _schedule_join_blocked(
        conn: Any,
        *,
        tenant_id: str,
        joins_on: Sequence[UUID] | None,
        tick_iteration: int | None,
        now: datetime | None = None,
    ) -> bool:
        """True until every waiter finished the next join generation.

        A paused waiter, or one whose mutex peer is paused / inflight,
        keeps the join blocked for a generation it has not completed.
        Missing waiter rows (delete / move) also fail closed.
        """
        ids = list(joins_on or [])
        if not ids:
            return False
        needed = int(tick_iteration or 0) + 1
        inflight = list(_SCHEDULE_INFLIGHT_STATUSES)
        found = conn.execute(
            """
            SELECT id, tenant_id, enabled, last_completed_iteration,
                   mutex_group
            FROM schedules
            WHERE tenant_id = %s
              AND id = ANY(%s)
            """,
            (tenant_id, ids),
        ).fetchall()
        if len(found) < len(ids):
            return True
        for waiter in found:
            completed = waiter.get("last_completed_iteration")
            if completed is not None and int(completed) >= needed:
                continue
            if not bool(waiter["enabled"]):
                return True
            group = waiter.get("mutex_group")
            if group:
                peer = conn.execute(
                    """
                    SELECT 1
                    FROM schedules p
                    WHERE p.tenant_id = %s
                      AND p.mutex_group = %s
                      AND p.id <> %s
                      AND (
                        p.enabled = FALSE
                        OR EXISTS (
                            SELECT 1
                            FROM jobs j
                            WHERE j.schedule_id = p.id
                              AND j.status = ANY(%s)
                        )
                    )
                    LIMIT 1
                    """,
                    (
                        waiter["tenant_id"],
                        str(group),
                        waiter["id"],
                        inflight,
                    ),
                ).fetchone()
                if peer is not None:
                    return True
            return True
        return False

    @staticmethod
    def _schedule_waits_blocked(
        conn: Any,
        *,
        tenant_id: str,
        waits_on: Sequence[UUID] | None,
        now: datetime | None = None,
    ) -> bool:
        """True if an upstream tick, due slot, pause, or mutex peer blocks.

        Live jobs are matched by ``schedule_id`` on any tenant so a
        delete or a move cannot hide an in-flight tick from the waiter.
        A cancelled, dead-lettered, or failed tick also stays blocking
        until a later job for that schedule_id is completed — vanishing
        is not success. An enabled upstream that is already due blocks
        even before it has materialized a job, otherwise ``process_due``
        (ordered by ``next_run_at``) can fire a more-overdue downstream
        first.
        """
        ids = list(waits_on or [])
        if not ids:
            return False
        at = now if now is not None else _utcnow()
        inflight = list(_SCHEDULE_INFLIGHT_STATUSES)
        failed = list(_SCHEDULE_TICK_FAILED_STATUSES)
        row = conn.execute(
            """
            SELECT 1
            WHERE EXISTS (
                SELECT 1
                FROM jobs j
                WHERE j.schedule_id = ANY(%s)
                  AND j.status = ANY(%s)
            )
            OR EXISTS (
                SELECT 1
                FROM workflow_runs wr
                WHERE wr.schedule_id = ANY(%s)
                  AND wr.status IN ('running', 'compensating')
            )
            OR EXISTS (
                SELECT 1
                FROM (
                    SELECT j.schedule_id,
                           MAX(j.created_at) FILTER (
                               WHERE j.status = %s
                           ) AS last_ok,
                           MAX(j.created_at) FILTER (
                               WHERE j.status = ANY(%s)
                           ) AS last_bad
                    FROM jobs j
                    WHERE j.schedule_id = ANY(%s)
                    GROUP BY j.schedule_id
                ) t
                WHERE t.last_bad IS NOT NULL
                  AND (t.last_ok IS NULL OR t.last_bad >= t.last_ok)
            )
            OR EXISTS (
                SELECT 1
                FROM schedules u
                WHERE u.tenant_id = %s
                  AND u.id = ANY(%s)
                  AND (
                    u.enabled = FALSE
                    OR u.next_run_at <= %s
                    OR (
                        u.mutex_group IS NOT NULL
                        AND EXISTS (
                            SELECT 1
                            FROM schedules p
                            WHERE p.tenant_id = u.tenant_id
                              AND p.mutex_group = u.mutex_group
                              AND p.id <> u.id
                              AND (
                                p.enabled = FALSE
                                OR EXISTS (
                                    SELECT 1
                                    FROM jobs j
                                    WHERE j.schedule_id = p.id
                                      AND j.status = ANY(%s)
                                )
                              )
                        )
                    )
                )
            )
            LIMIT 1
            """,
            (
                ids,
                inflight,
                ids,
                JobStatus.COMPLETED.value,
                failed,
                ids,
                tenant_id,
                ids,
                at,
                inflight,
            ),
        ).fetchone()
        return row is not None

    @staticmethod
    def _schedule_workflow_tick_open(
        conn: Any,
        schedule_ids: Sequence[UUID],
    ) -> bool:
        """True while a schedule-tied workflow run is still running."""
        ids = list(schedule_ids or [])
        if not ids:
            return False
        row = conn.execute(
            """
            SELECT 1
            FROM workflow_runs
            WHERE schedule_id = ANY(%s)
              AND status IN ('running', 'compensating')
            LIMIT 1
            """,
            (ids,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _schedule_mutex_blocked(
        conn: Any,
        *,
        tenant_id: str,
        mutex_group: str | None,
    ) -> bool:
        """True when any member of the group has a live tick/DAG."""
        if not mutex_group:
            return False
        row = conn.execute(
            """
            SELECT 1
            WHERE EXISTS (
                SELECT j.id
                FROM jobs j
                JOIN schedules s ON s.id = j.schedule_id
                WHERE s.tenant_id = %s
                  AND j.tenant_id = s.tenant_id
                  AND s.mutex_group = %s
                  AND j.status = ANY(%s)
            )
            OR EXISTS (
                SELECT 1
                FROM workflow_runs wr
                JOIN schedules s ON s.id = wr.schedule_id
                WHERE s.tenant_id = %s
                  AND wr.tenant_id = s.tenant_id
                  AND s.mutex_group = %s
                  AND wr.status IN ('running', 'compensating')
            )
            LIMIT 1
            """,
            (
                tenant_id,
                mutex_group,
                list(_SCHEDULE_INFLIGHT_STATUSES),
                tenant_id,
                mutex_group,
            ),
        ).fetchone()
        return row is not None

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
