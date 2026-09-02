"""Per-tenant and per-schedule maintenance blackout windows."""

from __future__ import annotations

from datetime import datetime, time
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from job_queue.cronutil import validate_timezone
from job_queue.db import transaction
from job_queue.models import ScheduleBlackout
from job_queue.queue._common import (
    BlackoutError,
    ScheduleError,
    _as_utc,
    _utcnow,
)

BLACKOUT_ONCE = "once"
BLACKOUT_DAILY = "daily"
BLACKOUT_KINDS = frozenset({BLACKOUT_ONCE, BLACKOUT_DAILY})

_MAX_BLACKOUTS_PER_SCOPE = 32
_MAX_ONCE_SECONDS = 31 * 24 * 3600
_MIN_ONCE_SECONDS = 1
_MAX_REASON_LEN = 256

_BLACKOUT_COLUMNS = """
    id, tenant_id, schedule_id, kind, starts_at, ends_at,
    daily_start, daily_end, timezone, reason, created_at, updated_at
"""

# *now* is bound once via CROSS JOIN (SELECT %s::timestamptz AS now) n
_BLACKOUT_COVERS_SQL = """
    (
        (b.kind = 'once' AND b.starts_at <= n.now AND n.now < b.ends_at)
        OR (
            b.kind = 'daily'
            AND (
                CASE
                    WHEN b.daily_start < b.daily_end THEN
                        (timezone(b.timezone, n.now))::time >= b.daily_start
                        AND (timezone(b.timezone, n.now))::time < b.daily_end
                    ELSE
                        (timezone(b.timezone, n.now))::time >= b.daily_start
                        OR (timezone(b.timezone, n.now))::time < b.daily_end
                END
            )
        )
    )
"""

_BLACKOUT_EXISTS_SQL = f"""
    EXISTS (
        SELECT 1
        FROM schedule_blackouts b
        CROSS JOIN (SELECT %s::timestamptz AS now) n
        WHERE b.tenant_id = schedules.tenant_id
          AND (b.schedule_id IS NULL OR b.schedule_id = schedules.id)
          AND {_BLACKOUT_COVERS_SQL}
    )
"""


def normalize_blackout_kind(raw: str | None) -> str:
    if raw is None or str(raw).strip() == "":
        return BLACKOUT_ONCE
    key = str(raw).strip().lower().replace("-", "_")
    if key in ("once", "one_shot", "absolute", "window"):
        return BLACKOUT_ONCE
    if key in ("daily", "quiet_hours", "quiet", "recurring"):
        return BLACKOUT_DAILY
    raise ValueError("blackout kind must be 'once' or 'daily'")


def _parse_clock(raw: time | str | None) -> time | None:
    if raw is None:
        return None
    if isinstance(raw, time):
        return time(raw.hour, raw.minute, raw.second)
    text = str(raw).strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("daily time must be HH:MM or HH:MM:SS")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) == 3 else 0
    except ValueError as exc:
        raise ValueError("daily time must be HH:MM or HH:MM:SS") from exc
    if hour == 24 and minute == 0 and second == 0:
        raise ValueError("daily time 24:00 is not allowed; use 00:00")
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        raise ValueError("daily time is out of range")
    return time(hour, minute, second)


def _row_to_blackout(row: Any, *, now: datetime) -> ScheduleBlackout:
    data = dict(row)
    starts = data.get("starts_at")
    ends = data.get("ends_at")
    kind = str(data.get("kind") or BLACKOUT_ONCE)
    daily_start = data.get("daily_start")
    daily_end = data.get("daily_end")
    tz_name = str(data.get("timezone") or "UTC")
    active = False
    if kind == BLACKOUT_ONCE and starts is not None and ends is not None:
        active = _as_utc(starts) <= now < _as_utc(ends)
    elif kind == BLACKOUT_DAILY and daily_start is not None and daily_end is not None:
        local = now.astimezone(ZoneInfo(tz_name)).timetz()
        local_t = time(local.hour, local.minute, local.second)
        start_t = (
            daily_start
            if isinstance(daily_start, time)
            else _parse_clock(str(daily_start))
        )
        end_t = (
            daily_end if isinstance(daily_end, time) else _parse_clock(str(daily_end))
        )
        assert start_t is not None and end_t is not None
        if start_t < end_t:
            active = start_t <= local_t < end_t
        else:
            active = local_t >= start_t or local_t < end_t
    return ScheduleBlackout(
        id=data["id"],
        tenant_id=str(data["tenant_id"]),
        schedule_id=data.get("schedule_id"),
        kind=kind,
        starts_at=starts,
        ends_at=ends,
        daily_start=daily_start if isinstance(daily_start, time) else _parse_clock(daily_start),
        daily_end=daily_end if isinstance(daily_end, time) else _parse_clock(daily_end),
        timezone=tz_name,
        reason=data.get("reason"),
        created_at=data["created_at"],
        updated_at=data["updated_at"],
        active=active,
    )


class SchedulesBlackoutMixin:
    def create_blackout(
        self,
        *,
        tenant_id: str,
        schedule_id: UUID | None = None,
        kind: str | None = None,
        starts_at: datetime | None = None,
        ends_at: datetime | None = None,
        daily_start: time | str | None = None,
        daily_end: time | str | None = None,
        timezone: str = "UTC",
        reason: str | None = None,
    ) -> ScheduleBlackout:
        """Register a tenant-wide or per-schedule maintenance window.

        While a covering window is active, ``process_due_schedules`` does not
        enqueue for matching schedules and does not advance ``next_run_at``.
        When the window ends the schedule's misfire policy runs. Overlapping
        windows are a union; they do not stack extra fires.

        Mutations take the tenant advisory lock then the blackout row so
        concurrent create/update/delete cannot deadlock with each other or
        with ``process_due`` (which only SKIP LOCKs the schedule row).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        inferred = kind
        if inferred is None:
            if daily_start is not None or daily_end is not None:
                inferred = BLACKOUT_DAILY
            else:
                inferred = BLACKOUT_ONCE
        policy = normalize_blackout_kind(inferred)
        tz_name = validate_timezone(timezone)
        reason_n = _normalize_reason(reason)
        now = _utcnow()
        start = _as_utc(starts_at) if starts_at is not None else None
        end = _as_utc(ends_at) if ends_at is not None else None
        d_start = _parse_clock(daily_start)
        d_end = _parse_clock(daily_end)
        if policy == BLACKOUT_ONCE:
            if start is None or end is None:
                raise ValueError("once blackout requires starts_at and ends_at")
            _validate_once(start, end, now=now, allow_past_end=False)
            d_start = None
            d_end = None
        else:
            if d_start is None or d_end is None:
                raise ValueError("daily blackout requires daily_start and daily_end")
            if d_start == d_end:
                raise ValueError("daily_start and daily_end must differ")
            start = None
            end = None

        bid = uuid4()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise BlackoutError(
                    f"tenant {tenant_id!r} is offboarding; blackouts are refused"
                )
            self._assert_tenant_mutable(
                conn, tenant_id, action="update", error=BlackoutError
            )
            if schedule_id is not None:
                found = conn.execute(
                    """
                    SELECT 1 FROM schedules
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (schedule_id, tenant_id),
                ).fetchone()
                if found is None:
                    raise ScheduleError(
                        f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                    )
            count = conn.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM schedule_blackouts
                WHERE tenant_id = %s
                  AND schedule_id IS NOT DISTINCT FROM %s
                """,
                (tenant_id, schedule_id),
            ).fetchone()
            if int(count["n"]) >= _MAX_BLACKOUTS_PER_SCOPE:
                raise BlackoutError(
                    f"at most {_MAX_BLACKOUTS_PER_SCOPE} blackouts per "
                    f"{'schedule' if schedule_id else 'tenant'}"
                )
            row = conn.execute(
                f"""
                INSERT INTO schedule_blackouts (
                    id, tenant_id, schedule_id, kind, starts_at, ends_at,
                    daily_start, daily_end, timezone, reason,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                RETURNING {_BLACKOUT_COLUMNS}
                """,
                (
                    bid,
                    tenant_id,
                    schedule_id,
                    policy,
                    start,
                    end,
                    d_start,
                    d_end,
                    tz_name,
                    reason_n,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return _row_to_blackout(row, now=now)

    def update_blackout(
        self,
        blackout_id: UUID,
        *,
        tenant_id: str,
        schedule_id: UUID | None = None,
        starts_at: datetime | None = None,
        ends_at: datetime | None = None,
        daily_start: time | str | None = None,
        daily_end: time | str | None = None,
        timezone: str | None = None,
        reason: str | None = None,
        clear_reason: bool = False,
    ) -> ScheduleBlackout:
        """Change an existing window in place (no extra fire, no snap of next_run)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            row = conn.execute(
                f"""
                SELECT {_BLACKOUT_COLUMNS}
                FROM schedule_blackouts
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (blackout_id, tenant_id),
            ).fetchone()
            if row is None:
                raise BlackoutError(
                    f"blackout {blackout_id} not found for tenant {tenant_id!r}"
                )
            if schedule_id is not None and row["schedule_id"] != schedule_id:
                raise BlackoutError(
                    f"blackout {blackout_id} is not on schedule {schedule_id}"
                )
            kind = str(row["kind"])
            start = row["starts_at"] if starts_at is None else _as_utc(starts_at)
            end = row["ends_at"] if ends_at is None else _as_utc(ends_at)
            d_start = (
                row["daily_start"]
                if daily_start is None
                else _parse_clock(daily_start)
            )
            d_end = row["daily_end"] if daily_end is None else _parse_clock(daily_end)
            tz_name = (
                str(row["timezone"])
                if timezone is None
                else validate_timezone(timezone)
            )
            if clear_reason:
                reason_n = None
            elif reason is None:
                reason_n = row.get("reason")
            else:
                reason_n = _normalize_reason(reason)
            if kind == BLACKOUT_ONCE:
                if start is None or end is None:
                    raise ValueError("once blackout requires starts_at and ends_at")
                _validate_once(start, end, now=now, allow_past_end=True)
            else:
                if d_start is None or d_end is None:
                    raise ValueError("daily blackout requires daily_start and daily_end")
                if d_start == d_end:
                    raise ValueError("daily_start and daily_end must differ")
            updated = conn.execute(
                f"""
                UPDATE schedule_blackouts
                SET starts_at = %s,
                    ends_at = %s,
                    daily_start = %s,
                    daily_end = %s,
                    timezone = %s,
                    reason = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_BLACKOUT_COLUMNS}
                """,
                (
                    start if kind == BLACKOUT_ONCE else None,
                    end if kind == BLACKOUT_ONCE else None,
                    d_start if kind == BLACKOUT_DAILY else None,
                    d_end if kind == BLACKOUT_DAILY else None,
                    tz_name,
                    reason_n,
                    now,
                    blackout_id,
                    tenant_id,
                ),
            ).fetchone()
            assert updated is not None
            return _row_to_blackout(updated, now=now)

    def delete_blackout(
        self,
        blackout_id: UUID,
        *,
        tenant_id: str,
        schedule_id: UUID | None = None,
    ) -> bool:
        """Remove a window. Overdue ticks keep ``next_run_at`` for misfire policy."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if schedule_id is not None:
                row = conn.execute(
                    """
                    DELETE FROM schedule_blackouts
                    WHERE id = %s AND tenant_id = %s AND schedule_id = %s
                    RETURNING id
                    """,
                    (blackout_id, tenant_id, schedule_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    DELETE FROM schedule_blackouts
                    WHERE id = %s AND tenant_id = %s
                    RETURNING id
                    """,
                    (blackout_id, tenant_id),
                ).fetchone()
            return row is not None

    def get_blackout(
        self,
        blackout_id: UUID,
        *,
        tenant_id: str,
    ) -> ScheduleBlackout | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_BLACKOUT_COLUMNS}
                FROM schedule_blackouts
                WHERE id = %s AND tenant_id = %s
                """,
                (blackout_id, tenant_id),
            ).fetchone()
            return _row_to_blackout(row, now=now) if row else None

    def list_blackouts(
        self,
        *,
        tenant_id: str,
        schedule_id: UUID | None = None,
        include_tenant: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ScheduleBlackout]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        now = _utcnow()
        with self._pool.connection() as conn:
            if schedule_id is None:
                rows = conn.execute(
                    f"""
                    SELECT {_BLACKOUT_COLUMNS}
                    FROM schedule_blackouts
                    WHERE tenant_id = %s
                      AND schedule_id IS NULL
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, limit, offset),
                ).fetchall()
            elif include_tenant:
                rows = conn.execute(
                    f"""
                    SELECT {_BLACKOUT_COLUMNS}
                    FROM schedule_blackouts
                    WHERE tenant_id = %s
                      AND (schedule_id IS NULL OR schedule_id = %s)
                    ORDER BY
                        CASE WHEN schedule_id IS NULL THEN 0 ELSE 1 END,
                        created_at ASC, id ASC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, schedule_id, limit, offset),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT {_BLACKOUT_COLUMNS}
                    FROM schedule_blackouts
                    WHERE tenant_id = %s AND schedule_id = %s
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, schedule_id, limit, offset),
                ).fetchall()
            return [_row_to_blackout(r, now=now) for r in rows]

    def is_schedule_blacked_out(
        self,
        *,
        tenant_id: str,
        schedule_id: UUID,
        now: datetime | None = None,
    ) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        at = _as_utc(now) if now is not None else _utcnow()
        with self._pool.connection() as conn:
            return self._is_schedule_blacked_out(
                conn, tenant_id, schedule_id, at
            )

    @staticmethod
    def _is_schedule_blacked_out(
        conn: Any,
        tenant_id: str,
        schedule_id: UUID,
        now: datetime,
    ) -> bool:
        row = conn.execute(
            f"""
            SELECT 1
            FROM schedule_blackouts b
            CROSS JOIN (SELECT %s::timestamptz AS now) n
            WHERE b.tenant_id = %s
              AND (b.schedule_id IS NULL OR b.schedule_id = %s)
              AND {_BLACKOUT_COVERS_SQL}
            LIMIT 1
            """,
            (now, tenant_id, schedule_id),
        ).fetchone()
        return row is not None


def _normalize_reason(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if len(text) > _MAX_REASON_LEN:
        raise ValueError(f"reason longer than {_MAX_REASON_LEN} characters")
    return text


def _validate_once(
    start: datetime,
    end: datetime,
    *,
    now: datetime,
    allow_past_end: bool,
) -> None:
    if end <= start:
        raise ValueError("ends_at must be after starts_at")
    span = (end - start).total_seconds()
    if span < _MIN_ONCE_SECONDS:
        raise ValueError("once blackout must last at least 1 second")
    if span > _MAX_ONCE_SECONDS:
        raise ValueError("once blackout cannot last more than 31 days")
    if not allow_past_end and end <= now:
        raise ValueError("ends_at must be in the future")
