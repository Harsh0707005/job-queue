"""Cron expression helpers for recurring schedules.

Supports standard 5-field (min hour dom mon dow) and 6-field with seconds
(sec min hour dom mon dow). Uses the ``croniter`` package.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter


def validate_cron(cron_expr: str) -> str:
    """Normalize and validate a cron expression. Returns stripped expr."""
    if not cron_expr or not str(cron_expr).strip():
        raise ValueError("cron expression is required")
    expr = " ".join(str(cron_expr).split())
    fields = expr.split(" ")
    if len(fields) not in (5, 6):
        raise ValueError(
            "cron must have 5 fields (min hour dom mon dow) or "
            "6 fields (sec min hour dom mon dow)"
        )
    second_at_beginning = len(fields) == 6
    try:
        if not croniter.is_valid(expr, second_at_beginning=second_at_beginning):
            raise ValueError(f"invalid cron expression: {expr!r}")
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"invalid cron expression: {expr!r}") from exc
    return expr


def validate_timezone(tz_name: str) -> str:
    if not tz_name or not str(tz_name).strip():
        raise ValueError("timezone is required")
    name = str(tz_name).strip()
    try:
        ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {name!r}") from exc
    return name


def next_cron_fire(
    cron_expr: str,
    after: datetime,
    *,
    timezone_name: str = "UTC",
) -> datetime:
    """Return the next fire time strictly after *after* (UTC-aware).

    *after* may be naive (treated as UTC) or aware. Result is always
    timezone-aware UTC.
    """
    expr = validate_cron(cron_expr)
    tz_name = validate_timezone(timezone_name)
    tz = ZoneInfo(tz_name)

    if after.tzinfo is None:
        after_utc = after.replace(tzinfo=timezone.utc)
    else:
        after_utc = after.astimezone(timezone.utc)
    after_local = after_utc.astimezone(tz)

    fields = expr.split(" ")
    second_at_beginning = len(fields) == 6
    it = croniter(expr, after_local, second_at_beginning=second_at_beginning)
    nxt_local = it.get_next(datetime)
    if nxt_local.tzinfo is None:
        nxt_local = nxt_local.replace(tzinfo=tz)
    return nxt_local.astimezone(timezone.utc)
