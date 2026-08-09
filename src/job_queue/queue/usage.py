"""Per-tenant billing usage meters.

Counters live in ``tenant_usage_buckets`` (hourly) and are incremented in the
**same database transaction** as the operational event (enqueue/lease/ack/…).
A crash rolls back both the job mutation and the meter — no stale partial
increments, no double-count on retry (lease fencing / status predicates make
the mutation succeed at most once).

Operational ``queue_metric_buckets`` remain short-retention monitoring;
this ledger is kept independently so billing is not purged with metrics.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from job_queue.models import TenantUsage
from job_queue.queue._common import _utcnow


_USAGE_BUCKET_SECONDS = 3600

_USAGE_FIELDS: tuple[str, ...] = (
    "enqueues",
    "leases",
    "acks",
    "nacks",
    "fails",
    "dead_letters",
    "cancels",
    "releases",
    "preemptions",
    "schedule_fires",
    "webhook_successes",
    "webhook_failures",
    "spawn_children",
    "runtime_ms",
    "wait_ms",
)

_METRIC_TO_USAGE: dict[str, str] = {
    "lease": "leases",
    "ack": "acks",
    "nack": "nacks",
    "dead": "dead_letters",
    "failed": "fails",
    "webhook_success": "webhook_successes",
    "webhook_fail": "webhook_failures",
    "schedule_fire": "schedule_fires",
}

_SUM_SQL = ", ".join(
    f"COALESCE(SUM({c}), 0)::bigint AS {c}" for c in _USAGE_FIELDS
)


def _usage_bucket_start(now: datetime) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    aligned = epoch - (epoch % _USAGE_BUCKET_SECONDS)
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


def _parse_usage_dt(
    value: str | datetime | None, *, default: datetime | None = None
) -> datetime | None:
    if value is None:
        return default
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    s = str(value).strip()
    if not s:
        return default
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def usage_runtime_ms(row: dict[str, Any] | None, now: datetime) -> int:
    """Elapsed lease time from *row* (must be the pre-update leased row).

    Caps at ``lease_expires_at`` when the worker died and the lease timed
    out, so we bill held time rather than idle time after expiry.
    """
    if not row:
        return 0
    started = row.get("lease_started_at")
    if started is None:
        return 0
    if getattr(started, "tzinfo", None) is None:
        started = started.replace(tzinfo=timezone.utc)
    end = now
    expires = row.get("lease_expires_at")
    if expires is not None:
        if getattr(expires, "tzinfo", None) is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if expires < end:
            end = expires
    ms = int((end - started).total_seconds() * 1000.0)
    return max(0, ms)


def _usage_from_row(row: dict[str, Any], *, start: datetime | None, end: datetime | None) -> TenantUsage:
    return TenantUsage(
        tenant_id=str(row["tenant_id"]),
        period_start=start,
        period_end=end,
        enqueues=int(row.get("enqueues") or 0),
        leases=int(row.get("leases") or 0),
        acks=int(row.get("acks") or 0),
        nacks=int(row.get("nacks") or 0),
        fails=int(row.get("fails") or 0),
        dead_letters=int(row.get("dead_letters") or 0),
        cancels=int(row.get("cancels") or 0),
        releases=int(row.get("releases") or 0),
        preemptions=int(row.get("preemptions") or 0),
        schedule_fires=int(row.get("schedule_fires") or 0),
        webhook_successes=int(row.get("webhook_successes") or 0),
        webhook_failures=int(row.get("webhook_failures") or 0),
        spawn_children=int(row.get("spawn_children") or 0),
        runtime_ms=int(row.get("runtime_ms") or 0),
        wait_ms=int(row.get("wait_ms") or 0),
    )


class UsageMixin:
    def _record_usage(
        self,
        conn: Any,
        *,
        tenant_id: str,
        now: datetime | None = None,
        **increments: int,
    ) -> None:
        """Add *increments* to this tenant's hourly billing bucket (same txn)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        vals = {k: int(increments.get(k) or 0) for k in _USAGE_FIELDS}
        if any(v < 0 for v in vals.values()):
            raise ValueError("usage increments must be >= 0")
        if all(v == 0 for v in vals.values()):
            return
        ts = now or _utcnow()
        bucket = _usage_bucket_start(ts)
        cols = ", ".join(_USAGE_FIELDS)
        placeholders = ", ".join(["%s"] * len(_USAGE_FIELDS))
        updates = ", ".join(
            f"{c} = tenant_usage_buckets.{c} + EXCLUDED.{c}" for c in _USAGE_FIELDS
        )
        conn.execute(
            f"""
            INSERT INTO tenant_usage_buckets (
                bucket_start, tenant_id, {cols}
            ) VALUES (
                %s, %s, {placeholders}
            )
            ON CONFLICT (bucket_start, tenant_id) DO UPDATE
            SET {updates}
            """,
            (bucket, tenant_id, *[vals[c] for c in _USAGE_FIELDS]),
        )

    def _apply_usage_from_metric(
        self,
        conn: Any,
        *,
        tenant_id: str,
        metric: str,
        count: int,
        sum_ms: float,
        now: datetime,
    ) -> None:
        """Mirror an operational metric increment into the billing ledger."""
        if metric == "wait_before_claim":
            self._record_usage(
                conn,
                tenant_id=tenant_id,
                now=now,
                wait_ms=max(0, int(sum_ms)),
            )
            return
        col = _METRIC_TO_USAGE.get(metric)
        if col is None or count == 0:
            return
        self._record_usage(conn, tenant_id=tenant_id, now=now, **{col: int(count)})

    def _record_lease_end_usage(
        self,
        conn: Any,
        row: dict[str, Any] | None,
        *,
        now: datetime,
        **extra: int,
    ) -> None:
        """Bill runtime for a lease that just ended (row must still have start)."""
        if not row:
            return
        tid = str(row.get("tenant_id") or "")
        if not tid:
            return
        inc = {k: int(v) for k, v in extra.items() if v}
        ms = usage_runtime_ms(row, now)
        if ms:
            inc["runtime_ms"] = inc.get("runtime_ms", 0) + ms
        if inc:
            self._record_usage(conn, tenant_id=tid, now=now, **inc)

    def get_tenant_usage(
        self,
        tenant_id: str,
        *,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
    ) -> TenantUsage:
        """Aggregated billing usage for one tenant in [*start*, *end*)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        start_dt = _parse_usage_dt(start)
        end_dt = _parse_usage_dt(end)
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if start_dt is not None:
            clauses.append("bucket_start >= %s")
            params.append(start_dt)
        if end_dt is not None:
            clauses.append("bucket_start < %s")
            params.append(end_dt)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_SUM_SQL}
                FROM tenant_usage_buckets
                WHERE {where}
                """,
                params,
            ).fetchone()
        data = dict(row or {})
        data["tenant_id"] = tenant_id
        return _usage_from_row(data, start=start_dt, end=end_dt)

    def list_tenant_usage(
        self,
        *,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
        tenant_id: str | None = None,
    ) -> list[TenantUsage]:
        """Aggregated usage per tenant (tenants with no rows are omitted)."""
        start_dt = _parse_usage_dt(start)
        end_dt = _parse_usage_dt(end)
        clauses: list[str] = []
        params: list[Any] = []
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if start_dt is not None:
            clauses.append("bucket_start >= %s")
            params.append(start_dt)
        if end_dt is not None:
            clauses.append("bucket_start < %s")
            params.append(end_dt)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT tenant_id, {_SUM_SQL}
                FROM tenant_usage_buckets
                {where}
                GROUP BY tenant_id
                ORDER BY tenant_id
                """,
                params,
            ).fetchall()
        return [_usage_from_row(r, start=start_dt, end=end_dt) for r in rows]

    def list_tenant_usage_buckets(
        self,
        tenant_id: str,
        *,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Hourly billing buckets for one tenant (newest first)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        start_dt = _parse_usage_dt(start)
        end_dt = _parse_usage_dt(end)
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if start_dt is not None:
            clauses.append("bucket_start >= %s")
            params.append(start_dt)
        if end_dt is not None:
            clauses.append("bucket_start < %s")
            params.append(end_dt)
        params.append(int(limit))
        cols = ", ".join(_USAGE_FIELDS)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT bucket_start, tenant_id, {cols}
                FROM tenant_usage_buckets
                WHERE {' AND '.join(clauses)}
                ORDER BY bucket_start DESC
                LIMIT %s
                """,
                params,
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            bs = item.get("bucket_start")
            if hasattr(bs, "isoformat"):
                item["bucket_start"] = bs.isoformat()
                item["bucket_end"] = (bs + timedelta(seconds=_USAGE_BUCKET_SECONDS)).isoformat()
            out.append(item)
        return out
