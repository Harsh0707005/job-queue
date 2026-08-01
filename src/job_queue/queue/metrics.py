"""JobQueue operational metrics: counters, snapshots, history, noisy tenants.

Counters are written in the same DB transaction as the operational event
(lease/ack/nack/dead/webhook/schedule) so increments cannot be lost under
coordinator crash or high load. Gauges (lag, in-flight) are sampled into
``queue_metric_snapshots`` for trend history.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from job_queue.queue._common import _DEPS_SATISFIED_SQL, _utcnow

# Fixed bucket width for counter history (seconds).
_METRIC_BUCKET_SECONDS = 60

# Reserved tenant key for fleet-wide rollups stored in snapshots.
OVERALL_TENANT = "__overall__"

# Counter metric names (noisy throughput series).
METRIC_LEASE = "lease"
METRIC_ACK = "ack"
METRIC_NACK = "nack"
METRIC_DEAD = "dead"
METRIC_FAILED = "failed"
METRIC_WEBHOOK_SUCCESS = "webhook_success"
METRIC_WEBHOOK_FAIL = "webhook_fail"
METRIC_SCHEDULE_FIRE = "schedule_fire"
METRIC_SCHEDULE_SKIP = "schedule_skip"
METRIC_WAIT_BEFORE_CLAIM = "wait_before_claim"  # count + sum_ms

_COUNTER_METRICS = (
    METRIC_LEASE,
    METRIC_ACK,
    METRIC_NACK,
    METRIC_DEAD,
    METRIC_FAILED,
    METRIC_WEBHOOK_SUCCESS,
    METRIC_WEBHOOK_FAIL,
    METRIC_SCHEDULE_FIRE,
    METRIC_SCHEDULE_SKIP,
    METRIC_WAIT_BEFORE_CLAIM,
)

# Metrics treated as high-volume / "noisy" by default (throughput series).
_NOISY_METRICS = frozenset(
    {
        METRIC_LEASE,
        METRIC_ACK,
        METRIC_NACK,
        METRIC_WAIT_BEFORE_CLAIM,
        METRIC_WEBHOOK_SUCCESS,
        METRIC_WEBHOOK_FAIL,
    }
)


def _bucket_start(now: datetime, *, seconds: int = _METRIC_BUCKET_SECONDS) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    aligned = epoch - (epoch % seconds)
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


def _parse_dt(
    value: str | datetime | None, *, default: datetime | None = None
) -> datetime:
    if value is None:
        if default is None:
            raise ValueError("timestamp required")
        return default
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class MetricsMixin:
    def _record_metric(
        self,
        conn: Any,
        *,
        tenant_id: str,
        metric: str,
        count: int = 1,
        sum_ms: float = 0.0,
        now: datetime | None = None,
    ) -> None:
        """Atomically increment a time-bucket counter (same txn as the event)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if count == 0 and sum_ms == 0:
            return
        if count < 0 or sum_ms < 0:
            raise ValueError("count and sum_ms must be >= 0")
        ts = now or _utcnow()
        bucket = _bucket_start(ts)
        conn.execute(
            """
            INSERT INTO queue_metric_buckets (bucket_start, tenant_id, metric, count, sum_ms)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bucket_start, tenant_id, metric) DO UPDATE
            SET count = queue_metric_buckets.count + EXCLUDED.count,
                sum_ms = queue_metric_buckets.sum_ms + EXCLUDED.sum_ms
            """,
            (bucket, tenant_id, metric, int(count), float(sum_ms)),
        )

    def _record_metrics_batch(
        self,
        conn: Any,
        *,
        tenant_id: str,
        increments: dict[str, int],
        wait_sum_ms: float = 0.0,
        wait_count: int = 0,
        now: datetime | None = None,
    ) -> None:
        """Record several counters for one tenant in one bucket."""
        for metric, n in increments.items():
            if n:
                self._record_metric(
                    conn, tenant_id=tenant_id, metric=metric, count=n, now=now
                )
        if wait_count:
            self._record_metric(
                conn,
                tenant_id=tenant_id,
                metric=METRIC_WAIT_BEFORE_CLAIM,
                count=wait_count,
                sum_ms=wait_sum_ms,
                now=now,
            )

    def sample_metrics(
        self,
        *,
        tenant_id: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Persist gauge snapshots for one tenant or all known tenants + overall."""
        ts = now or _utcnow()
        with self._pool.connection() as conn:
            if tenant_id:
                tenants = [tenant_id]
            else:
                rows = conn.execute(
                    """
                    SELECT tenant_id FROM jobs
                    UNION
                    SELECT tenant_id FROM tenant_settings
                    UNION
                    SELECT tenant_id FROM webhook_endpoints
                    UNION
                    SELECT tenant_id FROM schedules
                    ORDER BY tenant_id
                    """
                ).fetchall()
                tenants = [r["tenant_id"] for r in rows]
            out: list[dict[str, Any]] = []
            for tid in tenants:
                snap = self._compute_gauge_snapshot(conn, tenant_id=tid, now=ts)
                self._insert_snapshot(conn, snap)
                out.append(snap)
            overall = self._compute_gauge_snapshot(conn, tenant_id=None, now=ts)
            overall["tenant_id"] = OVERALL_TENANT
            self._insert_snapshot(conn, overall)
            out.append(overall)
            conn.commit()
        return out

    def _insert_snapshot(self, conn: Any, snap: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO queue_metric_snapshots (
                sampled_at, tenant_id, pending, leased, claimable, failed, dead,
                completed, cancelled, awaiting_approval, awaiting_children,
                queue_lag_seconds, avg_pending_age_seconds, max_pending_age_seconds,
                webhook_pending, webhook_delivering, webhook_succeeded, webhook_dead,
                schedules_enabled, schedules_due
            ) VALUES (
                %(sampled_at)s, %(tenant_id)s, %(pending)s, %(leased)s, %(claimable)s,
                %(failed)s, %(dead)s, %(completed)s, %(cancelled)s,
                %(awaiting_approval)s, %(awaiting_children)s,
                %(queue_lag_seconds)s, %(avg_pending_age_seconds)s,
                %(max_pending_age_seconds)s,
                %(webhook_pending)s, %(webhook_delivering)s, %(webhook_succeeded)s,
                %(webhook_dead)s, %(schedules_enabled)s, %(schedules_due)s
            )
            ON CONFLICT (sampled_at, tenant_id) DO UPDATE SET
                pending = EXCLUDED.pending,
                leased = EXCLUDED.leased,
                claimable = EXCLUDED.claimable,
                failed = EXCLUDED.failed,
                dead = EXCLUDED.dead,
                completed = EXCLUDED.completed,
                cancelled = EXCLUDED.cancelled,
                awaiting_approval = EXCLUDED.awaiting_approval,
                awaiting_children = EXCLUDED.awaiting_children,
                queue_lag_seconds = EXCLUDED.queue_lag_seconds,
                avg_pending_age_seconds = EXCLUDED.avg_pending_age_seconds,
                max_pending_age_seconds = EXCLUDED.max_pending_age_seconds,
                webhook_pending = EXCLUDED.webhook_pending,
                webhook_delivering = EXCLUDED.webhook_delivering,
                webhook_succeeded = EXCLUDED.webhook_succeeded,
                webhook_dead = EXCLUDED.webhook_dead,
                schedules_enabled = EXCLUDED.schedules_enabled,
                schedules_due = EXCLUDED.schedules_due
            """,
            snap,
        )

    def _compute_gauge_snapshot(
        self,
        conn: Any,
        *,
        tenant_id: str | None,
        now: datetime,
    ) -> dict[str, Any]:
        params: list[Any] = []
        tenant_clause = ""
        if tenant_id is not None:
            tenant_clause = " AND tenant_id = %s"
            params.append(tenant_id)

        counts = conn.execute(
            f"""
            SELECT
              COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
              COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
              COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
              COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
              COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
              COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
              COUNT(*) FILTER (WHERE status = 'awaiting_approval')::int AS awaiting_approval,
              COUNT(*) FILTER (WHERE status = 'awaiting_children')::int AS awaiting_children
            FROM jobs
            WHERE TRUE {tenant_clause}
            """,
            tuple(params),
        ).fetchone()
        assert counts is not None

        lag_params: list[Any] = [now, now, now, now]
        lag_tenant = ""
        if tenant_id is not None:
            lag_tenant = " AND tenant_id = %s"
            lag_params.append(tenant_id)
        lag = conn.execute(
            f"""
            SELECT
              EXTRACT(EPOCH FROM (%s - MIN(available_at)))::float AS queue_lag_seconds,
              EXTRACT(EPOCH FROM AVG(%s - available_at))::float AS avg_pending_age_seconds,
              EXTRACT(EPOCH FROM MAX(%s - available_at))::float AS max_pending_age_seconds
            FROM jobs
            WHERE status = 'pending'
              AND available_at <= %s
              {lag_tenant}
            """,
            tuple(lag_params),
        ).fetchone()

        claimable = 0
        if tenant_id is not None:
            paused = self._is_tenant_paused(conn, tenant_id)
            if not paused:
                crow = conn.execute(
                    f"""
                    SELECT COUNT(*)::int AS n
                    FROM jobs
                    WHERE tenant_id = %s
                      AND status = 'pending'
                      AND available_at <= %s
                      AND {_DEPS_SATISFIED_SQL}
                    """,
                    (tenant_id, now),
                ).fetchone()
                claimable = int(crow["n"]) if crow else 0
        else:
            crow = conn.execute(
                f"""
                SELECT COUNT(*)::int AS n
                FROM jobs
                WHERE status = 'pending'
                  AND available_at <= %s
                  AND {_DEPS_SATISFIED_SQL}
                  AND NOT EXISTS (
                    SELECT 1 FROM tenant_settings ts
                    WHERE ts.tenant_id = jobs.tenant_id AND ts.paused = TRUE
                  )
                """,
                (now,),
            ).fetchone()
            claimable = int(crow["n"]) if crow else 0

        wh_params: list[Any] = []
        wh_clause = ""
        if tenant_id is not None:
            wh_clause = " WHERE tenant_id = %s"
            wh_params.append(tenant_id)
        wh = conn.execute(
            f"""
            SELECT
              COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
              COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
              COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
              COUNT(*) FILTER (WHERE status = 'dead')::int AS dead
            FROM webhook_deliveries
            {wh_clause}
            """,
            tuple(wh_params),
        ).fetchone()
        assert wh is not None

        sch_params: list[Any] = [now]
        sch_clause = ""
        if tenant_id is not None:
            sch_clause = " AND tenant_id = %s"
            sch_params.append(tenant_id)
        sch = conn.execute(
            f"""
            SELECT
              COUNT(*) FILTER (WHERE enabled)::int AS enabled,
              COUNT(*) FILTER (WHERE enabled AND next_run_at <= %s)::int AS due
            FROM schedules
            WHERE TRUE {sch_clause}
            """,
            tuple(sch_params),
        ).fetchone()
        assert sch is not None

        def _f(v: Any) -> float | None:
            if v is None:
                return None
            return float(v)

        return {
            "sampled_at": now,
            "tenant_id": tenant_id if tenant_id is not None else OVERALL_TENANT,
            "pending": int(counts["pending"]),
            "leased": int(counts["leased"]),
            "claimable": claimable,
            "failed": int(counts["failed"]),
            "dead": int(counts["dead"]),
            "completed": int(counts["completed"]),
            "cancelled": int(counts["cancelled"]),
            "awaiting_approval": int(counts["awaiting_approval"]),
            "awaiting_children": int(counts["awaiting_children"]),
            "queue_lag_seconds": _f(lag["queue_lag_seconds"]) if lag else None,
            "avg_pending_age_seconds": _f(lag["avg_pending_age_seconds"])
            if lag
            else None,
            "max_pending_age_seconds": _f(lag["max_pending_age_seconds"])
            if lag
            else None,
            "webhook_pending": int(wh["pending"]),
            "webhook_delivering": int(wh["delivering"]),
            "webhook_succeeded": int(wh["succeeded"]),
            "webhook_dead": int(wh["dead"]),
            "schedules_enabled": int(sch["enabled"]),
            "schedules_due": int(sch["due"]),
        }

    def get_metrics_snapshot(
        self,
        *,
        tenant_id: str | None = None,
        window_seconds: int = 300,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Live gauges + throughput over *window_seconds* (default 5m)."""
        if window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")
        ts = now or _utcnow()
        window_start = ts - timedelta(seconds=window_seconds)
        with self._pool.connection() as conn:
            gauges = self._compute_gauge_snapshot(conn, tenant_id=tenant_id, now=ts)
            rates = self._throughput_window(
                conn,
                tenant_id=tenant_id,
                start=window_start,
                end=ts,
            )
            tenants = None
            if tenant_id is None:
                tenants = self._per_tenant_throughput(
                    conn, start=window_start, end=ts, now=ts
                )
        return {
            "as_of": ts.isoformat(),
            "window_seconds": window_seconds,
            "scope": tenant_id or "overall",
            "gauges": {k: v for k, v in gauges.items() if k not in ("sampled_at",)},
            "throughput": rates,
            "tenants": tenants,
            "noisy": self._rank_noisy(rates if tenant_id else None, tenants),
        }

    def _throughput_window(
        self,
        conn: Any,
        *,
        tenant_id: str | None,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        params: list[Any] = [start, end]
        clause = "bucket_start >= %s AND bucket_start < %s"
        if tenant_id is not None:
            clause += " AND tenant_id = %s"
            params.append(tenant_id)
        rows = conn.execute(
            f"""
            SELECT metric,
                   COALESCE(SUM(count), 0)::bigint AS count,
                   COALESCE(SUM(sum_ms), 0)::float AS sum_ms
            FROM queue_metric_buckets
            WHERE {clause}
            GROUP BY metric
            """,
            tuple(params),
        ).fetchall()
        out: dict[str, Any] = {
            m: {"count": 0, "rate_per_sec": 0.0} for m in _COUNTER_METRICS
        }
        secs = max((end - start).total_seconds(), 1e-9)
        for r in rows:
            m = r["metric"]
            c = int(r["count"])
            entry: dict[str, Any] = {
                "count": c,
                "rate_per_sec": c / secs,
            }
            if m == METRIC_WAIT_BEFORE_CLAIM and c > 0:
                entry["avg_ms"] = float(r["sum_ms"]) / c
                entry["sum_ms"] = float(r["sum_ms"])
            out[m] = entry
        return out

    def _per_tenant_throughput(
        self,
        conn: Any,
        *,
        start: datetime,
        end: datetime,
        now: datetime,
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT tenant_id, metric,
                   COALESCE(SUM(count), 0)::bigint AS count,
                   COALESCE(SUM(sum_ms), 0)::float AS sum_ms
            FROM queue_metric_buckets
            WHERE bucket_start >= %s AND bucket_start < %s
            GROUP BY tenant_id, metric
            """,
            (start, end),
        ).fetchall()
        by_tenant: dict[str, dict[str, Any]] = {}
        secs = max((end - start).total_seconds(), 1e-9)
        for r in rows:
            tid = r["tenant_id"]
            slot = by_tenant.setdefault(tid, {"tenant_id": tid, "throughput": {}})
            m = r["metric"]
            c = int(r["count"])
            entry: dict[str, Any] = {"count": c, "rate_per_sec": c / secs}
            if m == METRIC_WAIT_BEFORE_CLAIM and c > 0:
                entry["avg_ms"] = float(r["sum_ms"]) / c
            slot["throughput"][m] = entry
        # Attach live in-flight for each tenant that appears in counters or jobs.
        job_tenants = conn.execute(
            "SELECT DISTINCT tenant_id FROM jobs ORDER BY tenant_id"
        ).fetchall()
        for r in job_tenants:
            by_tenant.setdefault(
                r["tenant_id"], {"tenant_id": r["tenant_id"], "throughput": {}}
            )
        out = []
        for tid, slot in sorted(by_tenant.items()):
            gauges = self._compute_gauge_snapshot(conn, tenant_id=tid, now=now)
            slot["in_flight"] = int(gauges["leased"])
            slot["pending"] = int(gauges["pending"])
            slot["queue_lag_seconds"] = gauges["queue_lag_seconds"]
            slot["dead"] = int(gauges["dead"])
            # Noise score: sum of high-volume rates + lag weight.
            thr = slot["throughput"]
            score = 0.0
            for m in _NOISY_METRICS:
                score += float(thr.get(m, {}).get("rate_per_sec") or 0.0)
            lag = gauges["queue_lag_seconds"] or 0.0
            score += float(lag) / 10.0
            slot["noise_score"] = score
            out.append(slot)
        out.sort(key=lambda x: (-float(x["noise_score"]), x["tenant_id"]))
        return out

    def _rank_noisy(
        self,
        overall_rates: dict[str, Any] | None,
        tenants: list[dict[str, Any]] | None,
    ) -> dict[str, Any]:
        noisy_metrics: list[dict[str, Any]] = []
        if overall_rates:
            for m in sorted(_NOISY_METRICS):
                info = overall_rates.get(m) or {}
                rate = float(info.get("rate_per_sec") or 0.0)
                if rate > 0:
                    noisy_metrics.append(
                        {
                            "metric": m,
                            "rate_per_sec": rate,
                            "count": int(info.get("count") or 0),
                        }
                    )
            noisy_metrics.sort(key=lambda x: -x["rate_per_sec"])
        top_tenants = []
        if tenants:
            top_tenants = [
                {
                    "tenant_id": t["tenant_id"],
                    "noise_score": t["noise_score"],
                    "lease_rate": float(
                        (t.get("throughput") or {})
                        .get(METRIC_LEASE, {})
                        .get("rate_per_sec")
                        or 0.0
                    ),
                    "ack_rate": float(
                        (t.get("throughput") or {})
                        .get(METRIC_ACK, {})
                        .get("rate_per_sec")
                        or 0.0
                    ),
                    "queue_lag_seconds": t.get("queue_lag_seconds"),
                    "in_flight": t.get("in_flight"),
                }
                for t in tenants[:20]
                if float(t.get("noise_score") or 0) > 0
            ]
        return {
            "metrics": noisy_metrics,
            "tenants": top_tenants,
        }

    def get_metrics_history(
        self,
        *,
        tenant_id: str | None = None,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        include_snapshots: bool = True,
    ) -> dict[str, Any]:
        """Return time-bucket counter series and optional gauge snapshots."""
        now = _utcnow()
        end_dt = _parse_dt(end, default=now)
        start_dt = _parse_dt(start, default=end_dt - timedelta(hours=1))
        if start_dt >= end_dt:
            raise ValueError("start must be before end")
        with self._pool.connection() as conn:
            params: list[Any] = [start_dt, end_dt]
            clause = "bucket_start >= %s AND bucket_start < %s"
            if tenant_id is not None:
                clause += " AND tenant_id = %s"
                params.append(tenant_id)
            buckets = conn.execute(
                f"""
                SELECT bucket_start, tenant_id, metric, count, sum_ms
                FROM queue_metric_buckets
                WHERE {clause}
                ORDER BY bucket_start ASC, tenant_id ASC, metric ASC
                """,
                tuple(params),
            ).fetchall()
            series = [
                {
                    "bucket_start": r["bucket_start"].isoformat(),
                    "tenant_id": r["tenant_id"],
                    "metric": r["metric"],
                    "count": int(r["count"]),
                    "sum_ms": float(r["sum_ms"]),
                    "avg_ms": (float(r["sum_ms"]) / int(r["count"]))
                    if int(r["count"]) > 0 and r["metric"] == METRIC_WAIT_BEFORE_CLAIM
                    else None,
                }
                for r in buckets
            ]
            snapshots: list[dict[str, Any]] = []
            if include_snapshots:
                sparams: list[Any] = [start_dt, end_dt]
                sclause = "sampled_at >= %s AND sampled_at <= %s"
                if tenant_id is not None:
                    sclause += " AND tenant_id = %s"
                    sparams.append(tenant_id)
                else:
                    # overall + all tenants when no filter
                    pass
                srows = conn.execute(
                    f"""
                    SELECT *
                    FROM queue_metric_snapshots
                    WHERE {sclause}
                    ORDER BY sampled_at ASC, tenant_id ASC
                    """,
                    tuple(sparams),
                ).fetchall()
                for r in srows:
                    d = dict(r)
                    if d.get("sampled_at") is not None:
                        d["sampled_at"] = d["sampled_at"].isoformat()
                    snapshots.append(d)
        return {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "scope": tenant_id or "overall+per_tenant",
            "buckets": series,
            "snapshots": snapshots,
        }

    def get_metrics_noisy(
        self,
        *,
        window_seconds: int = 300,
        min_rate: float = 0.5,
        limit: int = 50,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Surface high-volume tenants/metrics over a recent window."""
        snap = self.get_metrics_snapshot(
            tenant_id=None, window_seconds=window_seconds, now=now
        )
        tenants = [
            t
            for t in (snap.get("tenants") or [])
            if float(t.get("noise_score") or 0) >= min_rate
        ][:limit]
        metrics = [
            m
            for m in (snap.get("noisy") or {}).get("metrics") or []
            if float(m.get("rate_per_sec") or 0) >= min_rate
        ]
        return {
            "as_of": snap["as_of"],
            "window_seconds": window_seconds,
            "min_rate": min_rate,
            "noisy_metrics": metrics,
            "noisy_tenants": tenants,
        }

    def export_metrics(
        self,
        *,
        kind: str,
        format: str = "json",
        tenant_id: str | None = None,
        window_seconds: int = 300,
        start: datetime | str | None = None,
        end: datetime | str | None = None,
        min_rate: float = 0.5,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Build a metrics payload and optionally flatten for CSV/Parquet."""
        kind = kind.lower().strip()
        fmt = format.lower().strip()
        if fmt not in ("json", "csv", "parquet"):
            raise ValueError("format must be json, csv, or parquet")
        if kind == "snapshot":
            data = self.get_metrics_snapshot(
                tenant_id=tenant_id, window_seconds=window_seconds
            )
            rows = self._flatten_snapshot_rows(data)
        elif kind == "history":
            data = self.get_metrics_history(tenant_id=tenant_id, start=start, end=end)
            rows = list(data.get("buckets") or [])
        elif kind == "noisy":
            data = self.get_metrics_noisy(
                window_seconds=window_seconds, min_rate=min_rate, limit=limit
            )
            rows = list(data.get("noisy_tenants") or [])
        else:
            raise ValueError("kind must be snapshot, history, or noisy")

        if fmt == "json":
            return {"format": "json", "content_type": "application/json", "data": data}

        if fmt == "csv":
            body = self._rows_to_csv(rows)
            return {
                "format": "csv",
                "content_type": "text/csv; charset=utf-8",
                "body": body.encode("utf-8"),
                "data": data,
            }

        # parquet
        body = self._rows_to_parquet(rows)
        return {
            "format": "parquet",
            "content_type": "application/vnd.apache.parquet",
            "body": body,
            "data": data,
        }

    def _flatten_snapshot_rows(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        thr = data.get("throughput") or {}
        gauges = data.get("gauges") or {}
        base = {
            "as_of": data.get("as_of"),
            "scope": data.get("scope"),
            "window_seconds": data.get("window_seconds"),
            **{f"gauge_{k}": v for k, v in gauges.items() if k != "tenant_id"},
        }
        for metric, info in thr.items():
            rows.append(
                {
                    **base,
                    "metric": metric,
                    "count": info.get("count"),
                    "rate_per_sec": info.get("rate_per_sec"),
                    "avg_ms": info.get("avg_ms"),
                }
            )
        if not rows:
            rows.append(base)
        # per-tenant noise summary when present
        for t in data.get("tenants") or []:
            rows.append(
                {
                    "as_of": data.get("as_of"),
                    "scope": "tenant",
                    "tenant_id": t.get("tenant_id"),
                    "noise_score": t.get("noise_score"),
                    "in_flight": t.get("in_flight"),
                    "pending": t.get("pending"),
                    "queue_lag_seconds": t.get("queue_lag_seconds"),
                    "dead": t.get("dead"),
                    "lease_rate": (t.get("throughput") or {})
                    .get(METRIC_LEASE, {})
                    .get("rate_per_sec"),
                    "ack_rate": (t.get("throughput") or {})
                    .get(METRIC_ACK, {})
                    .get("rate_per_sec"),
                }
            )
        return rows

    def _rows_to_csv(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return ""
        # union of keys
        keys: list[str] = []
        seen: set[str] = set()
        for r in rows:
            for k in r:
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            flat = {}
            for k, v in r.items():
                if isinstance(v, (dict, list)):
                    flat[k] = json.dumps(v, default=str)
                else:
                    flat[k] = v
            w.writerow(flat)
        return buf.getvalue()

    def _rows_to_parquet(self, rows: list[dict[str, Any]]) -> bytes:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError(
                "parquet export requires pyarrow; install job-queue[metrics] or pyarrow"
            ) from exc
        if not rows:
            table = pa.table({"_empty": pa.array([], type=pa.int64())})
        else:
            # normalize keys
            keys: list[str] = []
            seen: set[str] = set()
            for r in rows:
                for k in r:
                    if k not in seen:
                        seen.add(k)
                        keys.append(k)
            cols: dict[str, list[Any]] = {k: [] for k in keys}
            for r in rows:
                for k in keys:
                    v = r.get(k)
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, default=str)
                    cols[k].append(v)
            table = pa.table(cols)
        sink = io.BytesIO()
        pq.write_table(table, sink)
        return sink.getvalue()
