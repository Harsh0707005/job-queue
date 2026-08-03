"""Metric threshold alerts with durable history and webhook delivery.

Rules evaluate against live gauges / throughput windows (and worker-level
lease counts). When a rule fires, an ``alert_events`` row is appended and
``alert.fired`` webhook deliveries are enqueued (same outbox as job events).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import AlertEvent, AlertRule, WebhookEvent
from job_queue.queue._common import _utcnow
from job_queue.queue.metrics import OVERALL_TENANT

# Supported metric names for alert rules.
ALERT_METRICS = frozenset(
    {
        "queue_lag_seconds",
        "dead_count",
        "dead_letter_lag_seconds",
        "failed_count",
        "leased_count",
        "pending_count",
        "webhook_fail_rate",
        "webhook_fail_count",
        "nack_rate",
        "nack_count",
        "dead_rate",
        "worker_leased_count",
    }
)

_ALERT_RULE_COLUMNS = """
    id, name, enabled, scope, tenant_id, worker_id, metric, comparator,
    threshold, window_seconds, cooldown_seconds, last_fired_at,
    created_at, updated_at
"""

_ALERT_EVENT_COLUMNS = """
    id, rule_id, rule_name, metric, scope, tenant_id, worker_id,
    value, threshold, comparator, window_seconds, detail, fired_at
"""

# Synthetic tenant used for overall-scoped alert webhook subscriptions.
ALERTS_OVERALL_TENANT = "__overall__"


class AlertsMixin:
    def create_alert_rule(
        self,
        *,
        name: str,
        metric: str,
        threshold: float,
        comparator: str = "gt",
        scope: str = "overall",
        tenant_id: str | None = None,
        worker_id: str | None = None,
        window_seconds: int = 300,
        cooldown_seconds: int = 300,
        enabled: bool = True,
        rule_id: UUID | None = None,
    ) -> AlertRule:
        if not name or not str(name).strip():
            raise ValueError("name is required")
        metric = str(metric).strip()
        if metric not in ALERT_METRICS:
            raise ValueError(
                f"unsupported metric {metric!r}; choose from {sorted(ALERT_METRICS)}"
            )
        comparator = str(comparator).strip().lower()
        if comparator not in ("gt", "gte", "lt", "lte"):
            raise ValueError("comparator must be gt, gte, lt, or lte")
        scope = str(scope).strip().lower()
        if scope not in ("overall", "tenant", "worker"):
            raise ValueError("scope must be overall, tenant, or worker")
        if scope == "tenant":
            if not tenant_id or not str(tenant_id).strip():
                raise ValueError("tenant_id required for scope=tenant")
            tenant_id = str(tenant_id).strip()
            worker_id = None
        elif scope == "worker":
            if not worker_id or not str(worker_id).strip():
                raise ValueError("worker_id required for scope=worker")
            worker_id = str(worker_id).strip()
            tenant_id = str(tenant_id).strip() if tenant_id else None
            if metric not in ("worker_leased_count", "leased_count"):
                # Allow leased_count as alias for worker scope.
                if metric != "worker_leased_count":
                    raise ValueError(
                        "worker scope supports metric worker_leased_count "
                        "(or leased_count)"
                    )
                metric = "worker_leased_count"
        else:
            tenant_id = None
            worker_id = None
        if window_seconds < 1:
            raise ValueError("window_seconds must be >= 1")
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")
        now = _utcnow()
        rid = rule_id or uuid4()
        try:
            with transaction(self._pool) as conn:
                row = conn.execute(
                    f"""
                    INSERT INTO alert_rules (
                        id, name, enabled, scope, tenant_id, worker_id, metric,
                        comparator, threshold, window_seconds, cooldown_seconds,
                        last_fired_at, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        NULL, %s, %s
                    )
                    RETURNING {_ALERT_RULE_COLUMNS}
                    """,
                    (
                        rid,
                        str(name).strip(),
                        bool(enabled),
                        scope,
                        tenant_id,
                        worker_id,
                        metric,
                        comparator,
                        float(threshold),
                        int(window_seconds),
                        int(cooldown_seconds),
                        now,
                        now,
                    ),
                ).fetchone()
        except Exception as exc:
            # Unique name / constraint → clean ValueError for API 400.
            msg = str(exc).lower()
            if "unique" in msg or "duplicate" in msg:
                raise ValueError(f"alert rule name already exists: {name!r}") from exc
            raise
        assert row is not None
        return self._row_to_alert_rule(row)

    def list_alert_rules(self, *, enabled_only: bool = False) -> list[AlertRule]:
        with self._pool.connection() as conn:
            if enabled_only:
                rows = conn.execute(
                    f"""
                    SELECT {_ALERT_RULE_COLUMNS}
                    FROM alert_rules
                    WHERE enabled = TRUE
                    ORDER BY name ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT {_ALERT_RULE_COLUMNS}
                    FROM alert_rules
                    ORDER BY name ASC
                    """
                ).fetchall()
        return [self._row_to_alert_rule(r) for r in rows]

    def get_alert_rule(self, rule_id: UUID) -> AlertRule | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_ALERT_RULE_COLUMNS}
                FROM alert_rules WHERE id = %s
                """,
                (rule_id,),
            ).fetchone()
        return self._row_to_alert_rule(row) if row else None

    def delete_alert_rule(self, rule_id: UUID) -> bool:
        with transaction(self._pool) as conn:
            row = conn.execute(
                "DELETE FROM alert_rules WHERE id = %s RETURNING id",
                (rule_id,),
            ).fetchone()
        return row is not None

    def set_alert_rule_enabled(self, rule_id: UUID, *, enabled: bool) -> AlertRule:
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE alert_rules
                SET enabled = %s, updated_at = %s
                WHERE id = %s
                RETURNING {_ALERT_RULE_COLUMNS}
                """,
                (bool(enabled), now, rule_id),
            ).fetchone()
        if row is None:
            raise ValueError(f"alert rule {rule_id} not found")
        return self._row_to_alert_rule(row)

    def list_alert_events(
        self,
        *,
        rule_id: UUID | None = None,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AlertEvent]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        clauses: list[str] = []
        params: list[Any] = []
        if rule_id is not None:
            clauses.append("rule_id = %s")
            params.append(rule_id)
        if tenant_id is not None:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.extend([limit, offset])
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_ALERT_EVENT_COLUMNS}
                FROM alert_events
                {where}
                ORDER BY fired_at DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            ).fetchall()
        return [self._row_to_alert_event(r) for r in rows]

    def evaluate_alerts(self, *, now: datetime | None = None) -> list[AlertEvent]:
        """Evaluate all enabled rules; fire + record + webhook when breached."""
        ts = now or _utcnow()
        rules = self.list_alert_rules(enabled_only=True)
        fired: list[AlertEvent] = []
        for rule in rules:
            value, detail = self._compute_alert_value(rule, now=ts)
            if value is None:
                continue
            if not self._compare(value, rule.comparator, rule.threshold):
                continue
            if rule.last_fired_at is not None and rule.cooldown_seconds > 0:
                cool_until = rule.last_fired_at + timedelta(
                    seconds=rule.cooldown_seconds
                )
                if ts < cool_until:
                    continue
            ev = self._fire_alert(rule, value=float(value), detail=detail, now=ts)
            fired.append(ev)
        return fired

    def _compute_alert_value(
        self, rule: AlertRule, *, now: datetime
    ) -> tuple[float | None, dict[str, Any]]:
        metric = rule.metric
        window = rule.window_seconds
        detail: dict[str, Any] = {"evaluated_at": now.isoformat()}

        if rule.scope == "worker" or metric == "worker_leased_count":
            worker_id = rule.worker_id
            if not worker_id:
                return None, detail
            with self._pool.connection() as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*)::int AS n
                    FROM jobs
                    WHERE status = 'leased'
                      AND leased_by = %s
                    """,
                    (worker_id,),
                ).fetchone()
            n = float(row["n"] if row else 0)
            detail["worker_id"] = worker_id
            detail["leased_count"] = n
            return n, detail

        tenant_id = rule.tenant_id if rule.scope == "tenant" else None
        snap = self.get_metrics_snapshot(
            tenant_id=tenant_id, window_seconds=window, now=now
        )
        gauges = snap.get("gauges") or {}
        thr = snap.get("throughput") or {}
        detail["scope"] = rule.scope
        if tenant_id:
            detail["tenant_id"] = tenant_id

        if metric == "queue_lag_seconds":
            v = gauges.get("queue_lag_seconds")
            return (float(v) if v is not None else 0.0), detail
        if metric == "dead_count":
            return float(gauges.get("dead") or 0), detail
        if metric == "failed_count":
            return float(gauges.get("failed") or 0), detail
        if metric == "leased_count":
            return float(gauges.get("leased") or 0), detail
        if metric == "pending_count":
            return float(gauges.get("pending") or 0), detail
        if metric == "webhook_fail_count":
            return float((thr.get("webhook_fail") or {}).get("count") or 0), detail
        if metric == "nack_count":
            return float((thr.get("nack") or {}).get("count") or 0), detail
        if metric == "dead_rate":
            return float((thr.get("dead") or {}).get("rate_per_sec") or 0.0), detail
        if metric == "nack_rate":
            return float((thr.get("nack") or {}).get("rate_per_sec") or 0.0), detail
        if metric == "webhook_fail_rate":
            fails = float((thr.get("webhook_fail") or {}).get("count") or 0)
            ok = float((thr.get("webhook_success") or {}).get("count") or 0)
            total = fails + ok
            rate = (fails / total) if total > 0 else 0.0
            detail["webhook_fail"] = fails
            detail["webhook_success"] = ok
            detail["window_seconds"] = window
            return rate, detail
        if metric == "dead_letter_lag_seconds":
            lag = self._dead_letter_lag_seconds(tenant_id=tenant_id, now=now)
            detail["dead_letter_lag_seconds"] = lag
            return lag, detail
        return None, detail

    def _dead_letter_lag_seconds(
        self, *, tenant_id: str | None, now: datetime
    ) -> float:
        params: list[Any] = [now]
        clause = ""
        if tenant_id is not None:
            clause = " AND tenant_id = %s"
            params.append(tenant_id)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT EXTRACT(
                    EPOCH FROM (%s - MIN(COALESCE(completed_at, updated_at)))
                )::float AS lag
                FROM jobs
                WHERE status = 'dead'
                {clause}
                """,
                tuple(params),
            ).fetchone()
        if row is None or row["lag"] is None:
            return 0.0
        return max(0.0, float(row["lag"]))

    @staticmethod
    def _compare(value: float, comparator: str, threshold: float) -> bool:
        if comparator == "gt":
            return value > threshold
        if comparator == "gte":
            return value >= threshold
        if comparator == "lt":
            return value < threshold
        if comparator == "lte":
            return value <= threshold
        return False

    def _fire_alert(
        self,
        rule: AlertRule,
        *,
        value: float,
        detail: dict[str, Any],
        now: datetime,
    ) -> AlertEvent:
        event_id = uuid4()
        payload_detail = dict(detail)
        payload_detail.update(
            {
                "rule_id": str(rule.id),
                "rule_name": rule.name,
                "metric": rule.metric,
                "value": value,
                "threshold": rule.threshold,
                "comparator": rule.comparator,
            }
        )
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                INSERT INTO alert_events (
                    id, rule_id, rule_name, metric, scope, tenant_id, worker_id,
                    value, threshold, comparator, window_seconds, detail, fired_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                RETURNING {_ALERT_EVENT_COLUMNS}
                """,
                (
                    event_id,
                    rule.id,
                    rule.name,
                    rule.metric,
                    rule.scope,
                    rule.tenant_id,
                    rule.worker_id,
                    value,
                    rule.threshold,
                    rule.comparator,
                    rule.window_seconds,
                    Jsonb(payload_detail),
                    now,
                ),
            ).fetchone()
            conn.execute(
                """
                UPDATE alert_rules
                SET last_fired_at = %s, updated_at = %s
                WHERE id = %s
                """,
                (now, now, rule.id),
            )
            self._enqueue_alert_webhooks(
                conn,
                event_id=event_id,
                rule=rule,
                value=value,
                detail=payload_detail,
                now=now,
            )
        assert row is not None
        return self._row_to_alert_event(row)

    def _enqueue_alert_webhooks(
        self,
        conn: Any,
        *,
        event_id: UUID,
        rule: AlertRule,
        value: float,
        detail: dict[str, Any],
        now: datetime,
    ) -> None:
        """Enqueue alert.fired deliveries to matching webhook endpoints."""
        event = WebhookEvent.ALERT_FIRED.value
        # Tenant-scoped: that tenant's endpoints. Overall: __overall__ tenant.
        # Worker-scoped: tenant endpoints if tenant set, else overall.
        if rule.scope == "tenant" and rule.tenant_id:
            tenants = [rule.tenant_id]
        elif rule.scope == "worker" and rule.tenant_id:
            tenants = [rule.tenant_id]
        else:
            tenants = [ALERTS_OVERALL_TENANT]

        for tid in tenants:
            if self._is_tenant_paused(conn, tid):
                # Still allow overall alert channel even if "__overall__" has no settings.
                if tid != ALERTS_OVERALL_TENANT:
                    continue
            endpoints = conn.execute(
                f"""
                SELECT {self._WEBHOOK_ENDPOINT_COLUMNS}
                FROM webhook_endpoints
                WHERE tenant_id = %s
                  AND enabled = TRUE
                  AND %s = ANY(events)
                """,
                (tid, event),
            ).fetchall()
            for ep in endpoints:
                delivery_id = uuid4()
                payload = {
                    "event": event,
                    "delivery_id": str(delivery_id),
                    "alert_event_id": str(event_id),
                    "rule_id": str(rule.id),
                    "rule_name": rule.name,
                    "metric": rule.metric,
                    "scope": rule.scope,
                    "tenant_id": rule.tenant_id,
                    "worker_id": rule.worker_id,
                    "value": value,
                    "threshold": rule.threshold,
                    "comparator": rule.comparator,
                    "window_seconds": rule.window_seconds,
                    "detail": detail,
                    "timestamp": now.isoformat(),
                }
                conn.execute(
                    f"""
                    INSERT INTO webhook_deliveries (
                        id, endpoint_id, tenant_id, job_id, event, payload,
                        status, attempts, max_attempts, backoff_base_seconds,
                        backoff_max_seconds, next_attempt_at, signing_secret,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, NULL, %s, %s,
                        'pending', 0, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        delivery_id,
                        ep["id"],
                        tid,
                        event,
                        Jsonb(payload),
                        ep["max_attempts"],
                        ep["backoff_base_seconds"],
                        ep["backoff_max_seconds"],
                        now,
                        ep.get("secret"),
                        now,
                        now,
                    ),
                )

    def _row_to_alert_rule(self, row: dict[str, Any]) -> AlertRule:
        return AlertRule(
            id=row["id"],
            name=row["name"],
            enabled=bool(row["enabled"]),
            scope=row["scope"],
            tenant_id=row.get("tenant_id"),
            worker_id=row.get("worker_id"),
            metric=row["metric"],
            comparator=row["comparator"],
            threshold=float(row["threshold"]),
            window_seconds=int(row["window_seconds"]),
            cooldown_seconds=int(row["cooldown_seconds"]),
            last_fired_at=row.get("last_fired_at"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_alert_event(self, row: dict[str, Any]) -> AlertEvent:
        detail = row.get("detail") or {}
        if isinstance(detail, str):
            import json

            detail = json.loads(detail)
        return AlertEvent(
            id=row["id"],
            rule_id=row["rule_id"],
            rule_name=row["rule_name"],
            metric=row["metric"],
            scope=row["scope"],
            tenant_id=row.get("tenant_id"),
            worker_id=row.get("worker_id"),
            value=float(row["value"]),
            threshold=float(row["threshold"]),
            comparator=row["comparator"],
            window_seconds=row.get("window_seconds"),
            detail=dict(detail),
            fired_at=row["fired_at"],
        )
