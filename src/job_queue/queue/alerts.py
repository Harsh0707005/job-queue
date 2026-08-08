"""Metric threshold alerts with durable history and webhook delivery.

Rules evaluate against live gauges / throughput windows (and worker-level
lease counts). When a rule fires, an ``alert_events`` row is appended and
``alert.fired`` webhook deliveries are enqueued (same outbox as job events).
When an open event is resolved (manual or auto-recovery), ``alert.resolved``
deliveries are enqueued the same way.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import AlertEvaluationResult, AlertEvent, AlertRule, WebhookEvent
from job_queue.queue._common import _utcnow
from job_queue.queue.metrics import OVERALL_TENANT

# resolved_by marker for automatic recovery closes.
AUTO_RESOLVE_BY = "auto"

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
    muted_until, auto_pause_tenant, recovery_threshold, recovery_evaluations,
    healthy_interval_seconds, healthy_streak, last_healthy_at,
    created_at, updated_at
"""

_ALERT_EVENT_COLUMNS = """
    id, rule_id, rule_name, metric, scope, tenant_id, worker_id,
    value, threshold, comparator, window_seconds, detail, fired_at,
    resolved_at, resolved_by, resolution_note
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
        auto_pause_tenant: bool = False,
        recovery_threshold: float | None = None,
        recovery_evaluations: int = 2,
        healthy_interval_seconds: int = 1,
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
        auto_pause = bool(auto_pause_tenant)
        if auto_pause and scope != "tenant":
            raise ValueError("auto_pause_tenant is only valid for scope=tenant")
        rec_evals = int(recovery_evaluations)
        if rec_evals < 0:
            raise ValueError("recovery_evaluations must be >= 0 (0 disables auto-resolve)")
        healthy_iv = int(healthy_interval_seconds)
        if healthy_iv < 0:
            raise ValueError("healthy_interval_seconds must be >= 0")
        fire_threshold = float(threshold)
        if recovery_threshold is None:
            rec_threshold: float | None = None
        else:
            rec_threshold = float(recovery_threshold)
            self._validate_recovery_threshold(
                comparator=comparator,
                fire_threshold=fire_threshold,
                recovery_threshold=rec_threshold,
            )
        now = _utcnow()
        rid = rule_id or uuid4()
        try:
            with transaction(self._pool) as conn:
                row = conn.execute(
                    f"""
                    INSERT INTO alert_rules (
                        id, name, enabled, scope, tenant_id, worker_id, metric,
                        comparator, threshold, window_seconds, cooldown_seconds,
                        last_fired_at, muted_until, auto_pause_tenant,
                        recovery_threshold, recovery_evaluations,
                        healthy_interval_seconds, healthy_streak, last_healthy_at,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        NULL, NULL, %s,
                        %s, %s,
                        %s, 0, NULL,
                        %s, %s
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
                        fire_threshold,
                        int(window_seconds),
                        int(cooldown_seconds),
                        auto_pause,
                        rec_threshold,
                        rec_evals,
                        healthy_iv,
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

    @staticmethod
    def _validate_recovery_threshold(
        *,
        comparator: str,
        fire_threshold: float,
        recovery_threshold: float,
    ) -> None:
        """Ensure recovery threshold sits on the healthy side of the fire line."""
        if comparator in ("gt", "gte"):
            if recovery_threshold > fire_threshold:
                raise ValueError(
                    "recovery_threshold must be <= threshold for gt/gte "
                    "(hysteresis band below the fire line)"
                )
        elif comparator in ("lt", "lte"):
            if recovery_threshold < fire_threshold:
                raise ValueError(
                    "recovery_threshold must be >= threshold for lt/lte "
                    "(hysteresis band above the fire line)"
                )

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

    def mute_alert_rule(
        self,
        rule_id: UUID,
        *,
        duration_seconds: int | None = None,
        muted_until: datetime | None = None,
        now: datetime | None = None,
    ) -> AlertRule:
        """Silence a rule until ``muted_until`` (or now + duration_seconds).

        While muted, evaluation skips the rule (no new events / webhooks).
        """
        ts = now or _utcnow()
        if muted_until is not None and duration_seconds is not None:
            raise ValueError("pass duration_seconds or muted_until, not both")
        if muted_until is None:
            if duration_seconds is None:
                raise ValueError("duration_seconds or muted_until is required")
            if int(duration_seconds) < 1:
                raise ValueError("duration_seconds must be >= 1")
            until = ts + timedelta(seconds=int(duration_seconds))
        else:
            until = muted_until
            if until.tzinfo is None:
                # Treat naive as UTC wall time consistent with queue timestamps.
                from datetime import timezone

                until = until.replace(tzinfo=timezone.utc)
            if until <= ts:
                raise ValueError("muted_until must be in the future")
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE alert_rules
                SET muted_until = %s, updated_at = %s
                WHERE id = %s
                RETURNING {_ALERT_RULE_COLUMNS}
                """,
                (until, ts, rule_id),
            ).fetchone()
        if row is None:
            raise ValueError(f"alert rule {rule_id} not found")
        return self._row_to_alert_rule(row)

    def unmute_alert_rule(self, rule_id: UUID) -> AlertRule:
        """Clear mute so the rule evaluates again immediately."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE alert_rules
                SET muted_until = NULL, updated_at = %s
                WHERE id = %s
                RETURNING {_ALERT_RULE_COLUMNS}
                """,
                (now, rule_id),
            ).fetchone()
        if row is None:
            raise ValueError(f"alert rule {rule_id} not found")
        return self._row_to_alert_rule(row)

    def resolve_alert_event(
        self,
        event_id: UUID,
        *,
        tenant_id: str,
        resolved_by: str | None = None,
        note: str | None = None,
        now: datetime | None = None,
    ) -> AlertEvent:
        """Mark a history event resolved (tenant-scoped for isolation).

        On first resolve, enqueues ``alert.resolved`` webhook deliveries.
        Idempotent re-resolve returns the existing row without re-notifying.
        """
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        ts = now or _utcnow()
        by = str(resolved_by).strip() if resolved_by else None
        res_note = str(note).strip() if note else None
        newly_resolved = False
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE alert_events
                SET resolved_at = %s,
                    resolved_by = %s,
                    resolution_note = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND resolved_at IS NULL
                RETURNING {_ALERT_EVENT_COLUMNS}
                """,
                (ts, by, res_note, event_id, tenant_id),
            ).fetchone()
            if row is None:
                existing = conn.execute(
                    """
                    SELECT id, tenant_id, resolved_at
                    FROM alert_events
                    WHERE id = %s
                    """,
                    (event_id,),
                ).fetchone()
                if existing is None:
                    raise ValueError(f"alert event {event_id} not found")
                if existing["tenant_id"] != tenant_id:
                    raise ValueError(
                        f"alert event {event_id} not found for tenant {tenant_id!r}"
                    )
                # Already resolved — return current row (no re-notify).
                row = conn.execute(
                    f"""
                    SELECT {_ALERT_EVENT_COLUMNS}
                    FROM alert_events WHERE id = %s
                    """,
                    (event_id,),
                ).fetchone()
            else:
                newly_resolved = True
                self._enqueue_alert_resolved_webhooks(
                    conn, event_row=row, now=ts
                )
        assert row is not None
        # newly_resolved kept for clarity / future hooks
        _ = newly_resolved
        return self._row_to_alert_event(row)

    def list_alert_events(
        self,
        *,
        tenant_id: str,
        rule_id: UUID | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AlertEvent]:
        """List alert history for a single tenant (required for isolation).

        Overall / unscoped firings are stored under
        :data:`ALERTS_OVERALL_TENANT` (``__overall__``). Pass that value to
        read fleet-wide alert history.

        ``status`` may be ``open`` or ``resolved`` to filter history.
        """
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        clauses: list[str] = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if rule_id is not None:
            clauses.append("rule_id = %s")
            params.append(rule_id)
        if status is not None:
            st = str(status).strip().lower()
            if st == "open":
                clauses.append("resolved_at IS NULL")
            elif st == "resolved":
                clauses.append("resolved_at IS NOT NULL")
            else:
                raise ValueError("status must be open or resolved")
        where = "WHERE " + " AND ".join(clauses)
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

    def evaluate_alerts(
        self, *, now: datetime | None = None
    ) -> AlertEvaluationResult:
        """Evaluate all enabled rules; fire on breach, auto-resolve on recovery.

        Cooldown and event insert are claimed under a per-rule row lock so
        concurrent evaluators (multi-coordinator) cannot double-fire the same
        rule within the cooldown window. Muted rules (``muted_until > now``)
        are skipped. Tenant-scoped rules with ``auto_pause_tenant`` pause that
        tenant on fire to stop further overflow.

        Auto-resolve: when the metric is healthy vs ``recovery_threshold``
        (defaults to fire ``threshold``) for ``recovery_evaluations``
        *spaced* healthy ticks, all open history events for that rule are
        closed with ``resolved_by=auto``. Each healthy tick is only counted
        if at least ``healthy_interval_seconds`` has elapsed since
        ``last_healthy_at``, so concurrent multi-coordinator evaluates in the
        same window cannot inflate the streak and resolve instantly. The
        hysteresis band between fire and recovery thresholds also prevents
        flip-flop; set ``recovery_evaluations=0`` to disable auto-resolve.
        """
        ts = now or _utcnow()
        rules = self.list_alert_rules(enabled_only=True)
        fired: list[AlertEvent] = []
        resolved: list[AlertEvent] = []
        for rule in rules:
            if rule.muted_until is not None and ts < rule.muted_until:
                continue
            value, detail = self._compute_alert_value(rule, now=ts)
            if value is None:
                continue
            v = float(value)
            if self._compare(v, rule.comparator, rule.threshold):
                # Breach: reset healthy streak, then fire if cooldown allows.
                self._reset_healthy_progress(rule.id, now=ts)
                if rule.last_fired_at is not None and rule.cooldown_seconds > 0:
                    cool_until = rule.last_fired_at + timedelta(
                        seconds=rule.cooldown_seconds
                    )
                    if ts < cool_until:
                        continue
                ev = self._fire_alert(rule, value=v, detail=detail, now=ts)
                if ev is not None:
                    fired.append(ev)
                continue

            # Not breaching fire threshold — track recovery hysteresis.
            if rule.recovery_evaluations <= 0:
                continue
            closed = self._record_healthy_and_maybe_resolve(
                rule, value=v, now=ts
            )
            resolved.extend(closed)
        return AlertEvaluationResult(fired=fired, resolved=resolved)

    def _recovery_threshold_value(self, rule: AlertRule) -> float:
        if rule.recovery_threshold is not None:
            return float(rule.recovery_threshold)
        return float(rule.threshold)

    def _is_recovered(self, value: float, rule: AlertRule) -> bool:
        """True when value is on the healthy side of the recovery threshold.

        For gt/gte fire rules, healthy means value is strictly below (or at for
        gt) the recovery line. Uses inverted comparators so a recovery_threshold
        below the fire threshold creates a non-flapping hysteresis band.
        """
        rec = self._recovery_threshold_value(rule)
        comp = rule.comparator
        if comp == "gt":
            # Fire when value > T; recovered when value <= rec (rec <= T).
            return value <= rec
        if comp == "gte":
            # Fire when value >= T; recovered when value < rec (rec <= T).
            return value < rec
        if comp == "lt":
            # Fire when value < T; recovered when value >= rec (rec >= T).
            return value >= rec
        if comp == "lte":
            # Fire when value <= T; recovered when value > rec (rec >= T).
            return value > rec
        return False

    def _reset_healthy_progress(self, rule_id: UUID, *, now: datetime) -> None:
        """Clear healthy streak and last tick timestamp (breach / gray zone)."""
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE alert_rules
                SET healthy_streak = 0,
                    last_healthy_at = NULL,
                    updated_at = %s
                WHERE id = %s
                  AND (healthy_streak <> 0 OR last_healthy_at IS NOT NULL)
                """,
                (now, rule_id),
            )

    @staticmethod
    def _healthy_tick_due(
        *,
        last_healthy_at: datetime | None,
        interval_seconds: int,
        now: datetime,
    ) -> bool:
        """Whether enough time has passed to count another healthy tick.

        Concurrent evaluates that share the same ``now`` (or land inside the
        interval window) only produce one tick: the first writer wins and
        sets ``last_healthy_at``.
        """
        if last_healthy_at is None:
            return True
        iv = max(0, int(interval_seconds))
        if iv <= 0:
            # Still serialize same-timestamp dual hits.
            return last_healthy_at < now
        return last_healthy_at + timedelta(seconds=iv) <= now

    def _record_healthy_and_maybe_resolve(
        self,
        rule: AlertRule,
        *,
        value: float,
        now: datetime,
    ) -> list[AlertEvent]:
        """Atomically update healthy streak; auto-resolve when threshold met.

        Under a rule row lock. A healthy tick is counted only when
        ``healthy_interval_seconds`` has elapsed since ``last_healthy_at``,
        so parallel multi-coordinator hits in the same window do not advance
        the streak (and cannot auto-resolve instantly).
        """
        recovered = self._is_recovered(value, rule)
        with transaction(self._pool) as conn:
            locked = conn.execute(
                f"""
                SELECT {_ALERT_RULE_COLUMNS}
                FROM alert_rules
                WHERE id = %s
                FOR UPDATE
                """,
                (rule.id,),
            ).fetchone()
            if locked is None or not bool(locked["enabled"]):
                return []
            need = int(locked.get("recovery_evaluations") or 0)
            if need <= 0:
                return []
            locked_rule = self._row_to_alert_rule(locked)
            if not recovered or not self._is_recovered(value, locked_rule):
                # Gray zone / not healthy — reset progress.
                if (
                    int(locked.get("healthy_streak") or 0) != 0
                    or locked.get("last_healthy_at") is not None
                ):
                    conn.execute(
                        """
                        UPDATE alert_rules
                        SET healthy_streak = 0,
                            last_healthy_at = NULL,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (now, rule.id),
                    )
                return []

            last_h = locked.get("last_healthy_at")
            interval = int(
                locked.get("healthy_interval_seconds")
                if locked.get("healthy_interval_seconds") is not None
                else 1
            )
            if not self._healthy_tick_due(
                last_healthy_at=last_h,
                interval_seconds=interval,
                now=now,
            ):
                # Too soon since last counted tick (or concurrent same-window
                # hit) — do not advance streak.
                return []

            new_streak = int(locked.get("healthy_streak") or 0) + 1
            stored = min(new_streak, need)
            conn.execute(
                """
                UPDATE alert_rules
                SET healthy_streak = %s,
                    last_healthy_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (stored, now, now, rule.id),
            )
            if new_streak < need:
                return []
            rec_val = self._recovery_threshold_value(locked_rule)
            note = (
                f"auto-resolved after {new_streak} consecutive healthy "
                f"evaluation(s) spaced by >= {interval}s; value={value} "
                f"recovery_threshold={rec_val}"
            )
            rows = conn.execute(
                f"""
                UPDATE alert_events
                SET resolved_at = %s,
                    resolved_by = %s,
                    resolution_note = %s
                WHERE rule_id = %s
                  AND resolved_at IS NULL
                RETURNING {_ALERT_EVENT_COLUMNS}
                """,
                (now, AUTO_RESOLVE_BY, note, rule.id),
            ).fetchall()
            for r in rows:
                self._enqueue_alert_resolved_webhooks(
                    conn, event_row=r, now=now, recovery_value=value
                )
        return [self._row_to_alert_event(r) for r in rows]

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

    def _event_tenant_id(self, rule: AlertRule) -> str:
        """Tenant key stored on alert_events for isolation-scoped history reads.

        Tenant-scoped rules use their tenant. Overall (and worker without a
        tenant) firings are stored under :data:`ALERTS_OVERALL_TENANT`.
        """
        if rule.tenant_id and str(rule.tenant_id).strip():
            return str(rule.tenant_id).strip()
        return ALERTS_OVERALL_TENANT

    def _fire_alert(
        self,
        rule: AlertRule,
        *,
        value: float,
        detail: dict[str, Any],
        now: datetime,
    ) -> AlertEvent | None:
        """Atomically claim cooldown + append history + enqueue webhooks.

        Uses ``SELECT ... FOR UPDATE`` on the rule row so concurrent
        evaluators re-check cooldown under the lock; only the winner inserts
        an ``alert_events`` row. Returns ``None`` if the claim loses (still
        cooling down or rule disabled/deleted).

        Order for ``auto_pause_tenant``: enqueue ``alert.fired`` webhooks
        first, then pause the tenant so the paused-tenant gate cannot drop
        the notification that caused the pause. Claim path also allows
        ``alert.fired`` deliveries while paused.
        """
        event_id = uuid4()
        event_tenant = self._event_tenant_id(rule)
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
            locked = conn.execute(
                f"""
                SELECT {_ALERT_RULE_COLUMNS}
                FROM alert_rules
                WHERE id = %s
                FOR UPDATE
                """,
                (rule.id,),
            ).fetchone()
            if locked is None or not bool(locked["enabled"]):
                return None
            muted_until = locked.get("muted_until")
            if muted_until is not None and now < muted_until:
                return None
            last_fired = locked.get("last_fired_at")
            cool = int(locked["cooldown_seconds"])
            if last_fired is not None and cool > 0:
                cool_until = last_fired + timedelta(seconds=cool)
                if now < cool_until:
                    return None
            auto_pause = bool(locked.get("auto_pause_tenant"))
            # Atomic claim: only one concurrent txn wins for this (rule, now)
            # window. For cooldown=0, last_fired_at < now still serializes
            # same-timestamp dual-coordinator races; sequential calls with a
            # later ``now`` still fire.
            claimed = conn.execute(
                """
                UPDATE alert_rules
                SET last_fired_at = %s,
                    updated_at = %s,
                    healthy_streak = 0,
                    last_healthy_at = NULL
                WHERE id = %s
                  AND enabled = TRUE
                  AND (muted_until IS NULL OR muted_until <= %s)
                  AND (
                    last_fired_at IS NULL
                    OR (
                        cooldown_seconds > 0
                        AND last_fired_at
                            + (cooldown_seconds * INTERVAL '1 second') <= %s
                    )
                    OR (
                        cooldown_seconds <= 0
                        AND last_fired_at < %s
                    )
                  )
                RETURNING id
                """,
                (now, now, rule.id, now, now, now),
            ).fetchone()
            if claimed is None:
                return None
            # Annotate first; pause *after* webhook enqueue so alert.fired
            # deliveries are not skipped by the paused-tenant gate.
            pause_tid: str | None = None
            if (
                auto_pause
                and locked.get("scope") == "tenant"
                and locked.get("tenant_id")
            ):
                pause_tid = str(locked["tenant_id"])
                payload_detail["auto_paused_tenant"] = pause_tid
            row = conn.execute(
                f"""
                INSERT INTO alert_events (
                    id, rule_id, rule_name, metric, scope, tenant_id, worker_id,
                    value, threshold, comparator, window_seconds, detail, fired_at,
                    resolved_at, resolved_by, resolution_note
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    NULL, NULL, NULL
                )
                RETURNING {_ALERT_EVENT_COLUMNS}
                """,
                (
                    event_id,
                    rule.id,
                    rule.name,
                    rule.metric,
                    rule.scope,
                    event_tenant,
                    rule.worker_id,
                    value,
                    rule.threshold,
                    rule.comparator,
                    rule.window_seconds,
                    Jsonb(payload_detail),
                    now,
                ),
            ).fetchone()
            self._enqueue_alert_webhooks(
                conn,
                webhook_event=WebhookEvent.ALERT_FIRED.value,
                event_id=event_id,
                rule_id=rule.id,
                rule_name=rule.name,
                metric=rule.metric,
                scope=rule.scope,
                route_tenant_id=event_tenant,
                rule_tenant_id=rule.tenant_id,
                worker_id=rule.worker_id,
                value=value,
                threshold=rule.threshold,
                comparator=rule.comparator,
                window_seconds=rule.window_seconds,
                detail=payload_detail,
                now=now,
            )
            if pause_tid is not None:
                self._pause_tenant_conn(conn, pause_tid, now=now)
        assert row is not None
        return self._row_to_alert_event(row)

    @staticmethod
    def _pause_tenant_conn(conn: Any, tenant_id: str, *, now: datetime) -> None:
        """Pause tenant inside an existing transaction (alert auto-pause)."""
        conn.execute(
            """
            INSERT INTO tenant_settings (
                tenant_id, max_concurrent_leases, max_pending_jobs,
                paused, offboarding, updated_at
            )
            VALUES (%s, NULL, NULL, TRUE, FALSE, %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET paused = TRUE,
                updated_at = EXCLUDED.updated_at
            """,
            (tenant_id, now),
        )

    def _enqueue_alert_resolved_webhooks(
        self,
        conn: Any,
        *,
        event_row: dict[str, Any],
        now: datetime,
        recovery_value: float | None = None,
    ) -> None:
        """Enqueue alert.resolved for a just-closed history row."""
        detail = event_row.get("detail") or {}
        if isinstance(detail, str):
            import json

            detail = json.loads(detail)
        detail = dict(detail)
        if recovery_value is not None:
            detail["recovery_value"] = float(recovery_value)
        detail["resolved_by"] = event_row.get("resolved_by")
        detail["resolution_note"] = event_row.get("resolution_note")
        route_tid = event_row.get("tenant_id") or ALERTS_OVERALL_TENANT
        self._enqueue_alert_webhooks(
            conn,
            webhook_event=WebhookEvent.ALERT_RESOLVED.value,
            event_id=event_row["id"],
            rule_id=event_row["rule_id"],
            rule_name=event_row["rule_name"],
            metric=event_row["metric"],
            scope=event_row["scope"],
            route_tenant_id=str(route_tid),
            rule_tenant_id=event_row.get("tenant_id"),
            worker_id=event_row.get("worker_id"),
            value=float(event_row["value"]),
            threshold=float(event_row["threshold"]),
            comparator=event_row["comparator"],
            window_seconds=event_row.get("window_seconds"),
            detail=detail,
            now=now,
            resolved_at=event_row.get("resolved_at") or now,
            resolved_by=event_row.get("resolved_by"),
            resolution_note=event_row.get("resolution_note"),
        )

    def _enqueue_alert_webhooks(
        self,
        conn: Any,
        *,
        webhook_event: str,
        event_id: UUID,
        rule_id: UUID,
        rule_name: str,
        metric: str,
        scope: str,
        route_tenant_id: str,
        rule_tenant_id: str | None,
        worker_id: str | None,
        value: float,
        threshold: float,
        comparator: str,
        window_seconds: int | None,
        detail: dict[str, Any],
        now: datetime,
        resolved_at: datetime | None = None,
        resolved_by: str | None = None,
        resolution_note: str | None = None,
    ) -> None:
        """Enqueue alert.fired / alert.resolved deliveries to matching endpoints.

        Never skips paused tenants: operators must receive alert lifecycle
        notifications even when auto_pause (or manual pause) has stopped the
        tenant's job webhooks.

        Outbox ``next_attempt_at`` uses wall-clock time (not a synthetic
        evaluation ``now``) so deliveries are immediately claimable when
        evaluate/auto-resolve passes an advanced timestamp for streak math.
        """
        event = webhook_event
        tenants = [route_tenant_id or ALERTS_OVERALL_TENANT]
        # Schedule for immediate claim under real time.
        schedule_at = _utcnow()

        for tid in tenants:
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
                payload: dict[str, Any] = {
                    "event": event,
                    "delivery_id": str(delivery_id),
                    "alert_event_id": str(event_id),
                    "rule_id": str(rule_id),
                    "rule_name": rule_name,
                    "metric": metric,
                    "scope": scope,
                    "tenant_id": rule_tenant_id,
                    "worker_id": worker_id,
                    "value": value,
                    "threshold": threshold,
                    "comparator": comparator,
                    "window_seconds": window_seconds,
                    "detail": detail,
                    "timestamp": now.isoformat(),
                }
                if event == WebhookEvent.ALERT_RESOLVED.value:
                    payload["resolved_at"] = (
                        resolved_at.isoformat()
                        if resolved_at is not None
                        else now.isoformat()
                    )
                    payload["resolved_by"] = resolved_by
                    payload["resolution_note"] = resolution_note
                    payload["status"] = "resolved"
                from job_queue.crypto import delivery_aad

                sealed, payload_key_id = self._seal_dict(
                    conn,
                    tenant_id=str(tid),
                    plaintext=payload,
                    aad=delivery_aad(delivery_id),
                )
                conn.execute(
                    f"""
                    INSERT INTO webhook_deliveries (
                        id, endpoint_id, tenant_id, job_id, event, payload,
                        status, attempts, max_attempts, backoff_base_seconds,
                        backoff_max_seconds, next_attempt_at, signing_secret,
                        created_at, updated_at, payload_key_id
                    ) VALUES (
                        %s, %s, %s, NULL, %s, %s,
                        'pending', 0, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    (
                        delivery_id,
                        ep["id"],
                        tid,
                        event,
                        Jsonb(sealed),
                        ep["max_attempts"],
                        ep["backoff_base_seconds"],
                        ep["backoff_max_seconds"],
                        schedule_at,
                        ep.get("secret"),
                        schedule_at,
                        schedule_at,
                        payload_key_id,
                    ),
                )

    def _row_to_alert_rule(self, row: dict[str, Any]) -> AlertRule:
        rec_th = row.get("recovery_threshold")
        hiv = row.get("healthy_interval_seconds")
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
            muted_until=row.get("muted_until"),
            auto_pause_tenant=bool(row.get("auto_pause_tenant") or False),
            recovery_threshold=float(rec_th) if rec_th is not None else None,
            recovery_evaluations=int(row.get("recovery_evaluations") or 0),
            healthy_interval_seconds=int(hiv) if hiv is not None else 1,
            healthy_streak=int(row.get("healthy_streak") or 0),
            last_healthy_at=row.get("last_healthy_at"),
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
            resolved_at=row.get("resolved_at"),
            resolved_by=row.get("resolved_by"),
            resolution_note=row.get("resolution_note"),
        )
