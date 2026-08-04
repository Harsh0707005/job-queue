"""Coordinator alert rules / history API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.models import AlertEvent, AlertRule


def _rule_to_dict(r: AlertRule) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "name": r.name,
        "enabled": r.enabled,
        "scope": r.scope,
        "tenant_id": r.tenant_id,
        "worker_id": r.worker_id,
        "metric": r.metric,
        "comparator": r.comparator,
        "threshold": r.threshold,
        "window_seconds": r.window_seconds,
        "cooldown_seconds": r.cooldown_seconds,
        "last_fired_at": r.last_fired_at.isoformat() if r.last_fired_at else None,
        "muted_until": r.muted_until.isoformat() if r.muted_until else None,
        "auto_pause_tenant": r.auto_pause_tenant,
        "recovery_threshold": r.recovery_threshold,
        "recovery_evaluations": r.recovery_evaluations,
        "healthy_interval_seconds": r.healthy_interval_seconds,
        "healthy_streak": r.healthy_streak,
        "last_healthy_at": (
            r.last_healthy_at.isoformat() if r.last_healthy_at else None
        ),
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _event_to_dict(e: AlertEvent) -> dict[str, Any]:
    return {
        "id": str(e.id),
        "rule_id": str(e.rule_id),
        "rule_name": e.rule_name,
        "metric": e.metric,
        "scope": e.scope,
        "tenant_id": e.tenant_id,
        "worker_id": e.worker_id,
        "value": e.value,
        "threshold": e.threshold,
        "comparator": e.comparator,
        "window_seconds": e.window_seconds,
        "detail": e.detail,
        "fired_at": e.fired_at.isoformat() if e.fired_at else None,
        "status": e.status,
        "resolved_at": e.resolved_at.isoformat() if e.resolved_at else None,
        "resolved_by": e.resolved_by,
        "resolution_note": e.resolution_note,
    }


def _parse_rule_id(path: str, *, min_parts: int = 4) -> UUID | None:
    parts = path.strip("/").split("/")
    if len(parts) < min_parts:
        return None
    try:
        return UUID(parts[3])
    except ValueError:
        return None


class AlertsApiMixin:
    def _create_alert_rule(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            rec_raw = body.get("recovery_threshold", None)
            rec_threshold = float(rec_raw) if rec_raw is not None else None
            rule = self.queue.create_alert_rule(
                name=str(body.get("name") or ""),
                metric=str(body.get("metric") or ""),
                threshold=float(body["threshold"]),
                comparator=str(body.get("comparator") or "gt"),
                scope=str(body.get("scope") or "overall"),
                tenant_id=body.get("tenant_id"),
                worker_id=body.get("worker_id"),
                window_seconds=int(body.get("window_seconds") or 300),
                cooldown_seconds=int(body.get("cooldown_seconds") or 300),
                enabled=bool(body.get("enabled", True)),
                auto_pause_tenant=bool(body.get("auto_pause_tenant", False)),
                recovery_threshold=rec_threshold,
                recovery_evaluations=int(
                    body["recovery_evaluations"]
                    if body.get("recovery_evaluations") is not None
                    else 2
                ),
                healthy_interval_seconds=int(
                    body["healthy_interval_seconds"]
                    if body.get("healthy_interval_seconds") is not None
                    else 1
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"rule": _rule_to_dict(rule)}

    def _list_alert_rules(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        enabled_only = (query.get("enabled_only") or ["0"])[0] in (
            "1",
            "true",
            "yes",
        )
        rules = self.queue.list_alert_rules(enabled_only=enabled_only)
        return 200, {"rules": [_rule_to_dict(r) for r in rules], "count": len(rules)}

    def _delete_alert_rule(self, path: str) -> tuple[int, dict[str, Any]]:
        # /v1/alerts/rules/{id}
        rid = _parse_rule_id(path)
        if rid is None:
            return 400, {"error": "invalid rule id"}
        ok = self.queue.delete_alert_rule(rid)
        if not ok:
            return 404, {"error": "not found"}
        return 200, {"deleted": True, "id": str(rid)}

    def _set_alert_rule_enabled(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        rid = _parse_rule_id(path, min_parts=5)
        if rid is None:
            return 400, {"error": "invalid rule id"}
        enabled = body.get("enabled")
        if enabled is None:
            return 400, {"error": "enabled required"}
        try:
            rule = self.queue.set_alert_rule_enabled(rid, enabled=bool(enabled))
        except ValueError as exc:
            return 404, {"error": str(exc)}
        return 200, {"rule": _rule_to_dict(rule)}

    def _mute_alert_rule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        # POST /v1/alerts/rules/{id}/mute
        rid = _parse_rule_id(path, min_parts=5)
        if rid is None:
            return 400, {"error": "invalid rule id"}
        try:
            muted_until_raw = body.get("muted_until")
            muted_until = None
            if muted_until_raw is not None:
                muted_until = datetime.fromisoformat(
                    str(muted_until_raw).replace("Z", "+00:00")
                )
            duration = body.get("duration_seconds")
            rule = self.queue.mute_alert_rule(
                rid,
                duration_seconds=int(duration) if duration is not None else None,
                muted_until=muted_until,
            )
        except (TypeError, ValueError) as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        return 200, {"rule": _rule_to_dict(rule)}

    def _unmute_alert_rule(self, path: str) -> tuple[int, dict[str, Any]]:
        # POST /v1/alerts/rules/{id}/unmute
        rid = _parse_rule_id(path, min_parts=5)
        if rid is None:
            return 400, {"error": "invalid rule id"}
        try:
            rule = self.queue.unmute_alert_rule(rid)
        except ValueError as exc:
            return 404, {"error": str(exc)}
        return 200, {"rule": _rule_to_dict(rule)}

    def _list_alert_events(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant or not str(tenant).strip():
            return 400, {"error": "tenant_id is required"}
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
            rule_raw = (query.get("rule_id") or [None])[0]
            status = (query.get("status") or [None])[0]
            rule_id = UUID(rule_raw) if rule_raw else None
            events = self.queue.list_alert_events(
                tenant_id=str(tenant).strip(),
                rule_id=rule_id,
                status=status,
                limit=limit,
                offset=offset,
            )
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "events": [_event_to_dict(e) for e in events],
            "count": len(events),
            "tenant_id": str(tenant).strip(),
        }

    def _resolve_alert_event(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        # POST /v1/alerts/events/{id}/resolve
        parts = path.strip("/").split("/")
        if len(parts) < 5:
            return 400, {"error": "invalid event path"}
        try:
            eid = UUID(parts[3])
        except ValueError:
            return 400, {"error": "invalid event id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id or not str(tenant_id).strip():
            return 400, {"error": "tenant_id is required"}
        try:
            event = self.queue.resolve_alert_event(
                eid,
                tenant_id=str(tenant_id).strip(),
                resolved_by=body.get("resolved_by"),
                note=body.get("note") or body.get("resolution_note"),
            )
        except ValueError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        return 200, {"event": _event_to_dict(event)}

    def _evaluate_alerts(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            result = self.queue.evaluate_alerts()
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "fired": [_event_to_dict(e) for e in result.fired],
            "resolved": [_event_to_dict(e) for e in result.resolved],
            "count": len(result.fired),
            "resolved_count": len(result.resolved),
        }
