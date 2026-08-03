"""Coordinator alert rules / history API."""

from __future__ import annotations

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
    }


class AlertsApiMixin:
    def _create_alert_rule(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
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
        parts = path.strip("/").split("/")
        if len(parts) < 4:
            return 400, {"error": "invalid rule path"}
        try:
            rid = UUID(parts[3])
        except ValueError:
            return 400, {"error": "invalid rule id"}
        ok = self.queue.delete_alert_rule(rid)
        if not ok:
            return 404, {"error": "not found"}
        return 200, {"deleted": True, "id": str(rid)}

    def _set_alert_rule_enabled(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) < 5:
            return 400, {"error": "invalid enable path"}
        try:
            rid = UUID(parts[3])
        except ValueError:
            return 400, {"error": "invalid rule id"}
        enabled = body.get("enabled")
        if enabled is None:
            return 400, {"error": "enabled required"}
        try:
            rule = self.queue.set_alert_rule_enabled(rid, enabled=bool(enabled))
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
            rule_id = UUID(rule_raw) if rule_raw else None
            events = self.queue.list_alert_events(
                tenant_id=str(tenant).strip(),
                rule_id=rule_id,
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

    def _evaluate_alerts(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            fired = self.queue.evaluate_alerts()
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "fired": [_event_to_dict(e) for e in fired],
            "count": len(fired),
        }
