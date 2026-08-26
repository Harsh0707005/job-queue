"""Coordinator API for signed inbound HTTP triggers."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.codec import (
    dag_enqueue_result_to_dict,
    inbound_trigger_to_dict,
    job_to_dict,
)
from job_queue.coordinator.auth import _UUID_RE
from job_queue.coordinator.workflows_api import _run_to_dict, _signal_to_dict, _step_to_dict
from job_queue.queue import (
    DependencyError,
    QueueDrainError,
    RateLimitError,
)
from job_queue.webhooks import (
    NONCE_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_inbound_signature,
)


class InboundApiMixin:
    def _create_inbound_trigger(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            trig = self.queue.create_inbound_trigger(
                tenant_id=str(body["tenant_id"]),
                name=str(body["name"]),
                secret=body.get("secret"),
                kinds=body.get("kinds"),
                enabled=bool(body.get("enabled", True)),
            )
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"trigger": inbound_trigger_to_dict(trig, include_secret=True)}

    def _list_inbound_triggers(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        rows = self.queue.list_inbound_triggers(tenant_id=str(tenant))
        return 200, {
            "triggers": [inbound_trigger_to_dict(t) for t in rows],
            "count": len(rows),
        }

    def _get_inbound_trigger(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 3:
            return 400, {"error": "invalid trigger path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid trigger id"}
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        trig = self.queue.get_inbound_trigger(UUID(parts[2]), tenant_id=str(tenant))
        if trig is None:
            return 404, {"error": "not found"}
        return 200, {"trigger": inbound_trigger_to_dict(trig)}

    def _rotate_inbound_secret(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid trigger path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid trigger id"}
        tenant = body.get("tenant_id")
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            trig = self.queue.rotate_inbound_trigger_secret(
                UUID(parts[2]),
                tenant_id=str(tenant),
                secret=body.get("secret"),
            )
        except ValueError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        return 200, {"trigger": inbound_trigger_to_dict(trig, include_secret=True)}

    def _set_inbound_enabled(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid trigger path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid trigger id"}
        tenant = body.get("tenant_id")
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        if "enabled" not in body:
            return 400, {"error": "enabled is required"}
        try:
            trig = self.queue.set_inbound_trigger_enabled(
                UUID(parts[2]),
                tenant_id=str(tenant),
                enabled=bool(body.get("enabled")),
            )
        except ValueError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        return 200, {"trigger": inbound_trigger_to_dict(trig)}

    def _delete_inbound_trigger(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid trigger path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid trigger id"}
        tenant = body.get("tenant_id")
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        deleted = self.queue.delete_inbound_trigger(
            UUID(parts[2]), tenant_id=str(tenant)
        )
        if not deleted:
            return 404, {"error": "not found"}
        return 200, {"deleted": True}

    def _fire_inbound(
        self, path: str, raw: bytes, headers: Any
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 3 or not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid inbound path"}
        trigger_id = UUID(parts[2])
        trig = self.queue.get_inbound_trigger(trigger_id)
        if trig is None:
            return 404, {"error": "not found", "code": "trigger_not_found"}
        ts = headers.get(TIMESTAMP_HEADER) if headers is not None else None
        nonce = headers.get(NONCE_HEADER) if headers is not None else None
        sig = headers.get(SIGNATURE_HEADER) if headers is not None else None
        reason = verify_inbound_signature(
            trig.secret or "",
            raw,
            sig,
            timestamp=str(ts) if ts is not None else None,
            nonce=str(nonce) if nonce is not None else None,
        )
        if reason is not None:
            return 401, {"error": "invalid signature", "code": reason}
        try:
            import json

            parsed = json.loads(raw.decode("utf-8") or "{}") if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return 400, {"error": f"invalid json: {exc}"}
        if not isinstance(parsed, dict):
            return 400, {"error": "json body must be an object"}
        try:
            out = self.queue.fire_inbound_trigger(
                trigger_id, parsed, nonce=str(nonce).strip()
            )
        except FileExistsError:
            return 409, {"error": "replayed nonce", "code": "replay"}
        except LookupError:
            return 404, {"error": "not found", "code": "trigger_not_found"}
        except PermissionError as exc:
            return 403, {"error": str(exc), "code": "forbidden"}
        except RateLimitError as exc:
            from job_queue.coordinator.jobs import _limit_error_response

            return _limit_error_response(exc)
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except (KeyError, ValueError, TypeError, DependencyError) as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            if "offboarding" in msg:
                return 409, {"error": msg, "code": "offboarding"}
            return 400, {"error": msg}
        return self._inbound_result_payload(out)

    def _inbound_result_payload(
        self, out: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        action = out.get("action")
        if action == "job":
            return 201, {
                "action": "job",
                "job": job_to_dict(out["job"], include_payload=True),
            }
        if action == "dag":
            payload = dag_enqueue_result_to_dict(out["dag"])
            payload["action"] = "dag"
            return (201 if out["dag"].created else 200), payload
        if action == "workflow":
            return 201, {
                "action": "workflow",
                "run": _run_to_dict(out["run"]),
                "steps": [_step_to_dict(s) for s in (out.get("steps") or [])],
            }
        if action == "signal":
            code = 201 if out.get("created") else 200
            return code, {
                "action": "signal",
                "signal": _signal_to_dict(out["signal"]),
                "created": bool(out.get("created")),
                "unblocked": int(out.get("unblocked") or 0),
            }
        return 200, {"action": action}