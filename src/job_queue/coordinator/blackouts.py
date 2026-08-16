"""Coordinator API handlers for maintenance blackout windows."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.codec import blackout_to_dict
from job_queue.coordinator.auth import _UUID_RE
from job_queue.queue import BlackoutError, ScheduleError


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


class BlackoutsApiMixin:
    def _create_tenant_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant blackout path"}
        return self._create_blackout(tenant_id=parts[2], schedule_id=None, body=body)

    def _create_schedule_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule blackout path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        return self._create_blackout(
            tenant_id=str(tenant_id), schedule_id=UUID(sid), body=body
        )

    def _create_blackout(
        self,
        *,
        tenant_id: str,
        schedule_id: UUID | None,
        body: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        try:
            win = self.queue.create_blackout(
                tenant_id=tenant_id,
                schedule_id=schedule_id,
                kind=body.get("kind"),
                starts_at=_parse_dt(body.get("starts_at")),
                ends_at=_parse_dt(body.get("ends_at")),
                daily_start=body.get("daily_start"),
                daily_end=body.get("daily_end"),
                timezone=str(body.get("timezone") or "UTC"),
                reason=body.get("reason"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except ScheduleError as exc:
            return 404, {"error": str(exc)}
        except BlackoutError as exc:
            return 409, {"error": str(exc)}
        return 201, {"blackout": blackout_to_dict(win)}

    def _list_tenant_blackouts(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant blackout path"}
        return self._list_blackouts(tenant_id=parts[2], schedule_id=None, query=query)

    def _list_schedule_blackouts(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid schedule blackout path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = (query.get("tenant_id") or [None])[0]
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        return self._list_blackouts(
            tenant_id=str(tenant_id), schedule_id=UUID(sid), query=query
        )

    def _list_blackouts(
        self,
        *,
        tenant_id: str,
        schedule_id: UUID | None,
        query: dict[str, list[str]],
    ) -> tuple[int, dict[str, Any]]:
        include = (query.get("include_tenant") or ["1"])[0] in ("1", "true", "yes")
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
            rows = self.queue.list_blackouts(
                tenant_id=tenant_id,
                schedule_id=schedule_id,
                include_tenant=include,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"blackouts": [blackout_to_dict(w) for w in rows]}

    def _update_tenant_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 5 or not _UUID_RE.match(parts[4]):
            return 400, {"error": "invalid tenant blackout path"}
        return self._update_blackout(
            blackout_id=UUID(parts[4]),
            tenant_id=parts[2],
            schedule_id=None,
            body=body,
        )

    def _update_schedule_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 5 or not _UUID_RE.match(parts[2]) or not _UUID_RE.match(parts[4]):
            return 400, {"error": "invalid schedule blackout path"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        return self._update_blackout(
            blackout_id=UUID(parts[4]),
            tenant_id=str(tenant_id),
            schedule_id=UUID(parts[2]),
            body=body,
        )

    def _update_blackout(
        self,
        *,
        blackout_id: UUID,
        tenant_id: str,
        schedule_id: UUID | None,
        body: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        try:
            win = self.queue.update_blackout(
                blackout_id,
                tenant_id=tenant_id,
                schedule_id=schedule_id,
                starts_at=_parse_dt(body.get("starts_at"))
                if "starts_at" in body
                else None,
                ends_at=_parse_dt(body.get("ends_at")) if "ends_at" in body else None,
                daily_start=body.get("daily_start") if "daily_start" in body else None,
                daily_end=body.get("daily_end") if "daily_end" in body else None,
                timezone=body.get("timezone"),
                reason=body.get("reason"),
                clear_reason=bool(body.get("clear_reason")),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except BlackoutError as exc:
            return 404, {"error": str(exc)}
        return 200, {"blackout": blackout_to_dict(win)}

    def _delete_tenant_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 6 or not _UUID_RE.match(parts[4]):
            return 400, {"error": "invalid tenant blackout path"}
        return self._delete_blackout(
            blackout_id=UUID(parts[4]),
            tenant_id=parts[2],
            schedule_id=None,
        )

    def _delete_schedule_blackout(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 6 or not _UUID_RE.match(parts[2]) or not _UUID_RE.match(parts[4]):
            return 400, {"error": "invalid schedule blackout path"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        return self._delete_blackout(
            blackout_id=UUID(parts[4]),
            tenant_id=str(tenant_id),
            schedule_id=UUID(parts[2]),
        )

    def _delete_blackout(
        self,
        *,
        blackout_id: UUID,
        tenant_id: str,
        schedule_id: UUID | None,
    ) -> tuple[int, dict[str, Any]]:
        deleted = self.queue.delete_blackout(
            blackout_id, tenant_id=tenant_id, schedule_id=schedule_id
        )
        if not deleted:
            return 404, {"error": f"blackout {blackout_id} not found"}
        return 200, {"deleted": True, "blackout_id": str(blackout_id)}
