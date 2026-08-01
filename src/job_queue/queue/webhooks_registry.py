"""JobQueue webhook registry domain: register/list/delete/enable/rotate endpoints."""

from __future__ import annotations

import json

from collections.abc import Sequence
from typing import Any
from uuid import UUID, uuid4

from job_queue.db import transaction
from job_queue.models import (
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)
from job_queue.queue._common import (
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
    _utcnow,
)


class WebhooksRegistryMixin:
    _WEBHOOK_ENDPOINT_COLUMNS = _WEBHOOK_ENDPOINT_COLUMNS
    _WEBHOOK_DELIVERY_COLUMNS = _WEBHOOK_DELIVERY_COLUMNS

    def register_webhook(
        self,
        *,
        tenant_id: str,
        url: str,
        events: Sequence[str] | None = None,
        secret: str | None = None,
        max_attempts: int = 5,
        backoff_base_seconds: int = 1,
        backoff_max_seconds: int = 300,
        enabled: bool = True,
        endpoint_id: UUID | None = None,
    ) -> WebhookEndpoint:
        """Register an HTTP webhook endpoint for terminal job events."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        # SSRF harden: scheme + block private/link-local/metadata destinations.
        from job_queue.webhooks import validate_webhook_url

        url = validate_webhook_url(url, resolve_dns=True)
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_base_seconds < 0 or backoff_max_seconds < 0:
            raise ValueError("backoff seconds must be >= 0")

        event_list = (
            list(events)
            if events is not None
            else [
                WebhookEvent.JOB_COMPLETED.value,
                WebhookEvent.JOB_FAILED.value,
                WebhookEvent.JOB_DEAD.value,
            ]
        )
        allowed = {e.value for e in WebhookEvent}
        for ev in event_list:
            if ev not in allowed:
                raise ValueError(f"unsupported webhook event {ev!r}")
        if not event_list:
            raise ValueError("events must be non-empty")

        now = _utcnow()
        eid = endpoint_id or uuid4()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; "
                    "webhook registration is rejected"
                )
            row = conn.execute(
                f"""
                INSERT INTO webhook_endpoints (
                    id, tenant_id, url, secret, events, enabled, max_attempts,
                    backoff_base_seconds, backoff_max_seconds, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                RETURNING {self._WEBHOOK_ENDPOINT_COLUMNS}
                """,
                (
                    eid,
                    tenant_id,
                    url,
                    secret,
                    event_list,
                    enabled,
                    max_attempts,
                    backoff_base_seconds,
                    backoff_max_seconds,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return self._row_to_webhook_endpoint(row)

    def list_webhooks(self, *, tenant_id: str) -> list[WebhookEndpoint]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {self._WEBHOOK_ENDPOINT_COLUMNS}
                FROM webhook_endpoints
                WHERE tenant_id = %s
                ORDER BY created_at ASC
                """,
                (tenant_id,),
            ).fetchall()
            return [self._row_to_webhook_endpoint(r) for r in rows]

    def delete_webhook(self, endpoint_id: UUID, *, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM webhook_endpoints
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (endpoint_id, tenant_id),
            ).fetchone()
            return row is not None

    def set_webhook_enabled(
        self, endpoint_id: UUID, *, tenant_id: str, enabled: bool
    ) -> WebhookEndpoint:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE webhook_endpoints
                SET enabled = %s, updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {self._WEBHOOK_ENDPOINT_COLUMNS}
                """,
                (enabled, now, endpoint_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ValueError(
                    f"webhook {endpoint_id} not found for tenant {tenant_id!r}"
                )
            return self._row_to_webhook_endpoint(row)

    def rotate_webhook_secret(
        self,
        endpoint_id: UUID,
        *,
        tenant_id: str,
        secret: str | None,
    ) -> WebhookEndpoint:
        """Update the endpoint signing secret without touching in-flight deliveries.

        New deliveries snapshot the new secret. Pending/retrying deliveries keep
        their original ``signing_secret`` so receivers can still verify them.
        Pass ``secret=None`` to disable signing for future deliveries only.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if secret is not None:
            secret = str(secret)
            if secret == "":
                raise ValueError("secret must be non-empty when provided")
            if len(secret) > 512:
                raise ValueError("secret must be at most 512 characters")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE webhook_endpoints
                SET secret = %s, updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {self._WEBHOOK_ENDPOINT_COLUMNS}
                """,
                (secret, now, endpoint_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ValueError(
                    f"webhook {endpoint_id} not found for tenant {tenant_id!r}"
                )
            return self._row_to_webhook_endpoint(row)

    @staticmethod
    def _row_to_webhook_endpoint(row: dict[str, Any]) -> WebhookEndpoint:
        events = row["events"] or []
        return WebhookEndpoint(
            id=row["id"],
            tenant_id=row["tenant_id"],
            url=row["url"],
            secret=row["secret"],
            events=tuple(events),
            enabled=bool(row["enabled"]),
            max_attempts=int(row["max_attempts"]),
            backoff_base_seconds=int(row["backoff_base_seconds"]),
            backoff_max_seconds=int(row["backoff_max_seconds"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_webhook_delivery(row: dict[str, Any]) -> WebhookDelivery:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return WebhookDelivery(
            id=row["id"],
            endpoint_id=row["endpoint_id"],
            tenant_id=row["tenant_id"],
            job_id=row["job_id"],
            event=row["event"],
            payload=payload,
            status=row["status"],
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            backoff_base_seconds=int(row["backoff_base_seconds"]),
            backoff_max_seconds=int(row["backoff_max_seconds"]),
            next_attempt_at=row["next_attempt_at"],
            delivering_until=row.get("delivering_until"),
            last_error=row["last_error"],
            last_status_code=row["last_status_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            signing_secret=row.get("signing_secret"),
        )
