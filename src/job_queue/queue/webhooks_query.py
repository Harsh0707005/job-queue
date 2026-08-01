"""JobQueue webhook delivery query: list/get deliveries."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.models import (
    WebhookDelivery,
)


class WebhooksQueryMixin:
    def list_webhook_deliveries(
        self,
        *,
        tenant_id: str,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[WebhookDelivery]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if status is not None:
            clauses.append("status = %s")
            params.append(status)
        params.extend([limit, offset])
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [self._row_to_webhook_delivery(r) for r in rows]

    def get_webhook_delivery(
        self, delivery_id: UUID, *, tenant_id: str
    ) -> WebhookDelivery | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE id = %s AND tenant_id = %s
                """,
                (delivery_id, tenant_id),
            ).fetchone()
            return self._row_to_webhook_delivery(row) if row else None
