"""JobQueue finalize_webhook_delivery."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.queue._common import (
    _utcnow,
    compute_retry_backoff_seconds,
)


class WebhooksFinalizeMixin:
    def finalize_webhook_delivery(
        self,
        delivery_id: UUID,
        *,
        status_code: int | None = None,
        error: str | None = None,
        endpoint_missing: bool = False,
    ) -> Any | None:
        """Finalize a claimed (delivering) webhook after the agent POSTed (or not).

        On success, persists ``last_status_code`` first so a crash before the
        status flip still prevents double-POST on reclaim.
        Returns ``None`` if the delivery is no longer ``delivering`` (lost race).

        While a tenant is offboarding, failed deliveries are forced terminal
        (``dead``) instead of requeued to ``pending``, then the tenant is
        auto-purged once no leased jobs / delivering webhooks remain.
        """
        from job_queue.webhooks import WebhookDeliveryResult

        now = _utcnow()

        if endpoint_missing:
            with transaction(self._pool) as conn:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_error = %s,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (
                        "endpoint missing or disabled",
                        now,
                        now,
                        delivery_id,
                    ),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(
                    conn, tenant_id=updated["tenant_id"], now=now
                )
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=False,
                    status_code=None,
                    error="endpoint missing or disabled",
                )

        success = status_code is not None and 200 <= int(status_code) < 300
        detail = (error or "")[:2000] if error else None

        # Load attempts / max under lock for retry vs dead decision.
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE id = %s
                FOR UPDATE
                """,
                (delivery_id,),
            ).fetchone()
            if row is None or row["status"] != "delivering":
                return None

            delivery = self._row_to_webhook_delivery(row)
            attempts = delivery.attempts
            tenant_id = delivery.tenant_id
            offboarding = self._is_tenant_offboarding(conn, tenant_id)

            if success:
                # Single UPDATE: persist 2xx evidence and terminal status together.
                # (Crash mid-statement still leaves status=delivering; reclaim
                # auto-succeeds when last_status_code is 2xx.)
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'succeeded',
                        delivering_until = NULL,
                        last_status_code = %s,
                        last_error = NULL,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (int(status_code), now, now, delivery_id),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=True,
                    status_code=int(status_code),
                    error=None,
                )

            # Offboarding: never requeue pending webhook work after a claim.
            force_dead = offboarding or attempts >= delivery.max_attempts
            if force_dead:
                err = detail or "delivery failed"
                if offboarding and attempts < delivery.max_attempts:
                    err = (detail or "delivery failed") + " (tenant offboard)"
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_status_code = %s,
                        last_error = %s,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (
                        status_code,
                        err,
                        now,
                        now,
                        delivery_id,
                    ),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=False,
                    status_code=status_code,
                    error=err,
                )

            delay = compute_retry_backoff_seconds(
                attempts,
                base_seconds=delivery.backoff_base_seconds,
                max_seconds=delivery.backoff_max_seconds,
            )
            next_at = now + timedelta(seconds=delay)
            updated = conn.execute(
                f"""
                UPDATE webhook_deliveries
                SET status = 'pending',
                    delivering_until = NULL,
                    last_status_code = %s,
                    last_error = %s,
                    next_attempt_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND status = 'delivering'
                RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                """,
                (
                    status_code,
                    detail or "delivery failed",
                    next_at,
                    now,
                    delivery_id,
                ),
            ).fetchone()
            if updated is None:
                return None
            return WebhookDeliveryResult(
                delivery=self._row_to_webhook_delivery(updated),
                success=False,
                status_code=status_code,
                error=detail or "delivery failed",
            )
