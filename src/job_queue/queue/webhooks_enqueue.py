"""JobQueue webhook enqueue and offboarding reap."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from job_queue.models import (
    Job,
    WebhookEvent,
)


class WebhooksEnqueueMixin:
    def _enqueue_job_webhooks(
        self,
        conn: Any,
        *,
        job: Job,
        event: WebhookEvent,
        now: datetime,
    ) -> None:
        """Insert pending delivery rows for all matching enabled endpoints.

        No-op when the tenant is paused (no new outbox rows until resume).
        An open failure breaker still enqueues: it only stops new leases,
        not notifications for work that already ran (complete / fail / dead).
        """
        if self._is_tenant_paused(conn, job.tenant_id):
            return

        endpoints = conn.execute(
            f"""
            SELECT {self._WEBHOOK_ENDPOINT_COLUMNS}
            FROM webhook_endpoints
            WHERE tenant_id = %s
              AND enabled = TRUE
              AND %s = ANY(events)
            """,
            (job.tenant_id, event.value),
        ).fetchall()
        if not endpoints:
            return

        for ep in endpoints:
            delivery_id = uuid4()
            payload = {
                "event": event.value,
                "delivery_id": str(delivery_id),
                "job_id": str(job.id),
                "tenant_id": job.tenant_id,
                "queue_name": job.queue_name,
                "status": job.status.value,
                "payload": job.payload,
                "result": job.result,
                "attempts": job.attempts,
                "max_attempts": job.max_attempts,
                "last_error": job.last_error,
                "completed_at": (
                    job.completed_at.isoformat() if job.completed_at else None
                ),
                "timestamp": now.isoformat(),
            }
            from job_queue.crypto import delivery_aad

            sealed, payload_key_id = self._seal_dict(
                conn,
                tenant_id=job.tenant_id,
                plaintext=payload,
                aad=delivery_aad(delivery_id),
            )
            # Freeze signing secret at enqueue so rotation mid-retry is safe.
            signing_secret = ep.get("secret")
            conn.execute(
                f"""
                INSERT INTO webhook_deliveries (
                    id, endpoint_id, tenant_id, job_id, event, payload,
                    status, attempts, max_attempts, backoff_base_seconds,
                    backoff_max_seconds, next_attempt_at, signing_secret,
                    created_at, updated_at, payload_key_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    'pending', 0, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    delivery_id,
                    ep["id"],
                    job.tenant_id,
                    job.id,
                    event.value,
                    Jsonb(sealed),
                    ep["max_attempts"],
                    ep["backoff_base_seconds"],
                    ep["backoff_max_seconds"],
                    now,
                    signing_secret,
                    now,
                    now,
                    payload_key_id,
                ),
            )

    def _reap_offboarding_expired_webhooks(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
    ) -> None:
        """Dead-letter expired ``delivering`` webhooks for offboarding tenants.

        Offboarding tenants are paused, so stuck claims are never reclaimed for
        a re-POST. Without this, abandoned agents leave the tenant non-empty.
        """
        if tenant_id is not None:
            if not self._is_tenant_offboarding(conn, tenant_id):
                return
            expired = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE tenant_id = %s
                  AND status = 'delivering'
                  AND delivering_until IS NOT NULL
                  AND delivering_until < %s
                FOR UPDATE
                """,
                (tenant_id, now),
            ).fetchall()
        else:
            expired = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries d
                WHERE d.status = 'delivering'
                  AND d.delivering_until IS NOT NULL
                  AND d.delivering_until < %s
                  AND EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = d.tenant_id
                          AND ts.offboarding = TRUE
                  )
                FOR UPDATE OF d
                """,
                (now,),
            ).fetchall()

        tenants: set[str] = set()
        for row in expired:
            tid = row["tenant_id"]
            # Proven 2xx already posted → succeed; else dead (no re-POST).
            code = row.get("last_status_code")
            if code is not None and 200 <= int(code) < 300:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'succeeded',
                        delivering_until = NULL,
                        last_error = NULL,
                        updated_at = %s,
                        completed_at = COALESCE(completed_at, %s)
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING id
                    """,
                    (now, now, row["id"]),
                ).fetchone()
            else:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_error = COALESCE(
                            last_error,
                            'claim expired during tenant offboard'
                        ),
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING id
                    """,
                    (now, now, row["id"]),
                ).fetchone()
            if updated is not None:
                tenants.add(tid)

        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)
