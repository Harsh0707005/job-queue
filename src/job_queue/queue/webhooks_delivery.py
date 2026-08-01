"""JobQueue webhook delivery domain: claim, finalize, process, enqueue, offboard reap."""
from __future__ import annotations

import json

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import init_schema, transaction
from job_queue.models import (
    AuditConsistencyReport,
    BulkCancelResult,
    Job,
    JobAuditEvent,
    JobProgressEvent,
    JobRetentionSettings,
    JobSideEffect,
    JobStatus,
    PreemptResult,
    RetentionPurgeResult,
    Schedule,
    ScheduleFire,
    SpawnChildrenResult,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
    _ACTIVE_JOB_STATUSES,
    _CANCELABLE_STATUSES,
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _DEDUPE_LOCK_CLASS,
    _DEFAULT_ESTIMATE_SECONDS,
    _DEPS_SATISFIED_SQL,
    _EWMA_ALPHA,
    _JOB_COLUMNS,
    _MAX_LABELS,
    _MAX_LABEL_LEN,
    _MAX_PREEMPTS_PER_JOB,
    _NOTIFY_MAX_BYTES,
    _SCHEDULE_COLUMNS,
    _SIDE_EFFECT_SELECT,
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
    ApprovalError,
    ApprovalRequested,
    CancelError,
    ChildrenAwaited,
    DependencyError,
    LeaseError,
    MergeError,
    RateLimitError,
    RebuildError,
    ReplayError,
    ScheduleError,
    SpawnError,
    _SkipLeaseCandidate,
    _as_utc,
    _normalize_labels,
    _normalize_status_filter,
    _row_to_job,
    _row_to_schedule,
    _utcnow,
    compute_retry_backoff_seconds,
)


class WebhooksDeliveryMixin:
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

    def claim_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        claim_seconds: float | None = None,
        http_timeout_seconds: float = 10.0,
    ) -> tuple[list[Any], list[Any]]:
        """Claim due webhook deliveries without performing HTTP POSTs.

        Returns ``(claimed, auto_finalized)`` where *claimed* is a list of
        :class:`~job_queue.models.ClaimedWebhook` ready for the agent to POST,
        and *auto_finalized* are :class:`~job_queue.webhooks.WebhookDeliveryResult`
        for cases that need no HTTP (e.g. reclaim with proven 2xx last_status_code).

        Claim commits ``status=delivering`` before return so a crash mid-POST
        cannot leave the row as pending.

        Hot path uses a **set-based** ``UPDATE ... FROM (SELECT ... FOR UPDATE
        SKIP LOCKED LIMIT n)`` plus a single join to endpoints so large pending
        backlogs do not N-round-trip per row.
        """
        from job_queue.models import ClaimedWebhook
        from job_queue.webhooks import WebhookDeliveryResult

        if limit < 1:
            raise ValueError("limit must be >= 1")

        lease_s = claim_seconds if claim_seconds is not None else max(
            30.0, float(http_timeout_seconds) * 3.0
        )
        now = _utcnow()
        claim_until = now + timedelta(seconds=lease_s)

        claimed: list[ClaimedWebhook] = []
        auto_finalized: list[Any] = []

        with transaction(self._pool) as conn:
            # Cheap gate: only scan offboarding reaper when needed.
            has_offboard = conn.execute(
                """
                SELECT 1 FROM tenant_settings
                WHERE offboarding = TRUE
                LIMIT 1
                """
            ).fetchone()
            if has_offboard is not None:
                self._reap_offboarding_expired_webhooks(
                    conn, now=now, tenant_id=None
                )

            # 1) Auto-succeed expired delivering claims that already have 2xx
            #    evidence (crash after POST, before status flip).
            auto_rows = conn.execute(
                f"""
                UPDATE webhook_deliveries d
                SET status = 'succeeded',
                    delivering_until = NULL,
                    last_error = NULL,
                    updated_at = %s,
                    completed_at = COALESCE(d.completed_at, %s)
                WHERE d.id IN (
                    SELECT d2.id
                    FROM webhook_deliveries d2
                    WHERE d2.status = 'delivering'
                      AND d2.delivering_until IS NOT NULL
                      AND d2.delivering_until < %s
                      AND d2.last_status_code IS NOT NULL
                      AND d2.last_status_code >= 200
                      AND d2.last_status_code < 300
                      AND NOT EXISTS (
                            SELECT 1 FROM tenant_settings ts
                            WHERE ts.tenant_id = d2.tenant_id
                              AND ts.paused = TRUE
                              AND ts.offboarding = FALSE
                      )
                    FOR UPDATE OF d2 SKIP LOCKED
                    LIMIT %s
                )
                RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                """,
                (now, now, now, limit),
            ).fetchall()
            for u in auto_rows:
                auto_finalized.append(
                    WebhookDeliveryResult(
                        delivery=self._row_to_webhook_delivery(u),
                        success=True,
                        status_code=int(u["last_status_code"]),
                        error=None,
                    )
                )

            remaining = max(0, limit - len(auto_rows))
            if remaining < 1:
                return claimed, auto_finalized

            # 2) Set-based claim of pending + stuck delivering (no 2xx evidence).
            #    Also dead-letter pending rows that already exhausted attempts.
            claimed_rows = conn.execute(
                f"""
                WITH due AS (
                    SELECT d.id, d.status, d.attempts, d.max_attempts
                    FROM webhook_deliveries d
                    WHERE (
                            (
                                d.status = 'pending'
                            AND d.next_attempt_at <= %s
                            )
                         OR (
                                d.status = 'delivering'
                            AND d.delivering_until IS NOT NULL
                            AND d.delivering_until < %s
                            AND (
                                d.last_status_code IS NULL
                             OR d.last_status_code < 200
                             OR d.last_status_code >= 300
                            )
                         )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = d.tenant_id
                          AND ts.paused = TRUE
                    )
                    ORDER BY d.next_attempt_at ASC
                    FOR UPDATE OF d SKIP LOCKED
                    LIMIT %s
                ),
                updated AS (
                    UPDATE webhook_deliveries d
                    SET
                        status = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN 'dead'
                            ELSE 'delivering'
                        END,
                        attempts = CASE
                            WHEN d.status = 'pending'
                            THEN d.attempts + 1
                            ELSE d.attempts
                        END,
                        delivering_until = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN NULL
                            ELSE %s
                        END,
                        last_error = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN COALESCE(d.last_error, 'max attempts exceeded')
                            WHEN d.status = 'delivering'
                            THEN COALESCE(
                                d.last_error,
                                'reclaimed stuck delivering claim'
                            )
                            ELSE d.last_error
                        END,
                        completed_at = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN %s
                            ELSE d.completed_at
                        END,
                        updated_at = %s
                    FROM due
                    WHERE d.id = due.id
                    RETURNING
                        d.id, d.endpoint_id, d.tenant_id, d.job_id, d.event,
                        d.payload, d.status, d.attempts, d.max_attempts,
                        d.backoff_base_seconds, d.backoff_max_seconds,
                        d.next_attempt_at, d.delivering_until, d.last_error,
                        d.last_status_code, d.created_at, d.updated_at,
                        d.completed_at, d.signing_secret
                )
                SELECT
                    u.*,
                    e.url AS _ep_url,
                    -- Prefer frozen delivery secret; fall back for legacy rows.
                    COALESCE(u.signing_secret, e.secret) AS _sign_secret,
                    e.enabled AS _ep_enabled
                FROM updated u
                LEFT JOIN webhook_endpoints e ON e.id = u.endpoint_id
                """,
                (now, now, remaining, claim_until, now, now),
            ).fetchall()

            for row in claimed_rows:
                if row["status"] == "dead":
                    auto_finalized.append(
                        WebhookDeliveryResult(
                            delivery=self._row_to_webhook_delivery(row),
                            success=False,
                            status_code=None,
                            error="max attempts exceeded",
                        )
                    )
                    continue
                delivery = self._row_to_webhook_delivery(row)
                url = row.get("_ep_url")
                secret = row.get("_sign_secret")
                enabled = row.get("_ep_enabled")
                if url is None:
                    claimed.append(
                        ClaimedWebhook(
                            delivery=delivery,
                            url=None,
                            secret=None,
                            endpoint_enabled=False,
                        )
                    )
                else:
                    claimed.append(
                        ClaimedWebhook(
                            delivery=delivery,
                            url=url,
                            secret=secret,
                            endpoint_enabled=bool(enabled),
                        )
                    )

        return claimed, auto_finalized

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

    def process_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
        on_result: Any = None,
    ) -> list[Any]:
        """Local orchestrator: claim → POST → finalize (tests / in-process).

        Distributed webhook *agents* should call :meth:`claim_webhook_deliveries`
        and :meth:`finalize_webhook_delivery` themselves and never rely on the
        coordinator to POST.
        """
        from job_queue.webhooks import post_webhook

        claimed, auto = self.claim_webhook_deliveries(
            limit=limit,
            claim_seconds=claim_seconds,
            http_timeout_seconds=http_timeout_seconds,
        )
        results: list[Any] = list(auto)
        if on_result is not None:
            for rec in auto:
                on_result(rec)

        for item in claimed:
            delivery = item.delivery
            if not item.endpoint_enabled or not item.url:
                result = self.finalize_webhook_delivery(
                    delivery.id, endpoint_missing=True
                )
                if result is None:
                    continue
                results.append(result)
                if on_result is not None:
                    on_result(result)
                continue

            status_code, detail = post_webhook(
                item.url,
                delivery.payload,
                secret=item.secret,
                timeout_seconds=http_timeout_seconds,
                idempotency_key=str(delivery.id),
            )
            result = self.finalize_webhook_delivery(
                delivery.id,
                status_code=status_code,
                error=None if 200 <= status_code < 300 else detail,
            )
            if result is None:
                continue
            results.append(result)
            if on_result is not None:
                on_result(result)

        return results

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
            # Freeze signing secret at enqueue so rotation mid-retry is safe.
            signing_secret = ep.get("secret")
            conn.execute(
                f"""
                INSERT INTO webhook_deliveries (
                    id, endpoint_id, tenant_id, job_id, event, payload,
                    status, attempts, max_attempts, backoff_base_seconds,
                    backoff_max_seconds, next_attempt_at, signing_secret,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    'pending', 0, %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                """,
                (
                    delivery_id,
                    ep["id"],
                    job.tenant_id,
                    job.id,
                    event.value,
                    Jsonb(payload),
                    ep["max_attempts"],
                    ep["backoff_base_seconds"],
                    ep["backoff_max_seconds"],
                    now,
                    signing_secret,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _count_webhook_delivering(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM webhook_deliveries
            WHERE tenant_id = %s
              AND status = 'delivering'
            """,
            (tenant_id,),
        ).fetchone()
        return int(row["n"]) if row else 0

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
