"""JobQueue webhook claim and process delivery batch."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.queue._common import (
    _utcnow,
)


class WebhooksClaimBatchMixin:
    def claim_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        claim_seconds: float | None = None,
        http_timeout_seconds: float = 10.0,
        leader_token: UUID | None = None,
        leader_epoch: int | None = None,
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

        lease_s = (
            claim_seconds
            if claim_seconds is not None
            else max(30.0, float(http_timeout_seconds) * 3.0)
        )
        now = _utcnow()
        claim_until = now + timedelta(seconds=lease_s)

        claimed: list[ClaimedWebhook] = []
        auto_finalized: list[Any] = []

        with transaction(self._pool) as conn:
            if hasattr(self, "_fence_control_leader") and not self._fence_control_leader(
                conn, lease_token=leader_token, epoch=leader_epoch
            ):
                return [], []
            # Cheap gate: only scan offboarding reaper when needed.
            has_offboard = conn.execute(
                """
                SELECT 1 FROM tenant_settings
                WHERE offboarding = TRUE
                LIMIT 1
                """
            ).fetchone()
            if has_offboard is not None:
                self._reap_offboarding_expired_webhooks(conn, now=now, tenant_id=None)

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
                      AND (
                            d2.event IN ('alert.fired', 'alert.resolved')
                            -- Inbound callbacks stay claimable while paused so
                            -- offboard (which pauses the tenant) can finish.
                            OR d2.callback_url IS NOT NULL
                            OR NOT EXISTS (
                                SELECT 1 FROM tenant_settings ts
                                WHERE ts.tenant_id = d2.tenant_id
                                  AND ts.paused = TRUE
                                  AND ts.offboarding = FALSE
                            )
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
                # Crash-recovery auto-success still counts for metrics integrity.
                self._record_metric(
                    conn,
                    tenant_id=str(u["tenant_id"]),
                    metric="webhook_success",
                    count=1,
                    now=now,
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
                    AND (
                        d.event IN ('alert.fired', 'alert.resolved')
                        -- Offboard marks paused+offboarding and waits for
                        -- inbound callbacks; they must still be claimable.
                        OR d.callback_url IS NOT NULL
                        OR NOT EXISTS (
                            SELECT 1
                            FROM tenant_settings ts
                            WHERE ts.tenant_id = d.tenant_id
                              AND ts.paused = TRUE
                        )
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
                        d.completed_at, d.signing_secret, d.callback_url
                )
                SELECT
                    u.*,
                    COALESCE(e.url, u.callback_url) AS _ep_url,
                    -- Prefer frozen delivery secret; fall back for legacy rows.
                    COALESCE(u.signing_secret, e.secret) AS _sign_secret,
                    CASE
                        WHEN e.id IS NULL AND u.callback_url IS NOT NULL
                        THEN TRUE
                        ELSE e.enabled
                    END AS _ep_enabled
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
                    self._record_metric(
                        conn,
                        tenant_id=str(row["tenant_id"]),
                        metric="webhook_fail",
                        count=1,
                        now=now,
                    )
                    continue
                try:
                    if hasattr(self, "_fail_closed_unreadable_delivery"):
                        self._fail_closed_unreadable_delivery(
                            conn, row, now=now
                        )
                    delivery = self._row_to_webhook_delivery(row, conn=conn)
                except Exception as exc:
                    from job_queue.blob_store import BlobStoreError
                    from job_queue.queue._common import UnreadablePayloadError

                    if not isinstance(exc, (BlobStoreError, UnreadablePayloadError)):
                        raise
                    continue
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

    def process_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
        on_result: Any = None,
        on_before_post: Any = None,
        leader_token: UUID | None = None,
        leader_epoch: int | None = None,
    ) -> list[Any]:
        """Local orchestrator: claim → POST → finalize (tests / in-process).

        Distributed webhook *agents* should call :meth:`claim_webhook_deliveries`
        and :meth:`finalize_webhook_delivery` themselves and never rely on the
        coordinator to POST.

        When *leader_token* / *leader_epoch* are set, each POST is fenced.
        Losing the lease mid-batch stops further POSTs and releases the
        remaining claims so a new leader delivers each leftover once.
        A 2xx is written onto the row before finalize so a crash between
        POST and succeed cannot be retried as a second POST.
        """
        from job_queue.webhooks import post_webhook

        claimed, auto = self.claim_webhook_deliveries(
            limit=limit,
            claim_seconds=claim_seconds,
            http_timeout_seconds=http_timeout_seconds,
            leader_token=leader_token,
            leader_epoch=leader_epoch,
        )
        results: list[Any] = list(auto)
        if on_result is not None:
            for rec in auto:
                on_result(rec)

        def _still_leader() -> bool:
            if leader_token is None:
                return True
            if not hasattr(self, "still_control_leader"):
                return True
            return bool(
                self.still_control_leader(
                    lease_token=leader_token, epoch=leader_epoch
                )
            )

        def _drop_rest(from_index: int) -> None:
            rest = [claimed[j].delivery.id for j in range(from_index, len(claimed))]
            if rest and hasattr(self, "release_webhook_claims"):
                self.release_webhook_claims(rest)

        for i, item in enumerate(claimed):
            if not _still_leader():
                _drop_rest(i)
                break
            if on_before_post is not None:
                on_before_post(item)
                if not _still_leader():
                    _drop_rest(i)
                    break
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
            # Persist 2xx (or the failure code) before the terminal flip so
            # a reclaim after handoff/timeout will not POST again.
            if hasattr(self, "note_webhook_last_status"):
                self.note_webhook_last_status(delivery.id, int(status_code))
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

    @staticmethod
    def _count_webhook_delivering(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM webhook_deliveries
            WHERE tenant_id = %s
              AND (
                    status = 'delivering'
                 OR (
                        callback_url IS NOT NULL
                    AND status IN ('pending', 'delivering')
                    )
              )
            """,
            (tenant_id,),
        ).fetchone()
        return int(row["n"]) if row else 0
