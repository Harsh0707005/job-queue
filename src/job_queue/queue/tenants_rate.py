"""JobQueue tenant enqueue rate limits (token bucket)."""

from __future__ import annotations

import math

from datetime import datetime
from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import (
    RateLimitError,
    _as_utc,
    _utcnow,
)


class TenantsRateMixin:
    def set_enqueue_rate_limit(
        self,
        tenant_id: str,
        per_second: float | None,
        *,
        burst: int | None = None,
    ) -> None:
        """Cap how fast *tenant_id* may enqueue new jobs (token bucket).

        *per_second* is the steady refill rate. *burst* is the max bucket
        size (defaults to ``max(1, ceil(per_second))``). Pass ``None`` for
        *per_second* to clear (unlimited). Does not clear pause / other caps.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        rate, burst_n = self._normalize_rate_limit(per_second, burst)
        now = _utcnow()
        with transaction(self._pool) as conn:
            if rate is None:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, max_concurrent_leases, max_pending_jobs,
                        paused, offboarding, updated_at
                    )
                    VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET enqueue_rate_per_sec = NULL,
                        enqueue_burst = NULL,
                        enqueue_tokens = NULL,
                        enqueue_tokens_at = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return
            assert burst_n is not None
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    enqueue_rate_per_sec, enqueue_burst,
                    enqueue_tokens, enqueue_tokens_at,
                    updated_at
                )
                VALUES (
                    %s, NULL, NULL, FALSE, FALSE,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET enqueue_rate_per_sec = EXCLUDED.enqueue_rate_per_sec,
                    enqueue_burst = EXCLUDED.enqueue_burst,
                    enqueue_tokens = EXCLUDED.enqueue_tokens,
                    enqueue_tokens_at = EXCLUDED.enqueue_tokens_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, rate, burst_n, float(burst_n), now, now),
            )

    def get_enqueue_rate_limit(self, tenant_id: str) -> tuple[float | None, int | None]:
        """Return ``(per_second, burst)`` or ``(None, None)`` if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT enqueue_rate_per_sec, enqueue_burst
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None or row["enqueue_rate_per_sec"] is None:
                return None, None
            return float(row["enqueue_rate_per_sec"]), (
                int(row["enqueue_burst"]) if row["enqueue_burst"] is not None else None
            )

    @staticmethod
    def _normalize_rate_limit(
        per_second: float | None,
        burst: int | None,
    ) -> tuple[float | None, int | None]:
        if per_second is None:
            if burst is not None:
                raise ValueError("burst requires per_second to be set")
            return None, None
        rate = float(per_second)
        if rate <= 0:
            raise ValueError("per_second must be > 0 or None")
        if burst is None:
            burst_n = max(1, int(math.ceil(rate)))
        else:
            burst_n = int(burst)
            if burst_n < 1:
                raise ValueError("burst must be >= 1")
        return rate, burst_n

    def _consume_enqueue_rate(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> None:
        """Consume one enqueue token or raise :class:`RateLimitError`."""
        ok, retry_after = self._try_consume_rate_tokens(
            conn,
            tenant_id,
            kind="enqueue",
            now=now,
            cost=1.0,
        )
        if ok:
            return
        raise RateLimitError(
            f"tenant {tenant_id!r} enqueue rate limit exceeded"
            + (f"; retry after ~{retry_after:.3f}s" if retry_after is not None else ""),
            tenant_id=tenant_id,
            kind="enqueue",
            retry_after_seconds=retry_after,
        )

    def _try_consume_rate_tokens(
        self,
        conn: Any,
        tenant_id: str,
        *,
        kind: str,
        now: datetime,
        cost: float = 1.0,
    ) -> tuple[bool, float | None]:
        """Token-bucket consume under the tenant advisory lock.

        Returns ``(True, None)`` if allowed (or unlimited), else
        ``(False, retry_after_seconds)``.
        """
        if kind not in ("enqueue", "lease"):
            raise ValueError(f"unknown rate kind {kind!r}")
        rate_col = f"{kind}_rate_per_sec"
        burst_col = f"{kind}_burst"
        tokens_col = f"{kind}_tokens"
        at_col = f"{kind}_tokens_at"

        # Caller often already holds the tenant lock; take it again (re-entrant
        # xact lock is fine) so standalone paths are safe.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (tenant_id,),
        )
        row = conn.execute(
            f"""
            SELECT {rate_col} AS rate, {burst_col} AS burst,
                   {tokens_col} AS tokens, {at_col} AS tokens_at
            FROM tenant_settings
            WHERE tenant_id = %s
            FOR UPDATE
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["rate"] is None:
            return True, None

        rate = float(row["rate"])
        if rate <= 0:
            return True, None
        burst = int(row["burst"] or max(1, int(math.ceil(rate))))
        tokens = row["tokens"]
        tokens_at = row["tokens_at"]
        if tokens is None:
            tokens = float(burst)
        else:
            tokens = float(tokens)
        if tokens_at is None:
            tokens_at = now
        else:
            tokens_at = _as_utc(tokens_at)

        elapsed = max(0.0, (now - tokens_at).total_seconds())
        tokens = min(float(burst), tokens + elapsed * rate)
        if tokens + 1e-12 < cost:
            need = cost - tokens
            retry_after = need / rate if rate > 0 else None
            # Persist refilled (but not consumed) tokens so time advances.
            conn.execute(
                f"""
                UPDATE tenant_settings
                SET {tokens_col} = %s,
                    {at_col} = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                """,
                (tokens, now, now, tenant_id),
            )
            return False, retry_after

        tokens -= cost
        conn.execute(
            f"""
            UPDATE tenant_settings
            SET {tokens_col} = %s,
                {at_col} = %s,
                updated_at = %s
            WHERE tenant_id = %s
            """,
            (tokens, now, now, tenant_id),
        )
        return True, None
