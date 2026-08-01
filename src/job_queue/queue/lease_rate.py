"""JobQueue lease rate-limit domain: set/get/consume claim rate limits."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import (
    _utcnow,
)


class LeaseRateMixin:
    def set_lease_rate_limit(
        self,
        tenant_id: str,
        per_second: float | None,
        *,
        burst: int | None = None,
    ) -> None:
        """Cap how fast *tenant_id* jobs may be claimed (token bucket).

        Applies to new pending claims (scoped and fair multi-tenant). Expired
        lease reclaim is not rate-limited. Fair lease skips a rate-limited
        tenant and rotates LRS so others keep getting work. Pass ``None`` for
        *per_second* to clear.
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
                    SET lease_rate_per_sec = NULL,
                        lease_burst = NULL,
                        lease_tokens = NULL,
                        lease_tokens_at = NULL,
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
                    lease_rate_per_sec, lease_burst,
                    lease_tokens, lease_tokens_at,
                    updated_at
                )
                VALUES (
                    %s, NULL, NULL, FALSE, FALSE,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET lease_rate_per_sec = EXCLUDED.lease_rate_per_sec,
                    lease_burst = EXCLUDED.lease_burst,
                    lease_tokens = EXCLUDED.lease_tokens,
                    lease_tokens_at = EXCLUDED.lease_tokens_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, rate, burst_n, float(burst_n), now, now),
            )

    def get_lease_rate_limit(self, tenant_id: str) -> tuple[float | None, int | None]:
        """Return ``(per_second, burst)`` or ``(None, None)`` if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT lease_rate_per_sec, lease_burst
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None or row["lease_rate_per_sec"] is None:
                return None, None
            return float(row["lease_rate_per_sec"]), (
                int(row["lease_burst"]) if row["lease_burst"] is not None else None
            )

    def _try_consume_lease_rate(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> bool:
        """Consume one lease token; False if rate-limited (no exception)."""
        ok, _ = self._try_consume_rate_tokens(
            conn,
            tenant_id,
            kind="lease",
            now=now,
            cost=1.0,
        )
        return ok
