"""Per-tenant failure circuit breaker.

When a tenant's jobs fail hard (worker nack, or max-runtime dead) enough
times inside a window, the breaker opens and new leases / schedule ticks
for that tenant stop until cooldown elapses or an operator resets it.
Enqueue still works so work waits; other tenants keep draining.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from job_queue.db import transaction
from job_queue.models import TenantBreaker
from job_queue.queue._common import _as_utc, _utcnow


_MIN_WINDOW_SECONDS = 1
_MAX_WINDOW_SECONDS = 31 * 24 * 3600
_MIN_COOLDOWN_SECONDS = 1
_MAX_COOLDOWN_SECONDS = 31 * 24 * 3600

# Used in fair-lease / schedule / webhook SQL (alias ``ts``). An open
# breaker whose cooldown has elapsed is treated as closed here; the next
# claim under the tenant lock actually clears the row.
_TENANT_CLAIMS_BLOCKED_SQL = """
    (
        ts.paused = TRUE
        OR (
            ts.breaker_opened_at IS NOT NULL
            AND (
                ts.breaker_cooldown_seconds IS NULL
                OR ts.breaker_opened_at
                   + make_interval(secs => ts.breaker_cooldown_seconds)
                   > CLOCK_TIMESTAMP()
            )
        )
    )
"""


def _empty_breaker(tenant_id: str) -> TenantBreaker:
    return TenantBreaker(
        tenant_id=tenant_id,
        failure_threshold=None,
        window_seconds=None,
        cooldown_seconds=None,
        failures=0,
        window_start=None,
        opened_at=None,
        open=False,
        reset_at=None,
    )


class TenantsBreakerMixin:
    def set_tenant_breaker(
        self,
        tenant_id: str,
        *,
        failure_threshold: int | None = None,
        window_seconds: int | None = None,
        cooldown_seconds: int | None = None,
        clear: bool = False,
    ) -> TenantBreaker:
        """Configure or disable this tenant's failure circuit breaker.

        Pass ``clear=True`` (or ``failure_threshold=None``) to disable.
        Changing the window while the breaker is closed resets the failure
        count. An already-open breaker stays open until reset / cooldown.
        ``cooldown_seconds=None`` means manual reset only.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if clear or failure_threshold is None:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, paused, offboarding, updated_at,
                        breaker_failure_threshold, breaker_window_seconds,
                        breaker_cooldown_seconds, breaker_failures,
                        breaker_window_start, breaker_opened_at
                    ) VALUES (
                        %s, FALSE, FALSE, %s,
                        NULL, NULL, NULL, 0, NULL, NULL
                    )
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET breaker_failure_threshold = NULL,
                        breaker_window_seconds = NULL,
                        breaker_cooldown_seconds = NULL,
                        breaker_failures = 0,
                        breaker_window_start = NULL,
                        breaker_opened_at = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return self._read_breaker_locked(conn, tenant_id, now=now)

            threshold = int(failure_threshold)
            if threshold < 1:
                raise ValueError("failure_threshold must be >= 1 or None")

            if window_seconds is None:
                current = conn.execute(
                    """
                    SELECT breaker_window_seconds
                    FROM tenant_settings
                    WHERE tenant_id = %s
                    """,
                    (tenant_id,),
                ).fetchone()
                if current and current["breaker_window_seconds"] is not None:
                    window_seconds = int(current["breaker_window_seconds"])
                else:
                    raise ValueError(
                        "window_seconds is required when setting a breaker"
                    )
            window_seconds = int(window_seconds)
            if window_seconds < _MIN_WINDOW_SECONDS:
                raise ValueError(
                    f"window_seconds must be >= {_MIN_WINDOW_SECONDS}"
                )
            if window_seconds > _MAX_WINDOW_SECONDS:
                raise ValueError(
                    f"window_seconds must be <= {_MAX_WINDOW_SECONDS}"
                )

            if cooldown_seconds is not None:
                cooldown_seconds = int(cooldown_seconds)
                if cooldown_seconds < _MIN_COOLDOWN_SECONDS:
                    raise ValueError(
                        f"cooldown_seconds must be >= {_MIN_COOLDOWN_SECONDS} "
                        "or None"
                    )
                if cooldown_seconds > _MAX_COOLDOWN_SECONDS:
                    raise ValueError(
                        f"cooldown_seconds must be <= {_MAX_COOLDOWN_SECONDS}"
                    )

            existing = conn.execute(
                """
                SELECT breaker_window_seconds, breaker_opened_at
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            window_changed = (
                existing is None
                or existing["breaker_window_seconds"] is None
                or int(existing["breaker_window_seconds"]) != window_seconds
            )
            already_open = bool(existing and existing["breaker_opened_at"])
            if window_changed and not already_open:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, paused, offboarding, updated_at,
                        breaker_failure_threshold, breaker_window_seconds,
                        breaker_cooldown_seconds, breaker_failures,
                        breaker_window_start, breaker_opened_at
                    ) VALUES (
                        %s, FALSE, FALSE, %s,
                        %s, %s, %s, 0, NULL, NULL
                    )
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET breaker_failure_threshold =
                            EXCLUDED.breaker_failure_threshold,
                        breaker_window_seconds =
                            EXCLUDED.breaker_window_seconds,
                        breaker_cooldown_seconds =
                            EXCLUDED.breaker_cooldown_seconds,
                        breaker_failures = 0,
                        breaker_window_start = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        tenant_id,
                        now,
                        threshold,
                        window_seconds,
                        cooldown_seconds,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, paused, offboarding, updated_at,
                        breaker_failure_threshold, breaker_window_seconds,
                        breaker_cooldown_seconds, breaker_failures,
                        breaker_window_start, breaker_opened_at
                    ) VALUES (
                        %s, FALSE, FALSE, %s,
                        %s, %s, %s, 0, NULL, NULL
                    )
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET breaker_failure_threshold =
                            EXCLUDED.breaker_failure_threshold,
                        breaker_window_seconds =
                            EXCLUDED.breaker_window_seconds,
                        breaker_cooldown_seconds =
                            EXCLUDED.breaker_cooldown_seconds,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        tenant_id,
                        now,
                        threshold,
                        window_seconds,
                        cooldown_seconds,
                    ),
                )
            return self._read_breaker_locked(conn, tenant_id, now=now)

    def get_tenant_breaker(self, tenant_id: str) -> TenantBreaker:
        """Current breaker state (auto-closes when cooldown has elapsed)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            self._refresh_and_is_breaker_open(conn, tenant_id, now=now)
            return self._read_breaker_locked(conn, tenant_id, now=now)

    def reset_tenant_breaker(self, tenant_id: str) -> TenantBreaker:
        """Manually close the breaker and zero the failure window.

        Overdue schedules are aligned so the next tick fires once (same
        pickup as ``resume_tenant``).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            self._close_breaker_locked(conn, tenant_id, now=now)
            return self._read_breaker_locked(conn, tenant_id, now=now)

    def is_tenant_breaker_open(self, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        return self.get_tenant_breaker(tenant_id).open

    def _tenant_claims_blocked(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime | None = None,
    ) -> bool:
        """True when this tenant must not receive new leases / reclaim."""
        if self._is_tenant_paused(conn, tenant_id):
            return True
        return self._refresh_and_is_breaker_open(
            conn, tenant_id, now=now or _utcnow()
        )

    def _refresh_and_is_breaker_open(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> bool:
        """Close after cooldown if due; return whether the breaker is open."""
        row = conn.execute(
            """
            SELECT breaker_opened_at, breaker_cooldown_seconds
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["breaker_opened_at"] is None:
            return False
        cooldown = row["breaker_cooldown_seconds"]
        if cooldown is None:
            return True
        opened = _as_utc(row["breaker_opened_at"])
        if opened + timedelta(seconds=int(cooldown)) <= now:
            self._close_breaker_locked(conn, tenant_id, now=now)
            return False
        return True

    def _close_breaker_locked(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> None:
        conn.execute(
            """
            UPDATE tenant_settings
            SET breaker_opened_at = NULL,
                breaker_failures = 0,
                breaker_window_start = NULL,
                updated_at = %s
            WHERE tenant_id = %s
            """,
            (now, tenant_id),
        )
        conn.execute(
            """
            UPDATE schedules
            SET next_run_at = %s,
                updated_at = %s
            WHERE tenant_id = %s
              AND enabled = TRUE
              AND next_run_at < %s
            """,
            (now, now, tenant_id, now),
        )

    def _note_tenant_failure(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> bool:
        """Count one worker-driven failure. Returns True if this call opened.

        No-op when the breaker is not configured. Already-open breakers
        keep their open timestamp (no double-trip). The UPDATE is atomic
        so concurrent nacks cannot both slip past the threshold.
        """
        if not tenant_id:
            return False
        updated = conn.execute(
            """
            UPDATE tenant_settings
            SET breaker_failures = CASE
                    WHEN breaker_opened_at IS NOT NULL THEN breaker_failures
                    WHEN breaker_window_start IS NULL
                      OR breaker_window_start
                         + make_interval(secs => breaker_window_seconds) <= %s
                    THEN 1
                    ELSE breaker_failures + 1
                END,
                breaker_window_start = CASE
                    WHEN breaker_opened_at IS NOT NULL THEN breaker_window_start
                    WHEN breaker_window_start IS NULL
                      OR breaker_window_start
                         + make_interval(secs => breaker_window_seconds) <= %s
                    THEN %s
                    ELSE breaker_window_start
                END,
                breaker_opened_at = CASE
                    WHEN breaker_opened_at IS NOT NULL THEN breaker_opened_at
                    WHEN (
                        CASE
                            WHEN breaker_window_start IS NULL
                              OR breaker_window_start
                                 + make_interval(
                                       secs => breaker_window_seconds
                                   ) <= %s
                            THEN 1
                            ELSE breaker_failures + 1
                        END
                    ) >= breaker_failure_threshold THEN %s
                    ELSE NULL
                END,
                updated_at = %s
            WHERE tenant_id = %s
              AND breaker_failure_threshold IS NOT NULL
              AND breaker_window_seconds IS NOT NULL
            RETURNING breaker_opened_at, breaker_failures,
                      (breaker_opened_at = %s) AS just_opened
            """,
            (now, now, now, now, now, now, tenant_id, now),
        ).fetchone()
        if updated is None:
            return False
        just_opened = bool(updated.get("just_opened"))
        if just_opened and hasattr(self, "_record_metric"):
            self._record_metric(
                conn,
                tenant_id=str(tenant_id),
                metric="breaker_open",
                count=1,
                now=now,
            )
        return just_opened

    def _read_breaker_locked(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> TenantBreaker:
        row = conn.execute(
            """
            SELECT breaker_failure_threshold, breaker_window_seconds,
                   breaker_cooldown_seconds, breaker_failures,
                   breaker_window_start, breaker_opened_at
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["breaker_failure_threshold"] is None:
            return _empty_breaker(tenant_id)
        opened = (
            _as_utc(row["breaker_opened_at"])
            if row["breaker_opened_at"] is not None
            else None
        )
        start = (
            _as_utc(row["breaker_window_start"])
            if row["breaker_window_start"] is not None
            else None
        )
        cooldown = (
            int(row["breaker_cooldown_seconds"])
            if row["breaker_cooldown_seconds"] is not None
            else None
        )
        is_open = opened is not None
        reset_at = None
        if is_open and cooldown is not None and opened is not None:
            reset_at = opened + timedelta(seconds=cooldown)
        return TenantBreaker(
            tenant_id=tenant_id,
            failure_threshold=int(row["breaker_failure_threshold"]),
            window_seconds=(
                int(row["breaker_window_seconds"])
                if row["breaker_window_seconds"] is not None
                else None
            ),
            cooldown_seconds=cooldown,
            failures=int(row["breaker_failures"] or 0),
            window_start=start,
            opened_at=opened,
            open=is_open,
            reset_at=reset_at,
        )
