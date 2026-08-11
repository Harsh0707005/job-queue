"""Per-tenant spend caps: max enqueues / runtime in a tumbling window."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from job_queue.db import transaction
from job_queue.models import TenantSpendCap
from job_queue.queue._common import SpendCapError, _utcnow


_MIN_WINDOW_SECONDS = 1
_MAX_WINDOW_SECONDS = 31 * 24 * 3600


def _align_spend_window(now: datetime, window_seconds: int) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    epoch = int(now.timestamp())
    aligned = epoch - (epoch % int(window_seconds))
    return datetime.fromtimestamp(aligned, tz=timezone.utc)


def _retry_after_seconds(
    window_start: datetime | None, window_seconds: int, now: datetime
) -> float:
    if window_start is None:
        return float(window_seconds)
    end = window_start + timedelta(seconds=int(window_seconds))
    left = (end - now).total_seconds()
    return max(0.001, left)


class TenantsSpendMixin:
    def set_tenant_spend_cap(
        self,
        tenant_id: str,
        *,
        window_seconds: int | None = None,
        max_enqueues: int | None = None,
        max_runtime_ms: int | None = None,
        clear: bool = False,
    ) -> TenantSpendCap:
        """Set or clear this tenant's spend window (operator-only).

        Pass ``clear=True`` (or both limits ``None``) to remove the cap.
        Changing ``window_seconds`` starts a new window (counters reset).
        Raising/lowering a limit does not rewrite current counters.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if clear or (max_enqueues is None and max_runtime_ms is None):
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, paused, offboarding, updated_at,
                        spend_enqueue_limit, spend_runtime_ms_limit,
                        spend_window_seconds, spend_window_start,
                        spend_enqueues, spend_runtime_ms
                    ) VALUES (
                        %s, FALSE, FALSE, %s,
                        NULL, NULL, NULL, NULL, 0, 0
                    )
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET spend_enqueue_limit = NULL,
                        spend_runtime_ms_limit = NULL,
                        spend_window_seconds = NULL,
                        spend_window_start = NULL,
                        spend_enqueues = 0,
                        spend_runtime_ms = 0,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return self._read_spend_cap_locked(conn, tenant_id, now=now)

            if window_seconds is None:
                current = conn.execute(
                    """
                    SELECT spend_window_seconds
                    FROM tenant_settings
                    WHERE tenant_id = %s
                    """,
                    (tenant_id,),
                ).fetchone()
                if current and current["spend_window_seconds"] is not None:
                    window_seconds = int(current["spend_window_seconds"])
                else:
                    raise ValueError(
                        "window_seconds is required when setting a spend cap"
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
            if max_enqueues is not None:
                max_enqueues = int(max_enqueues)
                if max_enqueues < 1:
                    raise ValueError("max_enqueues must be >= 1 or None")
            if max_runtime_ms is not None:
                max_runtime_ms = int(max_runtime_ms)
                if max_runtime_ms < 1:
                    raise ValueError("max_runtime_ms must be >= 1 or None")

            existing = conn.execute(
                """
                SELECT spend_window_seconds
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            window_changed = (
                existing is None
                or existing["spend_window_seconds"] is None
                or int(existing["spend_window_seconds"]) != window_seconds
            )
            start = _align_spend_window(now, window_seconds)
            if window_changed:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, paused, offboarding, updated_at,
                        spend_enqueue_limit, spend_runtime_ms_limit,
                        spend_window_seconds, spend_window_start,
                        spend_enqueues, spend_runtime_ms
                    ) VALUES (
                        %s, FALSE, FALSE, %s,
                        %s, %s, %s, %s, 0, 0
                    )
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET spend_enqueue_limit = EXCLUDED.spend_enqueue_limit,
                        spend_runtime_ms_limit = EXCLUDED.spend_runtime_ms_limit,
                        spend_window_seconds = EXCLUDED.spend_window_seconds,
                        spend_window_start = EXCLUDED.spend_window_start,
                        spend_enqueues = 0,
                        spend_runtime_ms = 0,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        tenant_id,
                        now,
                        max_enqueues,
                        max_runtime_ms,
                        window_seconds,
                        start,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE tenant_settings
                    SET spend_enqueue_limit = %s,
                        spend_runtime_ms_limit = %s,
                        updated_at = %s
                    WHERE tenant_id = %s
                    """,
                    (max_enqueues, max_runtime_ms, now, tenant_id),
                )
            return self._read_spend_cap_locked(conn, tenant_id, now=now)

    def get_tenant_spend_cap(self, tenant_id: str) -> TenantSpendCap:
        """Current window remaining for *tenant_id* (rolls an expired window)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            self._roll_spend_window(conn, tenant_id, now=now)
            return self._read_spend_cap_locked(conn, tenant_id, now=now)

    def _read_spend_cap_locked(
        self, conn: Any, tenant_id: str, *, now: datetime
    ) -> TenantSpendCap:
        row = conn.execute(
            """
            SELECT spend_enqueue_limit, spend_runtime_ms_limit,
                   spend_window_seconds, spend_window_start,
                   spend_enqueues, spend_runtime_ms
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["spend_window_seconds"] is None:
            return TenantSpendCap(
                tenant_id=tenant_id,
                window_seconds=None,
                window_start=None,
                window_end=None,
                max_enqueues=None,
                enqueues=0,
                max_runtime_ms=None,
                runtime_ms=0,
                enqueue_remaining=None,
                runtime_remaining_ms=None,
                exhausted=False,
                exhausted_kinds=(),
            )
        window = int(row["spend_window_seconds"])
        start = row["spend_window_start"]
        if start is not None and getattr(start, "tzinfo", None) is None:
            start = start.replace(tzinfo=timezone.utc)
        end = start + timedelta(seconds=window) if start is not None else None
        enq = int(row["spend_enqueues"] or 0)
        rt = int(row["spend_runtime_ms"] or 0)
        max_e = (
            int(row["spend_enqueue_limit"])
            if row["spend_enqueue_limit"] is not None
            else None
        )
        max_r = (
            int(row["spend_runtime_ms_limit"])
            if row["spend_runtime_ms_limit"] is not None
            else None
        )
        kinds: list[str] = []
        if max_e is not None and enq >= max_e:
            kinds.append("enqueue")
        if max_r is not None and rt >= max_r:
            kinds.append("runtime")
        return TenantSpendCap(
            tenant_id=tenant_id,
            window_seconds=window,
            window_start=start,
            window_end=end,
            max_enqueues=max_e,
            enqueues=enq,
            max_runtime_ms=max_r,
            runtime_ms=rt,
            enqueue_remaining=(None if max_e is None else max(0, max_e - enq)),
            runtime_remaining_ms=(None if max_r is None else max(0, max_r - rt)),
            exhausted=bool(kinds),
            exhausted_kinds=tuple(kinds),
        )

    def _roll_spend_window(
        self, conn: Any, tenant_id: str, *, now: datetime
    ) -> None:
        row = conn.execute(
            """
            SELECT spend_window_seconds
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["spend_window_seconds"] is None:
            return
        start = _align_spend_window(now, int(row["spend_window_seconds"]))
        conn.execute(
            """
            UPDATE tenant_settings
            SET spend_window_start = %s,
                spend_enqueues = 0,
                spend_runtime_ms = 0,
                updated_at = %s
            WHERE tenant_id = %s
              AND spend_window_seconds IS NOT NULL
              AND (spend_window_start IS NULL OR spend_window_start <> %s)
            """,
            (start, now, tenant_id, start),
        )

    def _consume_spend_enqueues(
        self,
        conn: Any,
        tenant_id: str,
        *,
        extra: int,
        now: datetime,
    ) -> None:
        """Atomically charge *extra* enqueues or raise :class:`SpendCapError`.

        The ``tenant_settings`` row lock (from this UPDATE) plus the caller
        tenant advisory lock means concurrent coordinators cannot both slip
        past the cap. Counters only move from this server path.
        """
        if extra < 1:
            return
        self._roll_spend_window(conn, tenant_id, now=now)
        row = conn.execute(
            """
            SELECT spend_enqueue_limit, spend_runtime_ms_limit,
                   spend_window_seconds, spend_window_start,
                   spend_enqueues, spend_runtime_ms
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["spend_window_seconds"] is None:
            return
        if (
            row["spend_enqueue_limit"] is None
            and row["spend_runtime_ms_limit"] is None
        ):
            return
        updated = conn.execute(
            """
            UPDATE tenant_settings
            SET spend_enqueues = spend_enqueues + %s,
                updated_at = %s
            WHERE tenant_id = %s
              AND spend_window_seconds IS NOT NULL
              AND (
                    spend_enqueue_limit IS NULL
                    OR spend_enqueues + %s <= spend_enqueue_limit
                  )
              AND (
                    spend_runtime_ms_limit IS NULL
                    OR spend_runtime_ms < spend_runtime_ms_limit
                  )
            RETURNING spend_enqueues, spend_runtime_ms,
                      spend_enqueue_limit, spend_runtime_ms_limit,
                      spend_window_start, spend_window_seconds
            """,
            (int(extra), now, tenant_id, int(extra)),
        ).fetchone()
        if updated is not None:
            return
        window = int(row["spend_window_seconds"])
        retry = _retry_after_seconds(row["spend_window_start"], window, now)
        enq = int(row["spend_enqueues"] or 0)
        rt = int(row["spend_runtime_ms"] or 0)
        if (
            row["spend_runtime_ms_limit"] is not None
            and rt >= int(row["spend_runtime_ms_limit"])
        ):
            raise SpendCapError(
                f"tenant {tenant_id!r} spend runtime cap reached "
                f"({rt}/{int(row['spend_runtime_ms_limit'])} ms); "
                "new work is refused until the window resets",
                tenant_id=tenant_id,
                kind="runtime",
                retry_after_seconds=retry,
            )
        limit = int(row["spend_enqueue_limit"] or 0)
        raise SpendCapError(
            f"tenant {tenant_id!r} spend enqueue cap reached "
            f"({enq}+{extra}/{limit}); "
            "new work is refused until the window resets",
            tenant_id=tenant_id,
            kind="enqueue",
            retry_after_seconds=retry,
        )

    def _add_spend_runtime_ms(
        self,
        conn: Any,
        tenant_id: str,
        *,
        runtime_ms: int,
        now: datetime,
    ) -> None:
        """Add billed runtime to the current window (allows in-flight overshoot)."""
        if runtime_ms < 1 or not tenant_id:
            return
        self._roll_spend_window(conn, tenant_id, now=now)
        conn.execute(
            """
            UPDATE tenant_settings
            SET spend_runtime_ms = spend_runtime_ms + %s,
                updated_at = %s
            WHERE tenant_id = %s
              AND spend_window_seconds IS NOT NULL
              AND (
                    spend_enqueue_limit IS NOT NULL
                    OR spend_runtime_ms_limit IS NOT NULL
                  )
            """,
            (int(runtime_ms), now, tenant_id),
        )

    def _spend_runtime_exhausted(
        self, conn: Any, tenant_id: str, *, now: datetime
    ) -> bool:
        self._roll_spend_window(conn, tenant_id, now=now)
        row = conn.execute(
            """
            SELECT spend_runtime_ms_limit, spend_runtime_ms
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["spend_runtime_ms_limit"] is None:
            return False
        return int(row["spend_runtime_ms"] or 0) >= int(
            row["spend_runtime_ms_limit"]
        )
