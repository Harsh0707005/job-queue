"""Single-leader lease for coordinator control-plane ticks."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from job_queue.db import transaction
from job_queue.models import CoordinatorLeader
from job_queue.queue._common import _as_utc, _utcnow

CONTROL_LEADER_SLOT = "control"

ControlTickKind = Literal["schedules", "webhooks", "alerts", "retention"]

_LEADER_COLUMNS = """
    slot, node_id, lease_token, epoch, expires_at, acquired_at, renewed_at,
    schedule_ticks, webhook_ticks, alert_ticks, retention_ticks,
    last_schedule_at, last_webhook_at, last_alert_at, last_retention_at
"""


def _row_to_leader(row: Any) -> CoordinatorLeader:
    return CoordinatorLeader(
        slot=str(row["slot"]),
        node_id=str(row["node_id"]),
        lease_token=row["lease_token"],
        epoch=int(row["epoch"]),
        expires_at=_as_utc(row["expires_at"]),
        acquired_at=_as_utc(row["acquired_at"]),
        renewed_at=_as_utc(row["renewed_at"]),
        schedule_ticks=int(row["schedule_ticks"] or 0),
        webhook_ticks=int(row["webhook_ticks"] or 0),
        alert_ticks=int(row["alert_ticks"] or 0),
        retention_ticks=int(row["retention_ticks"] or 0),
        last_schedule_at=_as_utc(row["last_schedule_at"])
        if row.get("last_schedule_at") is not None
        else None,
        last_webhook_at=_as_utc(row["last_webhook_at"])
        if row.get("last_webhook_at") is not None
        else None,
        last_alert_at=_as_utc(row["last_alert_at"])
        if row.get("last_alert_at") is not None
        else None,
        last_retention_at=_as_utc(row["last_retention_at"])
        if row.get("last_retention_at") is not None
        else None,
    )


class CoordinatorLeaderMixin:
    def acquire_or_renew_control_leader(
        self,
        *,
        node_id: str,
        lease_token: UUID | None = None,
        ttl_seconds: float = 3.0,
        slot: str = CONTROL_LEADER_SLOT,
    ) -> CoordinatorLeader | None:
        """Become or stay the control leader. None if another node holds it."""
        if not node_id or not str(node_id).strip():
            raise ValueError("node_id is required")
        node_id = str(node_id).strip()
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        token = lease_token if lease_token is not None else uuid4()
        now = _utcnow()
        expires = now + timedelta(seconds=float(ttl_seconds))
        with transaction(self._pool) as conn:
            # Two nodes booting against an empty table must not both INSERT.
            conn.execute(
                """
                INSERT INTO coordinator_leader (
                    slot, node_id, lease_token, epoch,
                    expires_at, acquired_at, renewed_at
                ) VALUES (%s, %s, %s, 1, %s, %s, %s)
                ON CONFLICT (slot) DO NOTHING
                """,
                (slot, node_id, token, expires, now, now),
            )
            row = conn.execute(
                f"""
                SELECT {_LEADER_COLUMNS}
                FROM coordinator_leader
                WHERE slot = %s
                FOR UPDATE
                """,
                (slot,),
            ).fetchone()
            if row is None:
                return None
            held_token = row["lease_token"]
            same_holder = (
                str(row["node_id"]) == node_id and held_token == token
            )
            expired = _as_utc(row["expires_at"]) <= now
            if not same_holder and not expired:
                return None
            if same_holder:
                updated = conn.execute(
                    f"""
                    UPDATE coordinator_leader
                    SET expires_at = %s,
                        renewed_at = %s
                    WHERE slot = %s
                      AND node_id = %s
                      AND lease_token = %s
                    RETURNING {_LEADER_COLUMNS}
                    """,
                    (expires, now, slot, node_id, token),
                ).fetchone()
            else:
                updated = conn.execute(
                    f"""
                    UPDATE coordinator_leader
                    SET node_id = %s,
                        lease_token = %s,
                        epoch = epoch + 1,
                        expires_at = %s,
                        acquired_at = %s,
                        renewed_at = %s
                    WHERE slot = %s
                      AND expires_at <= %s
                    RETURNING {_LEADER_COLUMNS}
                    """,
                    (node_id, token, expires, now, now, slot, now),
                ).fetchone()
            if updated is None:
                return None
            return _row_to_leader(updated)

    def _fence_control_leader(
        self,
        conn: Any,
        *,
        lease_token: UUID | None,
        epoch: int | None,
        slot: str = CONTROL_LEADER_SLOT,
    ) -> bool:
        """True if this txn still holds *lease_token* at *epoch*.

        No token means no fence (tests / HTTP without a control loop).
        ``FOR SHARE`` so a steal (``FOR UPDATE``) waits until this
        control txn commits — a tick that started while we were leader
        finishes, but a tick that starts after the steal sees the new
        epoch and refuses.
        """
        if lease_token is None:
            return True
        if epoch is None:
            return False
        row = conn.execute(
            """
            SELECT lease_token, epoch, expires_at
            FROM coordinator_leader
            WHERE slot = %s
            FOR SHARE
            """,
            (slot,),
        ).fetchone()
        if row is None:
            return False
        return (
            row["lease_token"] == lease_token
            and int(row["epoch"]) == int(epoch)
            and _as_utc(row["expires_at"]) > _utcnow()
        )

    def get_control_leader(
        self, *, slot: str = CONTROL_LEADER_SLOT
    ) -> CoordinatorLeader | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_LEADER_COLUMNS}
                FROM coordinator_leader
                WHERE slot = %s
                """,
                (slot,),
            ).fetchone()
        if row is None:
            return None
        return _row_to_leader(row)

    def holds_control_leader(
        self,
        *,
        node_id: str,
        lease_token: UUID,
        slot: str = CONTROL_LEADER_SLOT,
        now: datetime | None = None,
    ) -> bool:
        lease = self.get_control_leader(slot=slot)
        if lease is None:
            return False
        at = now if now is not None else _utcnow()
        return (
            lease.node_id == node_id
            and lease.lease_token == lease_token
            and lease.expires_at > at
        )

    def bump_control_tick(
        self,
        *,
        lease_token: UUID,
        kind: ControlTickKind,
        count: int = 1,
        slot: str = CONTROL_LEADER_SLOT,
    ) -> CoordinatorLeader | None:
        """Count a control tick only while this token still holds the lease."""
        col = {
            "schedules": ("schedule_ticks", "last_schedule_at"),
            "webhooks": ("webhook_ticks", "last_webhook_at"),
            "alerts": ("alert_ticks", "last_alert_at"),
            "retention": ("retention_ticks", "last_retention_at"),
        }.get(kind)
        if col is None:
            raise ValueError(f"unknown control tick kind {kind!r}")
        n = max(0, int(count))
        now = _utcnow()
        ticks_col, at_col = col
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE coordinator_leader
                SET {ticks_col} = {ticks_col} + %s,
                    {at_col} = %s
                WHERE slot = %s
                  AND lease_token = %s
                  AND expires_at > %s
                RETURNING {_LEADER_COLUMNS}
                """,
                (n, now, slot, lease_token, now),
            ).fetchone()
        if row is None:
            return None
        return _row_to_leader(row)

    def release_control_leader(
        self,
        *,
        node_id: str,
        lease_token: UUID,
        slot: str = CONTROL_LEADER_SLOT,
    ) -> bool:
        """Drop our lease so another node can take over immediately."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                UPDATE coordinator_leader
                SET expires_at = %s,
                    renewed_at = %s
                WHERE slot = %s
                  AND node_id = %s
                  AND lease_token = %s
                  AND expires_at > %s
                RETURNING slot
                """,
                (now, now, slot, node_id, lease_token, now),
            ).fetchone()
        return row is not None
