"""Per-tenant fair-lease weight (higher weight → more turns, no starvation)."""

from __future__ import annotations

from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import _utcnow


_MIN_WEIGHT = 1
_MAX_WEIGHT = 100


class TenantsWeightMixin:
    def set_tenant_lease_weight(
        self, tenant_id: str, weight: int | None
    ) -> int:
        """Set this tenant's fair-lease weight, or None to reset to 1.

        Weight is how many turns the tenant should get relative to a
        weight-1 tenant under load. Virtual-time fair queuing still
        serves lower-weight tenants (they cannot be starved).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if weight is not None:
            weight = int(weight)
            if weight < _MIN_WEIGHT or weight > _MAX_WEIGHT:
                raise ValueError(
                    f"lease_weight must be {_MIN_WEIGHT}..{_MAX_WEIGHT} or None"
                )
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, paused, offboarding, updated_at, lease_weight
                )
                VALUES (%s, FALSE, FALSE, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET lease_weight = EXCLUDED.lease_weight,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now, weight),
            )
            if weight is None:
                self._delete_empty_tenant_settings(conn, tenant_id)
        return 1 if weight is None else weight

    def get_tenant_lease_weight(self, tenant_id: str) -> int:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._read_lease_weight(conn, tenant_id)

    @staticmethod
    def _read_lease_weight(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT lease_weight
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["lease_weight"] is None:
            return 1
        return max(1, int(row["lease_weight"]))

    @staticmethod
    def _touch_tenant_fairness(conn: Any, tenant_id: str) -> None:
        """Record a serve (or skip-rotate) using weighted virtual time."""
        conn.execute(
            """
            INSERT INTO tenant_fairness (tenant_id, last_lease_at, fair_virtual)
            VALUES (
                %s,
                clock_timestamp(),
                1.0 / GREATEST(
                    1,
                    COALESCE(
                        (
                            SELECT lease_weight
                            FROM tenant_settings
                            WHERE tenant_id = %s
                        ),
                        1
                    )
                )
            )
            ON CONFLICT (tenant_id) DO UPDATE
            SET last_lease_at = clock_timestamp(),
                fair_virtual = COALESCE(tenant_fairness.fair_virtual, 0)
                    + 1.0 / GREATEST(
                        1,
                        COALESCE(
                            (
                                SELECT lease_weight
                                FROM tenant_settings
                                WHERE tenant_id = EXCLUDED.tenant_id
                            ),
                            1
                        )
                    )
            """,
            (tenant_id, tenant_id),
        )
