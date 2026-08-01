"""JobQueue side-effect query helpers."""

from __future__ import annotations

import json

from typing import Any
from uuid import UUID

from job_queue.models import (
    JobSideEffect,
)
from job_queue.queue._common import (
    _SIDE_EFFECT_SELECT,
)


class SideEffectsQueryMixin:
    _SIDE_EFFECT_SELECT = _SIDE_EFFECT_SELECT

    def side_effect_done(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        effect_key: str,
    ) -> bool:
        """True if *effect_key* was already completed for this job (any worker)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        key = self._normalize_effect_key(effect_key)
        with self._pool.connection() as conn:
            # Allow read if job currently owned by tenant OR effect row tenant matches
            # (covers merge: effects stay by job_id).
            row = conn.execute(
                """
                SELECT 1
                FROM job_side_effects se
                WHERE se.job_id = %s
                  AND se.effect_key = %s
                  AND se.status = 'completed'
                  AND (
                        se.tenant_id = %s
                        OR EXISTS (
                            SELECT 1 FROM jobs j
                            WHERE j.id = se.job_id AND j.tenant_id = %s
                        )
                  )
                """,
                (job_id, key, tenant_id, tenant_id),
            ).fetchone()
            return row is not None

    def get_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        effect_key: str,
    ) -> JobSideEffect | None:
        """Return a side-effect marker (started or completed), or None."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        key = self._normalize_effect_key(effect_key)
        cols = self._SIDE_EFFECT_SELECT
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {cols}
                FROM job_side_effects se
                WHERE se.job_id = %s
                  AND se.effect_key = %s
                  AND (
                        se.tenant_id = %s
                        OR EXISTS (
                            SELECT 1 FROM jobs j
                            WHERE j.id = se.job_id AND j.tenant_id = %s
                        )
                  )
                """,
                (job_id, key, tenant_id, tenant_id),
            ).fetchone()
            return self._row_to_side_effect(row) if row else None

    def list_side_effects(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
    ) -> list[JobSideEffect]:
        """List side effects for a job (tenant-authorized via ownership)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        cols = self._SIDE_EFFECT_SELECT
        with self._pool.connection() as conn:
            job = conn.execute(
                "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
                (job_id, tenant_id),
            ).fetchone()
            if job is None:
                # Still allow if effects exist under this tenant (job missing/rebuild).
                rows = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s AND tenant_id = %s
                    ORDER BY effect_key ASC
                    """,
                    (job_id, tenant_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT {cols}
                    FROM job_side_effects
                    WHERE job_id = %s
                    ORDER BY effect_key ASC
                    """,
                    (job_id,),
                ).fetchall()
            return [self._row_to_side_effect(r) for r in rows]

    @staticmethod
    def _normalize_effect_key(effect_key: str) -> str:
        if not effect_key or not str(effect_key).strip():
            raise ValueError("effect_key is required")
        key = str(effect_key).strip()
        if len(key) > 256:
            raise ValueError("effect_key must be <= 256 characters")
        return key

    @staticmethod
    def _row_to_side_effect(row: dict[str, Any]) -> JobSideEffect:
        result = row.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                pass
        idem = row.get("idempotency_key")
        if not idem:
            idem = f"{row['job_id']}:{row['effect_key']}"
        return JobSideEffect(
            job_id=row["job_id"],
            effect_key=row["effect_key"],
            tenant_id=row["tenant_id"],
            status=row["status"],
            idempotency_key=str(idem),
            result=result,
            started_by=row.get("started_by"),
            started_at=row.get("started_at"),
            completed_by=row.get("completed_by"),
            lease_token=row.get("lease_token"),
            created_at=row["created_at"],
            completed_at=row.get("completed_at"),
        )
