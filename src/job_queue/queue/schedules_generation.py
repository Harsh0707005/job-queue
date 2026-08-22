"""Closed per-generation payload from an upstream tick to waiters and join."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from job_queue.crypto import result_aad, schedule_generation_aad
from job_queue.models import JobStatus
from job_queue.queue._common import (
    _SCHEDULE_INFLIGHT_STATUSES,
    _SCHEDULE_TICK_FAILED_STATUSES,
    _utcnow,
)


class SchedulesGenerationMixin:
    def _begin_schedule_generation(
        self,
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        iteration: int,
        job_ids: list[UUID],
    ) -> None:
        if iteration < 1:
            return
        now = _utcnow()
        conn.execute(
            """
            INSERT INTO schedule_generations (
                schedule_id, iteration, tenant_id, status,
                source_job_ids, created_at, updated_at
            ) VALUES (%s, %s, %s, 'pending', %s, %s, %s)
            ON CONFLICT (schedule_id, iteration) DO NOTHING
            """,
            (schedule_id, iteration, tenant_id, list(job_ids), now, now),
        )

    def _complete_schedule_generation(
        self,
        conn: Any,
        *,
        schedule_id: UUID | None,
        tenant_id: str,
    ) -> None:
        if schedule_id is None:
            return
        gen = conn.execute(
            """
            SELECT schedule_id, iteration, tenant_id, source_job_ids
            FROM schedule_generations
            WHERE schedule_id = %s
              AND tenant_id = %s
              AND status = 'pending'
            ORDER BY iteration DESC
            LIMIT 1
            FOR UPDATE
            """,
            (schedule_id, tenant_id),
        ).fetchone()
        if gen is None:
            return
        ids = list(gen.get("source_job_ids") or [])
        if not ids:
            self._fail_schedule_generation(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                iteration=int(gen["iteration"]),
            )
            return
        rows = conn.execute(
            """
            SELECT id, status, result
            FROM jobs
            WHERE id = ANY(%s)
            """,
            (ids,),
        ).fetchall()
        if len(rows) != len(ids):
            self._fail_schedule_generation(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                iteration=int(gen["iteration"]),
            )
            return
        statuses = {str(r["status"]) for r in rows}
        if statuses & set(_SCHEDULE_INFLIGHT_STATUSES):
            return
        if statuses & set(_SCHEDULE_TICK_FAILED_STATUSES):
            self._fail_schedule_generation(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                iteration=int(gen["iteration"]),
            )
            return
        if statuses != {JobStatus.COMPLETED.value}:
            return
        results: list[Any] = []
        for row in rows:
            opened = self._open_json_value(
                row.get("result"),
                tenant_id=tenant_id,
                aad=result_aad(row["id"]),
                conn=conn,
            )
            results.append(opened)
        body: dict[str, Any] = {
            "schedule_id": str(schedule_id),
            "iteration": int(gen["iteration"]),
            "result": results[0] if len(results) == 1 else results,
        }
        sealed, kid = self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext=body,
            aad=schedule_generation_aad(
                schedule_id, int(gen["iteration"]), tenant_id=tenant_id
            ),
        )
        now = _utcnow()
        conn.execute(
            """
            UPDATE schedule_generations
            SET status = 'completed',
                payload = %s,
                payload_key_id = %s,
                updated_at = %s
            WHERE schedule_id = %s
              AND iteration = %s
              AND tenant_id = %s
              AND status = 'pending'
            """,
            (
                Jsonb(sealed),
                kid,
                now,
                schedule_id,
                int(gen["iteration"]),
                tenant_id,
            ),
        )

    def _fail_schedule_generation(
        self,
        conn: Any,
        *,
        schedule_id: UUID | None,
        tenant_id: str,
        iteration: int | None = None,
    ) -> None:
        if schedule_id is None:
            return
        now = _utcnow()
        if iteration is None:
            conn.execute(
                """
                UPDATE schedule_generations
                SET status = 'failed',
                    payload = NULL,
                    payload_key_id = NULL,
                    updated_at = %s
                WHERE schedule_id = %s
                  AND tenant_id = %s
                  AND status = 'pending'
                """,
                (now, schedule_id, tenant_id),
            )
            return
        conn.execute(
            """
            UPDATE schedule_generations
            SET status = 'failed',
                payload = NULL,
                payload_key_id = NULL,
                updated_at = %s
            WHERE schedule_id = %s
              AND iteration = %s
              AND tenant_id = %s
              AND status = 'pending'
            """,
            (now, schedule_id, iteration, tenant_id),
        )

    def _fail_schedule_generation_for_job(
        self,
        conn: Any,
        job_row: Any,
    ) -> None:
        sid = job_row.get("schedule_id") if job_row is not None else None
        if sid is None:
            return
        self._fail_schedule_generation(
            conn,
            schedule_id=sid,
            tenant_id=str(job_row["tenant_id"]),
        )

    def _open_schedule_generation(
        self,
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
        iteration: int,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT payload, status, tenant_id
            FROM schedule_generations
            WHERE schedule_id = %s AND iteration = %s
            """,
            (schedule_id, iteration),
        ).fetchone()
        if row is None:
            return None
        if str(row["status"]) != "completed":
            return None
        if str(row["tenant_id"]) != tenant_id:
            return None
        if row.get("payload") is None:
            return None
        try:
            opened = self._open_dict(
                row["payload"],
                tenant_id=tenant_id,
                aad=schedule_generation_aad(
                    schedule_id, iteration, tenant_id=tenant_id
                ),
                conn=conn,
            )
        except Exception:
            return None
        return opened if isinstance(opened, dict) else None

    def _upstream_ids_for_generation(
        self,
        conn: Any,
        sched_row: Any,
    ) -> list[UUID] | None:
        waits = list(sched_row.get("waits_on") or [])
        if waits:
            return list(waits)
        joins = list(sched_row.get("joins_on") or [])
        if not joins:
            return []
        tenant_id = str(sched_row["tenant_id"])
        rows = conn.execute(
            """
            SELECT id, waits_on
            FROM schedules
            WHERE tenant_id = %s AND id = ANY(%s)
            """,
            (tenant_id, joins),
        ).fetchall()
        if len(rows) != len(joins):
            return None
        common: set[UUID] | None = None
        for row in rows:
            ids = set(row.get("waits_on") or [])
            common = ids if common is None else (common & ids)
        return list(common or [])

    def _schedule_generation_blocked(
        self,
        conn: Any,
        sched_row: Any,
        *,
        iteration: int,
    ) -> bool:
        """True when a waiter/join cannot attach a same-tenant closed payload."""
        ups = self._upstream_ids_for_generation(conn, sched_row)
        if ups is None:
            return True
        if not ups:
            return False
        if iteration < 1:
            return False
        reached = conn.execute(
            """
            SELECT COALESCE(MAX(tick_iteration), 0) AS n
            FROM schedules
            WHERE id = ANY(%s)
            """,
            (list(ups),),
        ).fetchone()
        if int((reached or {}).get("n") or 0) < iteration:
            return False
        tenant_id = str(sched_row["tenant_id"])
        for uid in ups:
            if self._open_schedule_generation(
                conn,
                tenant_id=tenant_id,
                schedule_id=uid,
                iteration=iteration,
            ) is None:
                return True
        return False

    def _attach_generation_payload(
        self,
        conn: Any,
        sched_row: Any,
        payload: dict[str, Any],
        *,
        iteration: int,
    ) -> dict[str, Any]:
        ups = self._upstream_ids_for_generation(conn, sched_row)
        if not ups:
            return payload
        tenant_id = str(sched_row["tenant_id"])
        attached: list[dict[str, Any]] = []
        for uid in ups:
            opened = self._open_schedule_generation(
                conn,
                tenant_id=tenant_id,
                schedule_id=uid,
                iteration=iteration,
            )
            if opened is None:
                continue
            attached.append(opened)
        if not attached:
            return payload
        out = dict(payload)
        out["upstream"] = attached[0] if len(attached) == 1 else attached
        return out

    def _reseal_schedule_generations(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        from_tenant: str,
        to_tenant: str,
    ) -> None:
        rows = conn.execute(
            """
            SELECT iteration, status, payload
            FROM schedule_generations
            WHERE schedule_id = %s AND tenant_id = %s
            ORDER BY iteration
            FOR UPDATE
            """,
            (schedule_id, from_tenant),
        ).fetchall()
        now = _utcnow()
        for row in rows:
            sealed = None
            kid = None
            if str(row["status"]) == "completed" and row.get("payload") is not None:
                opened = self._open_dict(
                    row["payload"],
                    tenant_id=from_tenant,
                    aad=schedule_generation_aad(
                        schedule_id, int(row["iteration"]), tenant_id=from_tenant
                    ),
                    conn=conn,
                )
                sealed, kid = self._seal_dict(
                    conn,
                    tenant_id=to_tenant,
                    plaintext=opened if isinstance(opened, dict) else {},
                    aad=schedule_generation_aad(
                        schedule_id, int(row["iteration"]), tenant_id=to_tenant
                    ),
                )
            conn.execute(
                """
                UPDATE schedule_generations
                SET tenant_id = %s,
                    payload = %s,
                    payload_key_id = %s,
                    updated_at = %s
                WHERE schedule_id = %s
                  AND iteration = %s
                  AND tenant_id = %s
                """,
                (
                    to_tenant,
                    None if sealed is None else Jsonb(sealed),
                    kid,
                    now,
                    schedule_id,
                    int(row["iteration"]),
                    from_tenant,
                ),
            )
