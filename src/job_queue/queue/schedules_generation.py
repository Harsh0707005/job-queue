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
    ScheduleError,
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
            ON CONFLICT (schedule_id, iteration) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                status = 'pending',
                payload = NULL,
                payload_key_id = NULL,
                source_job_ids = EXCLUDED.source_job_ids,
                updated_at = EXCLUDED.updated_at
            WHERE schedule_generations.status = 'failed'
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
        force: bool = False,
    ) -> None:
        if schedule_id is None:
            return
        now = _utcnow()
        status_clause = "" if force else "AND status = 'pending'"
        if iteration is None:
            conn.execute(
                f"""
                UPDATE schedule_generations
                SET status = 'failed',
                    payload = NULL,
                    payload_key_id = NULL,
                    updated_at = %s
                WHERE schedule_id = %s
                  AND tenant_id = %s
                  {status_clause}
                """,
                (now, schedule_id, tenant_id),
            )
            return
        conn.execute(
            f"""
            UPDATE schedule_generations
            SET status = 'failed',
                payload = NULL,
                payload_key_id = NULL,
                updated_at = %s
            WHERE schedule_id = %s
              AND iteration = %s
              AND tenant_id = %s
              {status_clause}
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
            FOR UPDATE
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

    def _generation_source_ids(
        self,
        conn: Any,
        sched_row: Any,
    ) -> list[UUID] | None:
        """Ids whose closed generation this tick must attach.

        Waiters attach the upstream tick. A join attaches every waiter
        in ``joins_on`` for the same iteration. Missing same-tenant
        waiters fail closed (``None``) so a moved/deleted member cannot
        produce a partial join.
        """
        joins = list(sched_row.get("joins_on") or [])
        if joins:
            tenant_id = str(sched_row["tenant_id"])
            rows = conn.execute(
                """
                SELECT id
                FROM schedules
                WHERE tenant_id = %s AND id = ANY(%s)
                ORDER BY id
                FOR UPDATE
                """,
                (tenant_id, list(joins)),
            ).fetchall()
            if len(rows) != len(joins):
                return None
            return [r["id"] for r in rows]
        waits = list(sched_row.get("waits_on") or [])
        if waits:
            return list(waits)
        return []

    def _collect_schedule_generations(
        self,
        conn: Any,
        *,
        tenant_id: str,
        schedule_ids: list[UUID],
        iteration: int,
    ) -> list[dict[str, Any]] | None:
        """All completed same-tenant gens, or None if any member is missing."""
        collected: list[dict[str, Any]] = []
        for uid in schedule_ids:
            opened = self._open_schedule_generation(
                conn,
                tenant_id=tenant_id,
                schedule_id=uid,
                iteration=iteration,
            )
            if opened is None:
                return None
            collected.append(opened)
        return collected

    def _schedule_generation_blocked(
        self,
        conn: Any,
        sched_row: Any,
        *,
        iteration: int,
    ) -> bool:
        """True when a waiter/join cannot attach a same-tenant closed payload.

        Waiters need the upstream generation. A join needs every waiter's
        generation for this iteration. If any waiter failed, was cancelled,
        is on another tenant, or cannot be decrypted, the join stays
        blocked with no partial payload.
        """
        ids = self._generation_source_ids(conn, sched_row)
        if ids is None:
            return True
        if not ids:
            return False
        if iteration < 1:
            return True
        tenant_id = str(sched_row["tenant_id"])
        return (
            self._collect_schedule_generations(
                conn,
                tenant_id=tenant_id,
                schedule_ids=ids,
                iteration=iteration,
            )
            is None
        )

    def _attach_generation_payload(
        self,
        conn: Any,
        sched_row: Any,
        payload: dict[str, Any],
        *,
        iteration: int,
    ) -> dict[str, Any]:
        ids = self._generation_source_ids(conn, sched_row)
        if not ids:
            if ids is None:
                raise ScheduleError(
                    f"schedule {sched_row['id']} has no closed generation payload"
                )
            return payload
        if iteration < 1:
            raise ScheduleError(
                f"schedule {sched_row['id']} has no closed generation payload"
            )
        tenant_id = str(sched_row["tenant_id"])
        collected = self._collect_schedule_generations(
            conn,
            tenant_id=tenant_id,
            schedule_ids=ids,
            iteration=iteration,
        )
        if collected is None:
            raise ScheduleError(
                f"schedule {sched_row['id']} has no closed generation payload"
            )
        out = dict(payload)
        if list(sched_row.get("joins_on") or []):
            out["waiters"] = collected
            ups = self._upstream_ids_for_generation(conn, sched_row)
            if ups:
                upstream = self._collect_schedule_generations(
                    conn,
                    tenant_id=tenant_id,
                    schedule_ids=list(ups),
                    iteration=iteration,
                )
                if upstream is not None:
                    out["upstream"] = (
                        upstream[0] if len(upstream) == 1 else upstream
                    )
        else:
            out["upstream"] = collected[0] if len(collected) == 1 else collected
        return out

    def _open_generation_plaintext_for_move(
        self,
        conn: Any,
        *,
        payload: Any,
        schedule_id: UUID,
        iteration: int,
        from_tenant: str,
        to_tenant: str,
    ) -> dict[str, Any]:
        """Decrypt a generation using source keys, or dest if keys already moved.

        AAD stays bound to *from_tenant* until we reseal. After merge moves
        DEK rows onto dest, load-by-source-tenant would orphan the ciphertext.
        """
        last: Exception | None = None
        aad = schedule_generation_aad(
            schedule_id, iteration, tenant_id=from_tenant
        )
        for load_tid in (from_tenant, to_tenant):
            try:
                opened = self._open_dict(
                    payload,
                    tenant_id=load_tid,
                    aad=aad,
                    conn=conn,
                )
            except Exception as exc:  # noqa: BLE001
                last = exc
                continue
            return opened if isinstance(opened, dict) else {}
        raise RuntimeError(
            f"cannot open generation {schedule_id} iter {iteration} "
            f"for tenant move {from_tenant!r}->{to_tenant!r}: {last}"
        ) from last

    def _reseal_schedule_generations(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        from_tenant: str,
        to_tenant: str,
    ) -> int:
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
        n = 0
        for row in rows:
            sealed = None
            kid = None
            if str(row["status"]) == "completed" and row.get("payload") is not None:
                opened = self._open_generation_plaintext_for_move(
                    conn,
                    payload=row["payload"],
                    schedule_id=schedule_id,
                    iteration=int(row["iteration"]),
                    from_tenant=from_tenant,
                    to_tenant=to_tenant,
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
            n += 1
        return n

    def _reseal_tenant_schedule_generations(
        self,
        conn: Any,
        *,
        from_tenant: str,
        to_tenant: str,
    ) -> int:
        """Reseal every source generation under dest before joins can see it."""
        ids = list(
            dict.fromkeys(
                r["schedule_id"]
                for r in conn.execute(
                    """
                    SELECT schedule_id
                    FROM schedule_generations
                    WHERE tenant_id = %s
                    ORDER BY schedule_id, iteration
                    FOR UPDATE
                    """,
                    (from_tenant,),
                ).fetchall()
            )
        )
        n = 0
        for sid in ids:
            n += self._reseal_schedule_generations(
                conn,
                schedule_id=sid,
                from_tenant=from_tenant,
                to_tenant=to_tenant,
            )
        return n
