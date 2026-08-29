"""Dynamic workflow fan-out: a step result spawns N child steps, then joins.

A ``type: map`` (or ``foreach`` / ``fanout``) block sits in the DAG as a
``map_join`` placeholder. When every predecessor completes, the join
reads those results, materializes one child leaf per item (capped), and
only then becomes complete so later steps can run.

Overflow (more items than the cap) fails the map closed — it does not
enqueue a storm. Cancel / sibling-fail reuse the existing settle path:
unstarted children are skipped, in-flight leases finish, completed
children compensate in reverse.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import workflow_steps_aad
from job_queue.models import (
    WorkflowRunStatus,
    WorkflowStepDirection,
    WorkflowStepStatus,
)
_MAP_STEP_COLUMNS = """
    id, run_id, tenant_id, step_index, name, direction, status, job_id,
    result, error, idempotency_key, retry_count, started_at, completed_at,
    created_at, updated_at, deadline_at
"""


# Default / hard ceiling so one result cannot explode the fleet.
DEFAULT_MAP_MAX_ITEMS = 256
ABSOLUTE_MAP_MAX_ITEMS = 1024

_MAP_KINDS = frozenset({"map", "foreach", "fanout"})


def _is_map_block(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    t = str(item.get("type") or item.get("kind") or "").strip().lower()
    return t in _MAP_KINDS


def _lookup_items_path(result: Any, path: str) -> Any:
    if not path or path in (".", "result"):
        return result
    cur = result
    for part in str(path).split("."):
        key = part.strip()
        if not key:
            continue
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
    return cur


def _as_item_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    raise ValueError("map items must be a list")


class WorkflowsMapMixin:
    workflow_map_max_items = DEFAULT_MAP_MAX_ITEMS

    @staticmethod
    def _map_max_items(
        spec: dict[str, Any], *, queue_cap: int = DEFAULT_MAP_MAX_ITEMS
    ) -> int:
        raw = spec.get("max_items")
        if raw is None:
            cap = int(queue_cap)
        else:
            cap = int(raw)
        if cap < 0:
            raise ValueError("max_items must be >= 0")
        return min(cap, ABSOLUTE_MAP_MAX_ITEMS)

    def _collect_map_items(
        self,
        conn: Any,
        *,
        run_id: UUID,
        from_indices: list[int],
        path: str,
    ) -> list[Any]:
        if not from_indices:
            return []
        rows = conn.execute(
            """
            SELECT step_index, result
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = %s
              AND step_index = ANY(%s)
            """,
            (
                run_id,
                WorkflowStepDirection.FORWARD.value,
                list(from_indices),
            ),
        ).fetchall()
        by_idx = {int(r["step_index"]): r.get("result") for r in rows}
        out: list[Any] = []
        for idx in from_indices:
            raw = by_idx.get(int(idx))
            try:
                opened = self._open_step_result(raw)
            except Exception:
                opened = raw
            try:
                found = _lookup_items_path(opened, path)
                if found is None and path == "items" and isinstance(opened, list):
                    found = opened
                out.extend(_as_item_list(found))
            except ValueError as exc:
                raise ValueError(
                    f"map items at step {idx} path {path!r} are not a list"
                ) from exc
        return out

    @staticmethod
    def _open_step_result(raw: Any) -> Any:
        if raw is None:
            return None
        if hasattr(raw, "items") and not isinstance(raw, (dict, list)):
            try:
                return dict(raw)
            except Exception:
                return raw
        return raw

    def _reseal_steps_def(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        now: Any,
    ) -> None:
        sealed, key_id = self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext={"steps": steps_def},
            aad=workflow_steps_aad(run_id),
        )
        conn.execute(
            """
            UPDATE workflow_runs
            SET steps_def = %s,
                payload_key_id = COALESCE(%s, payload_key_id),
                updated_at = %s
            WHERE id = %s
            """,
            (Jsonb(sealed), key_id, now, run_id),
        )

    def _materialize_map_locked(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        map_index: int,
        now: Any,
    ) -> list[int] | None:
        """Append child leaves for a map_join. None means overflow failed the run."""
        sdef = steps_def[map_index]
        spec = dict(sdef.get("map") or {})
        if sdef.get("map_children") is not None:
            return [int(i) for i in (sdef.get("map_children") or [])]
        templates = list(spec.get("template_steps") or [])
        template = spec.get("step")
        if not templates and isinstance(template, dict):
            templates = [template]
        if not templates:
            raise ValueError(f"map {sdef.get('name')!r} is missing a step template")
        from_idx = [
            int(i)
            for i in (spec.get("from_indices") or sdef.get("depends_on_indices") or [])
        ]
        path = str(spec.get("items") or "items").strip() or "items"
        try:
            items = self._collect_map_items(
                conn, run_id=run_id, from_indices=from_idx, path=path
            )
        except ValueError as exc:
            self._fail_map_join_locked(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                steps_def=steps_def,
                map_index=map_index,
                error=str(exc),
                now=now,
            )
            return None
        try:
            cap = self._map_max_items(
                spec,
                queue_cap=int(
                    getattr(self, "workflow_map_max_items", DEFAULT_MAP_MAX_ITEMS)
                ),
            )
        except ValueError as exc:
            self._fail_map_join_locked(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                steps_def=steps_def,
                map_index=map_index,
                error=str(exc),
                now=now,
            )
            return None
        if len(items) > cap:
            self._fail_map_join_locked(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                steps_def=steps_def,
                map_index=map_index,
                error=(
                    f"map fan-out exceeds max_items ({len(items)} > {cap})"
                ),
                now=now,
            )
            return None

        group = str(sdef.get("map_group") or sdef.get("name") or f"map-{map_index}")
        child_indices: list[int] = []
        for i, item in enumerate(items):
            index_map: dict[int, int] = {}
            for t_i, raw_tmpl in enumerate(templates):
                child = deepcopy(raw_tmpl)
                payload = dict(child.get("payload") or {})
                payload["item"] = item
                payload["index"] = i
                payload["map_group"] = group
                child["payload"] = payload
                base_name = str(child.get("name") or "item").strip() or "item"
                if len(templates) == 1:
                    child["name"] = f"{base_name}-{i}"
                else:
                    child["name"] = f"{base_name}-{i}"
                rel_deps = [int(d) for d in (child.get("depends_on_indices") or [])]
                new_deps = [
                    index_map[d] for d in rel_deps if d in index_map
                ]
                if not new_deps:
                    new_deps = list(from_idx)
                child["depends_on_indices"] = new_deps
                child["map_group"] = group
                child["map_index"] = i
                child["map_sub"] = t_i
                child["map_join_index"] = map_index
                if child.get("compensate"):
                    comp = dict(child["compensate"])
                    cp = dict(comp.get("payload") or {})
                    cp["item"] = item
                    cp["index"] = i
                    cp["map_group"] = group
                    comp["payload"] = cp
                    child["compensate"] = comp
                steps_def.append(child)
                new_idx = len(steps_def) - 1
                index_map[t_i] = new_idx
                child_indices.append(new_idx)

        sdef["map_children"] = list(child_indices)
        sdef["map_count"] = len(child_indices)
        if child_indices:
            sdef["depends_on_indices"] = list(child_indices)
        self._reseal_steps_def(
            conn,
            run_id=run_id,
            tenant_id=tenant_id,
            steps_def=steps_def,
            now=now,
        )
        return child_indices

    def _complete_map_join_locked(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        map_index: int,
        now: Any,
    ) -> None:
        sdef = steps_def[map_index]
        children = [int(i) for i in (sdef.get("map_children") or [])]
        results = []
        if children:
            rows = conn.execute(
                """
                SELECT step_index, result
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND direction = %s
                  AND step_index = ANY(%s)
                ORDER BY step_index ASC
                """,
                (
                    run_id,
                    WorkflowStepDirection.FORWARD.value,
                    children,
                ),
            ).fetchall()
            by_idx = {int(r["step_index"]): r.get("result") for r in rows}
            results = [by_idx.get(i) for i in children]
        join_result = {
            "map_group": sdef.get("map_group") or sdef.get("name"),
            "count": len(children),
            "child_indices": children,
            "results": results,
        }
        name = str(sdef.get("name") or f"map-{map_index}")
        idem = f"{run_id}:{map_index}:forward"
        conn.execute(
            f"""
            INSERT INTO workflow_step_runs (
                id, run_id, tenant_id, step_index, name, direction,
                status, job_id, result, error, idempotency_key, retry_count,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, NULL, %s, NULL, %s, 0,
                %s, %s, %s, %s
            )
            ON CONFLICT (run_id, step_index, direction) DO UPDATE
            SET status = EXCLUDED.status,
                result = COALESCE(workflow_step_runs.result, EXCLUDED.result),
                completed_at = COALESCE(
                    workflow_step_runs.completed_at, EXCLUDED.completed_at
                ),
                updated_at = EXCLUDED.updated_at
            """,
            (
                uuid4(),
                run_id,
                tenant_id,
                map_index,
                name,
                WorkflowStepDirection.FORWARD.value,
                WorkflowStepStatus.COMPLETED.value,
                Jsonb(join_result),
                idem,
                now,
                now,
                now,
                now,
            ),
        )

    def _fail_map_join_locked(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        map_index: int,
        error: str,
        now: Any,
    ) -> None:
        sdef = steps_def[map_index]
        name = str(sdef.get("name") or f"map-{map_index}")
        idem = f"{run_id}:{map_index}:forward"
        conn.execute(
            f"""
            INSERT INTO workflow_step_runs (
                id, run_id, tenant_id, step_index, name, direction,
                status, job_id, result, error, idempotency_key, retry_count,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, NULL, NULL, %s, %s, 0,
                %s, %s, %s, %s
            )
            ON CONFLICT (run_id, step_index, direction) DO UPDATE
            SET status = EXCLUDED.status,
                error = COALESCE(workflow_step_runs.error, EXCLUDED.error),
                completed_at = COALESCE(
                    workflow_step_runs.completed_at, EXCLUDED.completed_at
                ),
                updated_at = EXCLUDED.updated_at
            """,
            (
                uuid4(),
                run_id,
                tenant_id,
                map_index,
                name,
                WorkflowStepDirection.FORWARD.value,
                WorkflowStepStatus.FAILED.value,
                error[:2000],
                idem,
                now,
                now,
                now,
                now,
            ),
        )
        step = conn.execute(
            f"""
            SELECT {_MAP_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s AND step_index = %s AND direction = %s
            """,
            (run_id, map_index, WorkflowStepDirection.FORWARD.value),
        ).fetchone()
        run = conn.execute(
            """
            SELECT id, tenant_id, name, status, steps_def, current_index,
                   fail_step_index, fail_reason, max_compensate_retries,
                   created_at, updated_at, completed_at, payload_key_id,
                   schedule_id, tick_iteration
            FROM workflow_runs
            WHERE id = %s
            """,
            (run_id,),
        ).fetchone()
        if step is None or run is None:
            return
        if run["status"] in (
            WorkflowRunStatus.COMPENSATING.value,
            WorkflowRunStatus.FAILED.value,
            WorkflowRunStatus.COMPLETED.value,
            WorkflowRunStatus.CANCELLED.value,
        ):
            return
        self._fail_forward_step(
            conn, run=run, step=step, error=error, now=now
        )
