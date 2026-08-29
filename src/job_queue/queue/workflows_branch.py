"""Exclusive if / else / switch branches for workflow DAGs.

A block picks one path from the previous step's result. Unused branch
steps are inserted as ``skipped`` (no job) so later joins are not stuck
waiting on a path that will never run.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from job_queue.models import (
    WorkflowStepDirection,
    WorkflowStepStatus,
)

_IF_KINDS = frozenset({"if", "switch", "branch", "choose"})


def branch_records(sdef: dict[str, Any]) -> list[dict[str, Any]]:
    """Outer-to-inner exclusive-branch tags on a flattened leaf."""
    path = sdef.get("branch_path")
    if isinstance(path, list) and path:
        return [dict(p) for p in path if isinstance(p, dict)]
    b = sdef.get("branch")
    if isinstance(b, dict):
        return [dict(b)]
    return []


def if_block_group(*, index: int) -> str:
    """Internal exclusive-branch group id.

    Unique per if/switch block. The user-facing display name is only a
    label and may repeat, so it is never used as the group key.
    """
    return f"if:{int(index)}"


def _is_if_block(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    t = str(item.get("type") or item.get("kind") or "").strip().lower()
    if t in _IF_KINDS:
        return True
    if isinstance(item.get("then"), list) or isinstance(item.get("cases"), list):
        # Parallel uses ``branches``; map uses map kinds.
        if t in ("parallel", "map", "foreach", "fanout"):
            return False
        return True
    return False


def _lookup_result_path(result: Any, path: str) -> Any:
    if result is None:
        return None
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


def _values_equal(left: Any, right: Any) -> bool:
    if left is right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return left == right


def eval_branch_pred(pred: dict[str, Any], result: Any) -> bool:
    if pred.get("else"):
        return True
    on = str(pred.get("on") or pred.get("path") or "branch")
    val = _lookup_result_path(result, on)
    # Fleet workers may nest the declared payload under ``result``.
    if val is None and isinstance(result, dict) and isinstance(
        result.get("result"), dict
    ):
        val = _lookup_result_path(result["result"], on)
    if "equals" in pred:
        return _values_equal(val, pred["equals"])
    if "not_equals" in pred:
        return not _values_equal(val, pred["not_equals"])
    return bool(val)


def pick_branch_winner(
    cases: list[tuple[int, dict[str, Any]]], result: Any
) -> int | None:
    """First matching case; else-case if present; else None (skip all)."""
    else_id: int | None = None
    for cid, pred in cases:
        if pred.get("else"):
            else_id = cid
            continue
        if eval_branch_pred(pred, result):
            return cid
    return else_id


def normalize_if_cases(item: dict[str, Any], *, path: str) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    raw_cases = item.get("cases")
    default_on = item.get("on") or item.get("path")
    if raw_cases is not None:
        if not isinstance(raw_cases, list) or not raw_cases:
            raise ValueError(f"{path}.cases must be a non-empty list")
        for i, raw in enumerate(raw_cases):
            cpath = f"{path}.cases[{i}]"
            if not isinstance(raw, dict):
                raise ValueError(f"{cpath} must be an object")
            steps = raw.get("steps") or raw.get("then")
            if not isinstance(steps, list) or not steps:
                raise ValueError(f"{cpath}.steps must be a non-empty list")
            pred: dict[str, Any] = {}
            on = raw.get("on") or raw.get("path") or default_on
            if on is not None:
                pred["on"] = str(on)
            if "equals" in raw:
                pred["equals"] = raw["equals"]
            elif "eq" in raw:
                pred["equals"] = raw["eq"]
            elif "not_equals" in raw:
                pred["not_equals"] = raw["not_equals"]
            elif not pred.get("on"):
                raise ValueError(f"{cpath} needs on/equals")
            else:
                pred.setdefault("on", "branch")
            cases.append({"pred": pred, "steps": steps})
    elif "then" in item:
        then = item.get("then")
        if not isinstance(then, list) or not then:
            raise ValueError(f"{path}.then must be a non-empty list")
        pred = {}
        on = default_on
        if on is not None:
            pred["on"] = str(on)
        else:
            pred["on"] = "branch"
        if "equals" in item:
            pred["equals"] = item["equals"]
        elif "eq" in item:
            pred["equals"] = item["eq"]
        elif "not_equals" in item:
            pred["not_equals"] = item["not_equals"]
        cases.append({"pred": pred, "steps": then})
    else:
        raise ValueError(f"{path}: if/switch requires then or cases")

    els = item.get("else")
    if els is not None:
        if not isinstance(els, list) or not els:
            raise ValueError(f"{path}.else must be a non-empty list")
        cases.append({"pred": {"else": True}, "steps": els})
    return cases


def merge_step_results(results: list[Any]) -> Any:
    merged: dict[str, Any] = {}
    last_scalar: Any = None
    saw_scalar = False
    for res in results:
        if isinstance(res, dict):
            merged.update(res)
        elif res is not None:
            saw_scalar = True
            last_scalar = res
    if merged:
        if saw_scalar and "branch" not in merged:
            merged["branch"] = last_scalar
        return merged
    if saw_scalar:
        return {"branch": last_scalar, "value": last_scalar}
    return {}


class WorkflowsBranchMixin:
    def _skip_forward_step_locked(
        self,
        conn: Any,
        *,
        run_id: Any,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        step_index: int,
        now: Any,
        reason: str,
    ) -> None:
        sdef = steps_def[step_index]
        name = str(sdef.get("name") or f"step-{step_index}")
        conn.execute(
            f"""
            INSERT INTO workflow_step_runs (
                id, run_id, tenant_id, step_index, name, direction,
                status, job_id, result, error, idempotency_key, retry_count,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, NULL, %s, %s, %s, 0,
                %s, %s, %s, %s
            )
            ON CONFLICT (run_id, step_index, direction) DO NOTHING
            """,
            (
                uuid4(),
                run_id,
                tenant_id,
                step_index,
                name,
                WorkflowStepDirection.FORWARD.value,
                WorkflowStepStatus.SKIPPED.value,
                Jsonb({"skipped": True, "reason": reason}),
                reason,
                f"{run_id}:{step_index}:forward",
                now,
                now,
                now,
                now,
            ),
        )

    def _branch_source_result(
        self, conn: Any, *, run_id: Any, source_indices: list[int]
    ) -> Any:
        rows = []
        for idx in source_indices:
            row = conn.execute(
                """
                SELECT result
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND step_index = %s
                  AND direction = %s
                """,
                (run_id, idx, WorkflowStepDirection.FORWARD.value),
            ).fetchone()
            res = row["result"] if row else None
            if isinstance(res, str):
                import json

                res = json.loads(res)
            rows.append(res)
        return merge_step_results(rows)

    def _winner_for_branch_group(
        self,
        conn: Any,
        *,
        run_id: Any,
        steps_def: list[dict[str, Any]],
        group: str,
        source_indices: list[int] | None = None,
    ) -> int | None:
        want_src = [int(x) for x in (source_indices or [])]
        cases: dict[int, dict[str, Any]] = {}
        sources: list[int] = list(want_src)
        for sdef in steps_def:
            rec = None
            for b in branch_records(sdef):
                if str(b.get("group") or "") != group:
                    continue
                src = [int(x) for x in (b.get("source_indices") or [])]
                if want_src and src != want_src:
                    continue
                rec = b
                break
            if rec is None:
                continue
            src = [int(x) for x in (rec.get("source_indices") or [])]
            cid = int(rec["id"])
            cases[cid] = dict(rec.get("pred") or {})
            if src and not sources:
                sources = src
        ordered = [(i, cases[i]) for i in sorted(cases)]
        result = self._branch_source_result(
            conn, run_id=run_id, source_indices=sources
        )
        return pick_branch_winner(ordered, result)

    def _skip_losing_branch_steps(
        self,
        conn: Any,
        *,
        run_id: Any,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        group: str,
        winner: int | None,
        existing: set[int],
        ready: set[int],
        now: Any,
        source_indices: list[int] | None = None,
    ) -> int:
        want_src = [int(x) for x in (source_indices or [])]
        n = 0
        for i, sdef in enumerate(steps_def):
            rec = None
            for b in branch_records(sdef):
                if str(b.get("group") or "") != group:
                    continue
                src = [int(x) for x in (b.get("source_indices") or [])]
                if want_src and src != want_src:
                    continue
                rec = b
                break
            if rec is None:
                continue
            if winner is not None and int(rec.get("id")) == int(winner):
                continue
            if i in existing:
                continue
            self._skip_forward_step_locked(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                steps_def=steps_def,
                step_index=i,
                now=now,
                reason="branch not taken",
            )
            existing.add(i)
            ready.add(i)
            n += 1
        return n
