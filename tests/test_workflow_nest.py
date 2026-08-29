"""Nested parallel / if / map blocks: joins, unused skips, compensate."""

from __future__ import annotations

import pytest

from job_queue import JobQueue
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack, _nack


def _drive(queue: JobQueue, tenant_id: str, *, fail: bool = False):
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    if fail:
        _nack(queue, j, "w", error="boom", requeue=True)
        return j
    result = {"ok": True, "name": j.payload.get("step_name")}
    step = j.payload.get("step") if isinstance(j.payload.get("step"), dict) else {}
    declared = (step.get("payload") or {}).get("result")
    if isinstance(declared, dict):
        result.update(declared)
    _ack(queue, j, "w", result=result)
    return j


def _fwd(queue: JobQueue, run_id, tenant_id: str):
    return [
        s
        for s in queue.list_workflow_steps(run_id, tenant_id=tenant_id)
        if s.direction == "forward"
    ]


def test_parallel_branch_can_contain_if(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-pif",
        max_attempts=1,
        steps=[
            {"name": "decide", "payload": {"result": {"branch": "vip"}}},
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "type": "if",
                            "name": "route",
                            "on": "branch",
                            "equals": "vip",
                            "then": [{"name": "a-vip", "payload": {"op": "a-vip"}}],
                            "else": [{"name": "a-std", "payload": {"op": "a-std"}}],
                        }
                    ],
                    [{"name": "plain", "payload": {"op": "plain"}}],
                ],
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    assert _drive(queue, "nest-pif").payload.get("step_name") == "decide"
    names = set()
    for _ in range(2):
        names.add(_drive(queue, "nest-pif").payload.get("step_name"))
    assert names == {"a-vip", "plain"}
    assert _drive(queue, "nest-pif").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-pif")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    by = {s.name: s for s in _fwd(queue, run.id, "nest-pif")}
    assert by["a-vip"].status == WorkflowStepStatus.COMPLETED.value
    assert by["a-std"].status == WorkflowStepStatus.SKIPPED.value
    assert by["a-std"].job_id is None
    assert queue.lease(tenant_id="nest-pif", worker_id="w") is None


def test_if_then_can_contain_parallel(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-ifp",
        max_attempts=1,
        steps=[
            {"name": "decide", "payload": {"result": {"branch": "vip"}}},
            {
                "type": "if",
                "on": "branch",
                "equals": "vip",
                "then": [
                    {
                        "type": "parallel",
                        "name": "fan",
                        "branches": [
                            [{"name": "t1", "payload": {"op": "t1"}}],
                            [{"name": "t2", "payload": {"op": "t2"}}],
                        ],
                    }
                ],
                "else": [{"name": "e1", "payload": {"op": "e1"}}],
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-ifp")
    names = {_drive(queue, "nest-ifp").payload.get("step_name") for _ in range(2)}
    assert names == {"t1", "t2"}
    assert _drive(queue, "nest-ifp").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-ifp")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    by = {s.name: s for s in _fwd(queue, run.id, "nest-ifp")}
    assert by["e1"].status == WorkflowStepStatus.SKIPPED.value
    assert by["e1"].job_id is None


def test_if_else_can_contain_map(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-ifm",
        max_attempts=1,
        steps=[
            {
                "name": "decide",
                "payload": {"result": {"branch": "std", "ids": ["x", "y"]}},
            },
            {
                "type": "if",
                "on": "branch",
                "equals": "vip",
                "then": [{"name": "vip-only", "payload": {"op": "vip"}}],
                "else": [
                    {
                        "type": "map",
                        "name": "each",
                        "items": "ids",
                        "step": {"name": "work", "payload": {"op": "work"}},
                    }
                ],
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-ifm")
    kids = set()
    for _ in range(2):
        kids.add(_drive(queue, "nest-ifm").payload.get("step_name"))
    assert kids == {"work-0", "work-1"}
    assert _drive(queue, "nest-ifm").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-ifm")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    by = {s.name: s for s in _fwd(queue, run.id, "nest-ifm")}
    assert by["vip-only"].status == WorkflowStepStatus.SKIPPED.value
    assert by["each"].status == WorkflowStepStatus.COMPLETED.value


def test_unused_nested_if_skips_inner_parallel_and_map(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-skip",
        max_attempts=1,
        steps=[
            {"name": "decide", "payload": {"result": {"branch": "std"}}},
            {
                "type": "if",
                "on": "branch",
                "equals": "vip",
                "then": [
                    {
                        "type": "parallel",
                        "branches": [
                            [{"name": "inner-a", "payload": {"op": "a"}}],
                            [{"name": "inner-b", "payload": {"op": "b"}}],
                        ],
                    },
                    {
                        "type": "map",
                        "name": "dead-map",
                        "items": "ids",
                        "step": {"name": "ghost", "payload": {"op": "g"}},
                    },
                ],
                "else": [{"name": "std-work", "payload": {"op": "std"}}],
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-skip")
    assert _drive(queue, "nest-skip").payload.get("step_name") == "std-work"
    assert _drive(queue, "nest-skip").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-skip")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    by = {s.name: s for s in _fwd(queue, run.id, "nest-skip")}
    assert by["inner-a"].status == WorkflowStepStatus.SKIPPED.value
    assert by["inner-b"].status == WorkflowStepStatus.SKIPPED.value
    assert by["dead-map"].status == WorkflowStepStatus.SKIPPED.value
    assert "ghost-0" not in by
    assert queue.lease(tenant_id="nest-skip", worker_id="w") is None


def test_map_child_can_be_short_parallel(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-mp",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"result": {"ids": ["p", "q"]}}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "step": {
                    "type": "parallel",
                    "name": "pair",
                    "branches": [
                        [{"name": "left", "payload": {"op": "L"}}],
                        [{"name": "right", "payload": {"op": "R"}}],
                    ],
                },
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-mp")
    names = set()
    for _ in range(4):
        names.add(_drive(queue, "nest-mp").payload.get("step_name"))
    assert names == {"left-0", "right-0", "left-1", "right-1"}
    assert _drive(queue, "nest-mp").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-mp")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    assert queue.lease(tenant_id="nest-mp", worker_id="w") is None


def test_nested_if_then_fail_compensates_taken_path_only(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-fail",
        max_attempts=1,
        steps=[
            {
                "name": "decide",
                "payload": {"result": {"branch": "vip"}},
                "compensate": {"kind": "effect", "payload": {"op": "undo-d"}},
            },
            {
                "type": "if",
                "on": "branch",
                "equals": "vip",
                "then": [
                    {
                        "type": "parallel",
                        "branches": [
                            [
                                {
                                    "name": "boom",
                                    "payload": {"op": "boom"},
                                    "fail": True,
                                    "compensate": {
                                        "kind": "effect",
                                        "payload": {"op": "undo-boom"},
                                    },
                                }
                            ],
                            [
                                {
                                    "name": "ok",
                                    "payload": {"op": "ok"},
                                    "compensate": {
                                        "kind": "effect",
                                        "payload": {"op": "undo-ok"},
                                    },
                                }
                            ],
                        ],
                    }
                ],
                "else": [
                    {
                        "name": "else-work",
                        "payload": {"op": "e"},
                        "compensate": {"kind": "effect", "payload": {"op": "undo-e"}},
                    }
                ],
            },
        ],
    )
    _drive(queue, "nest-fail")
    # One of boom/ok. Drive until fail lands and compensate starts.
    saw_fail = False
    for _ in range(6):
        j = queue.lease(tenant_id="nest-fail", worker_id="w")
        if j is None:
            break
        if j.payload.get("step_name") == "boom" or j.payload.get("force_fail"):
            _nack(queue, j, "w", error="boom", requeue=True)
            saw_fail = True
        else:
            _ack(queue, j, "w", result={"ok": True})
    assert saw_fail
    steps = queue.list_workflow_steps(run.id, tenant_id="nest-fail")
    else_s = next(s for s in steps if s.name == "else-work")
    assert else_s.status == WorkflowStepStatus.SKIPPED.value
    assert not any(
        s.direction == "compensate" and s.name == "else-work"
        for s in steps
    )


def test_map_chain_later_leaf_waits_for_parent(queue: JobQueue):
    """A sequential map template must not lease step 2 before step 1 finishes."""
    run = queue.start_workflow(
        tenant_id="nest-chain",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"result": {"ids": ["a", "b"]}}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "step": [
                    {"name": "first", "payload": {"op": "1"}},
                    {"name": "second", "payload": {"op": "2"}},
                ],
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-chain")
    firsts = []
    for _ in range(2):
        j = queue.lease(tenant_id="nest-chain", worker_id="w")
        assert j is not None
        name = j.payload.get("step_name")
        assert name in ("first-0", "first-1")
        firsts.append(j)
    assert queue.lease(tenant_id="nest-chain", worker_id="w") is None
    _ack(queue, firsts[0], "w", result={"ok": True})
    nxt = queue.lease(tenant_id="nest-chain", worker_id="w")
    assert nxt is not None
    assert nxt.payload.get("step_name") == "second-0"
    assert queue.lease(tenant_id="nest-chain", worker_id="w") is None
    _ack(queue, nxt, "w", result={"ok": True})
    _ack(queue, firsts[1], "w", result={"ok": True})
    nxt = _drive(queue, "nest-chain")
    assert nxt.payload.get("step_name") == "second-1"
    assert _drive(queue, "nest-chain").payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="nest-chain")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_map_multileaf_max_items_counts_spawned_jobs(queue: JobQueue):
    """3 items × 2-leaf parallel = 6 jobs, so max_items=4 must fail closed."""
    run = queue.start_workflow(
        tenant_id="nest-cap",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"result": {"ids": [1, 2, 3]}}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "max_items": 4,
                "step": {
                    "type": "parallel",
                    "branches": [
                        [{"name": "left", "payload": {"op": "L"}}],
                        [{"name": "right", "payload": {"op": "R"}}],
                    ],
                },
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "nest-cap")
    assert queue.lease(tenant_id="nest-cap", worker_id="w") is None
    done = queue.get_workflow(run.id, tenant_id="nest-cap")
    assert done is not None
    assert done.status in (
        WorkflowRunStatus.FAILED.value,
        WorkflowRunStatus.COMPENSATING.value,
    )
    names = [s.name for s in _fwd(queue, run.id, "nest-cap")]
    assert not any(n.startswith("left-") or n.startswith("right-") for n in names)
    join = next(s for s in _fwd(queue, run.id, "nest-cap") if s.name == "each")
    assert join.status == WorkflowStepStatus.FAILED.value
    assert "max_items" in (join.error or "")
    assert "3 items" in (join.error or "")


def test_map_multileaf_under_job_cap_still_runs(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="nest-undercap",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"result": {"ids": ["p", "q"]}}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "max_items": 4,
                "step": {
                    "type": "parallel",
                    "branches": [
                        [{"name": "left", "payload": {"op": "L"}}],
                        [{"name": "right", "payload": {"op": "R"}}],
                    ],
                },
            },
        ],
    )
    _drive(queue, "nest-undercap")
    names = {_drive(queue, "nest-undercap").payload.get("step_name") for _ in range(4)}
    assert names == {"left-0", "right-0", "left-1", "right-1"}
    done = queue.get_workflow(run.id, tenant_id="nest-undercap")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_map_step_rejects_nested_map(queue: JobQueue):
    with pytest.raises(ValueError, match="nested map"):
        queue.start_workflow(
            tenant_id="nest-bad",
            steps=[
                {"name": "list", "payload": {"result": {"ids": [1]}}},
                {
                    "type": "map",
                    "name": "outer",
                    "items": "ids",
                    "step": {
                        "type": "map",
                        "name": "inner",
                        "items": "ids",
                        "step": {"name": "x", "payload": {}},
                    },
                },
            ],
        )
