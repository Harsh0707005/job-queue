"""Exclusive if / else / switch workflow branches."""

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
    else:
        result = {"ok": True, "name": j.payload.get("step_name")}
        step = j.payload.get("step") if isinstance(j.payload.get("step"), dict) else {}
        declared = (step.get("payload") or {}).get("result")
        if isinstance(declared, dict):
            result.update(declared)
        _ack(queue, j, "w", result=result)
    return j


def _if_steps(*, choice: str):
    return [
        {
            "name": "decide",
            "kind": "effect",
            "payload": {"op": "decide", "result": {"branch": choice}},
        },
        {
            "type": "if",
            "name": "route",
            "on": "branch",
            "equals": "vip",
            "then": [
                {
                    "name": "vip-work",
                    "kind": "effect",
                    "payload": {"op": "vip"},
                    "compensate": {"kind": "effect", "payload": {"op": "undo-vip"}},
                }
            ],
            "else": [
                {
                    "name": "std-work",
                    "kind": "effect",
                    "payload": {"op": "std"},
                    "compensate": {"kind": "effect", "payload": {"op": "undo-std"}},
                }
            ],
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def test_if_then_runs_and_else_is_skipped(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="br-then",
        max_attempts=1,
        steps=_if_steps(choice="vip"),
    )
    j = _drive(queue, "br-then")
    assert j.payload.get("step_name") == "decide"
    j = _drive(queue, "br-then")
    assert j.payload.get("step_name") == "vip-work"
    j = _drive(queue, "br-then")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="br-then")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-then")
    by_name = {s.name: s for s in steps if s.direction == "forward"}
    assert by_name["vip-work"].status == WorkflowStepStatus.COMPLETED.value
    assert by_name["std-work"].status == WorkflowStepStatus.SKIPPED.value
    assert by_name["std-work"].job_id is None
    assert queue.lease(tenant_id="br-then", worker_id="w") is None


def test_if_else_runs_and_then_is_skipped(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="br-else",
        max_attempts=1,
        steps=_if_steps(choice="std"),
    )
    _drive(queue, "br-else")
    j = _drive(queue, "br-else")
    assert j.payload.get("step_name") == "std-work"
    _drive(queue, "br-else")
    done = queue.get_workflow(run.id, tenant_id="br-else")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-else")
    by_name = {s.name: s for s in steps if s.direction == "forward"}
    assert by_name["std-work"].status == WorkflowStepStatus.COMPLETED.value
    assert by_name["vip-work"].status == WorkflowStepStatus.SKIPPED.value


def test_switch_cases_pick_one_path(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="br-sw",
        max_attempts=1,
        steps=[
            {
                "name": "decide",
                "payload": {"result": {"choice": "b"}},
            },
            {
                "type": "switch",
                "on": "choice",
                "cases": [
                    {
                        "equals": "a",
                        "steps": [{"name": "do-a", "payload": {"op": "a"}}],
                    },
                    {
                        "equals": "b",
                        "steps": [{"name": "do-b", "payload": {"op": "b"}}],
                    },
                ],
                "else": [{"name": "do-z", "payload": {"op": "z"}}],
            },
        ],
    )
    _drive(queue, "br-sw")
    j = _drive(queue, "br-sw")
    assert j.payload.get("step_name") == "do-b"
    done = queue.get_workflow(run.id, tenant_id="br-sw")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-sw")
    names = {
        s.name: s.status
        for s in steps
        if s.direction == "forward"
    }
    assert names["do-b"] == WorkflowStepStatus.COMPLETED.value
    assert names["do-a"] == WorkflowStepStatus.SKIPPED.value
    assert names["do-z"] == WorkflowStepStatus.SKIPPED.value


def test_if_without_else_skips_then_when_unmatched(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="br-noelse",
        max_attempts=1,
        steps=[
            {"name": "decide", "payload": {"result": {"branch": "nope"}}},
            {
                "type": "if",
                "on": "branch",
                "equals": "yes",
                "then": [{"name": "only", "payload": {"op": "only"}}],
            },
            {"name": "after", "payload": {"op": "after"}},
        ],
    )
    _drive(queue, "br-noelse")
    j = _drive(queue, "br-noelse")
    assert j.payload.get("step_name") == "after"
    done = queue.get_workflow(run.id, tenant_id="br-noelse")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-noelse")
    only = next(s for s in steps if s.name == "only")
    assert only.status == WorkflowStepStatus.SKIPPED.value


def test_failed_then_branch_compensates_taken_path_only(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="br-fail",
        max_attempts=1,
        steps=_if_steps(choice="vip"),
    )
    _drive(queue, "br-fail")
    _drive(queue, "br-fail", fail=True)
    failed = queue.get_workflow(run.id, tenant_id="br-fail")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-fail")
    std = next(s for s in steps if s.name == "std-work")
    assert std.status == WorkflowStepStatus.SKIPPED.value
    assert not any(
        s.direction == "compensate" and s.name == "std-work"
        for s in steps
        if s.status != WorkflowStepStatus.SKIPPED.value
    )


def test_if_reads_nested_worker_envelope_result(queue: JobQueue):
    """Reclaim-style ack nests the declared payload under result."""
    run = queue.start_workflow(
        tenant_id="br-env",
        max_attempts=1,
        steps=_if_steps(choice="unused"),
    )
    j = queue.lease(tenant_id="br-env", worker_id="w")
    assert j is not None
    assert j.payload.get("step_name") == "decide"
    _ack(
        queue,
        j,
        "w",
        result={
            "workflow": True,
            "skipped": True,
            "result": {"branch": "vip"},
        },
    )
    j = _drive(queue, "br-env")
    assert j.payload.get("step_name") == "vip-work"
    _drive(queue, "br-env")
    done = queue.get_workflow(run.id, tenant_id="br-env")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="br-env")
    by_name = {s.name: s for s in steps if s.direction == "forward"}
    assert by_name["vip-work"].status == WorkflowStepStatus.COMPLETED.value
    assert by_name["std-work"].status == WorkflowStepStatus.SKIPPED.value


def test_if_requires_then_or_cases(queue: JobQueue):
    with pytest.raises(ValueError, match="then or cases"):
        queue.start_workflow(
            tenant_id="br-bad",
            steps=[{"type": "if", "name": "x", "on": "a"}],
        )
