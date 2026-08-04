"""Multi-step saga workflows with reverse compensation."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack, _nack


def _drive_step(queue: JobQueue, tenant_id: str, *, fail: bool = False) -> None:
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    if fail:
        _nack(queue, j, "w", error="step boom", requeue=True)
    else:
        _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})


def test_workflow_happy_path_three_steps(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf1",
        name="checkout",
        max_attempts=1,
        steps=[
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve"},
                "compensate": {"kind": "effect", "payload": {"op": "release"}},
            },
            {
                "name": "charge",
                "kind": "effect",
                "payload": {"op": "charge"},
                "compensate": {"kind": "effect", "payload": {"op": "refund"}},
            },
            {
                "name": "ship",
                "kind": "effect",
                "payload": {"op": "ship"},
            },
        ],
    )
    assert run.status == WorkflowRunStatus.RUNNING.value
    for _ in range(3):
        _drive_step(queue, "wf1")
    done = queue.get_workflow(run.id, tenant_id="wf1")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf1")
    forwards = [s for s in steps if s.direction == "forward"]
    assert len(forwards) == 3
    assert [s.name for s in forwards] == ["reserve", "charge", "ship"]
    assert all(s.status == WorkflowStepStatus.COMPLETED.value for s in forwards)


def test_workflow_fail_compensates_reverse_order(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf2",
        max_attempts=1,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {
                "name": "b",
                "payload": {"op": "b"},
                "compensate": {"payload": {"op": "undo_b"}},
            },
            {
                "name": "c",
                "payload": {"op": "c"},
                "fail": True,
                "compensate": {"payload": {"op": "undo_c"}},
            },
        ],
    )
    _drive_step(queue, "wf2")  # a
    _drive_step(queue, "wf2")  # b
    _drive_step(queue, "wf2", fail=True)  # c fails → dead (max_attempts=1)

    # Compensate b then a (reverse).
    j1 = queue.lease(tenant_id="wf2", worker_id="w")
    assert j1 is not None
    assert j1.payload.get("direction") == "compensate"
    assert j1.payload.get("step_name") == "b"
    _ack(queue, j1, "w", result={"undid": "b"})

    j2 = queue.lease(tenant_id="wf2", worker_id="w")
    assert j2 is not None
    assert j2.payload.get("direction") == "compensate"
    assert j2.payload.get("step_name") == "a"
    _ack(queue, j2, "w", result={"undid": "a"})

    done = queue.get_workflow(run.id, tenant_id="wf2")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    assert done.fail_step_index == 2
    steps = queue.list_workflow_steps(run.id, tenant_id="wf2")
    comps = [
        s
        for s in steps
        if s.direction == "compensate"
        and s.status == WorkflowStepStatus.COMPENSATED.value
    ]
    # b and a compensated (c never completed). Reverse *execution* order
    # is b then a (asserted via lease order above); list is by step_index ASC.
    assert {s.name for s in comps} == {"a", "b"}
    by_done = sorted(comps, key=lambda s: s.completed_at or s.created_at)
    assert [s.name for s in by_done] == ["b", "a"]


def test_workflow_no_double_compensate_on_duplicate_terminal(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf3",
        max_attempts=1,
        steps=[
            {
                "name": "x",
                "payload": {"op": "x"},
                "compensate": {"payload": {"op": "undo_x"}},
            },
            {"name": "y", "payload": {"op": "y"}, "fail": True},
        ],
    )
    _drive_step(queue, "wf3")
    _drive_step(queue, "wf3", fail=True)
    jc = queue.lease(tenant_id="wf3", worker_id="w")
    assert jc is not None
    _ack(queue, jc, "w")
    steps1 = queue.list_workflow_steps(run.id, tenant_id="wf3")
    comps1 = [s for s in steps1 if s.direction == "compensate"]
    assert len(comps1) == 1
    assert comps1[0].status == WorkflowStepStatus.COMPENSATED.value
    # No more work.
    assert queue.lease(tenant_id="wf3", worker_id="w") is None
    done = queue.get_workflow(run.id, tenant_id="wf3")
    assert done is not None and done.status == WorkflowRunStatus.FAILED.value


def test_workflow_tenant_isolation(queue: JobQueue):
    r1 = queue.start_workflow(
        tenant_id="iso-a",
        max_attempts=1,
        steps=[{"name": "s", "payload": {}}],
    )
    with pytest.raises(ValueError, match="tenant"):
        queue.list_workflow_steps(r1.id, tenant_id="iso-b")
    assert queue.get_workflow(r1.id, tenant_id="iso-b") is None
    assert queue.get_workflow(r1.id, tenant_id="iso-a") is not None
