"""Workflow run deadline: cancel leftovers, undo only completed work."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, LeaseError
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack


def _drive(queue: JobQueue, tenant_id: str, *, worker: str = "w"):
    j = queue.lease(tenant_id=tenant_id, worker_id=worker)
    assert j is not None
    _ack(queue, j, worker, result={"ok": True, "name": j.payload.get("step_name")})
    return j


def _settle(queue: JobQueue, tenant_id: str, *, timeout: float = 5) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        j = queue.lease(tenant_id=tenant_id, worker_id="settle")
        if j is not None:
            _ack(queue, j, "settle", result={"ok": True})
            continue
        time.sleep(0.02)


def _reserve_then(*rest: dict) -> list[dict]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        *list(rest),
    ]


def _assert_no_pending(steps) -> None:
    leftover = [
        (s.name, s.direction, s.status)
        for s in steps
        if s.status in (
            WorkflowStepStatus.PENDING.value,
            WorkflowStepStatus.RUNNING.value,
            WorkflowStepStatus.AWAITING_SLEEP.value,
            WorkflowStepStatus.AWAITING_TIMER.value,
            WorkflowStepStatus.AWAITING_SIGNAL.value,
            WorkflowStepStatus.AWAITING_APPROVAL.value,
        )
    ]
    assert leftover == [], leftover


def _assert_single_compensate(steps, *, name: str = "reserve") -> None:
    comps = [
        s
        for s in steps
        if s.direction == "compensate" and s.name == name
    ]
    assert len(comps) == 1, comps
    assert comps[0].status in (
        WorkflowStepStatus.COMPENSATED.value,
        WorkflowStepStatus.COMPLETED.value,
        WorkflowStepStatus.SKIPPED.value,
    )


def test_run_deadline_zero_cancels_before_any_work(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-0",
        max_attempts=1,
        deadline_seconds=0,
        steps=_reserve_then({"name": "ship", "payload": {"op": "ship"}}),
    )
    assert run.deadline_at is not None
    done = queue.get_workflow(run.id, tenant_id="rd-0")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    assert "deadline" in (done.fail_reason or "")
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-0")
    _assert_no_pending(steps)
    assert queue.lease(tenant_id="rd-0", worker_id="w") is None
    assert not any(
        s.direction == "compensate"
        and s.status
        not in (
            WorkflowStepStatus.SKIPPED.value,
            WorkflowStepStatus.COMPENSATED.value,
        )
        for s in steps
    )


def test_run_deadline_skips_sleep_and_compensates_completed(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-sl",
        max_attempts=1,
        deadline_seconds=0.2,
        steps=_reserve_then(
            {"name": "pause", "kind": "sleep", "seconds": 30},
            {"name": "ship", "payload": {"op": "ship"}},
        ),
    )
    j = _drive(queue, "rd-sl")
    assert j.payload.get("step_name") == "reserve"
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-sl")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.AWAITING_SLEEP.value
    assert pause.job_id is None

    time.sleep(0.3)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-sl", limit=10) >= 1
    assert queue.process_workflow_run_deadlines(tenant_id="rd-sl", limit=10) == 0

    j = queue.lease(tenant_id="rd-sl", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    assert j.payload.get("step_name") == "reserve"
    _ack(queue, j, "w", result={"undid": True})

    done = queue.get_workflow(run.id, tenant_id="rd-sl")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-sl")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.SKIPPED.value
    assert not any(
        s.name == "ship"
        and s.direction == "forward"
        and s.status == WorkflowStepStatus.COMPLETED.value
        for s in steps
    )
    _assert_single_compensate(steps)
    _assert_no_pending(steps)


def test_run_deadline_skips_approval_and_wait_signal(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-ap",
        max_attempts=1,
        deadline_seconds=0.2,
        steps=_reserve_then(
            {"name": "gate", "kind": "approval", "message": "hold"},
            {"name": "ship", "payload": {"op": "ship"}},
        ),
    )
    _drive(queue, "rd-ap")
    time.sleep(0.3)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-ap", limit=5) >= 1
    _settle(queue, "rd-ap")
    done = queue.get_workflow(run.id, tenant_id="rd-ap")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-ap")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.SKIPPED.value
    _assert_single_compensate(steps)
    _assert_no_pending(steps)

    sig = queue.start_workflow(
        tenant_id="rd-sig",
        max_attempts=1,
        deadline_seconds=0.2,
        steps=_reserve_then(
            {"name": "wait", "kind": "wait_signal", "signal": "go"},
            {"name": "ship", "payload": {"op": "ship"}},
        ),
    )
    _drive(queue, "rd-sig")
    time.sleep(0.3)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-sig", limit=5) >= 1
    _settle(queue, "rd-sig")
    done = queue.get_workflow(sig.id, tenant_id="rd-sig")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(sig.id, tenant_id="rd-sig")
    wait = next(s for s in steps if s.name == "wait")
    assert wait.status == WorkflowStepStatus.SKIPPED.value
    _assert_no_pending(steps)


def test_run_deadline_survives_missed_ticks(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-crash",
        max_attempts=1,
        deadline_seconds=0.15,
        steps=_reserve_then({"name": "pause", "kind": "sleep", "seconds": 30}),
    )
    _drive(queue, "rd-crash")
    time.sleep(0.3)
    # Coordinator was "dead" for the whole window; one tick cancels once.
    assert queue.process_workflow_run_deadlines(tenant_id="rd-crash", limit=5) >= 1
    assert queue.process_workflow_run_deadlines(tenant_id="rd-crash", limit=5) == 0
    _settle(queue, "rd-crash")
    done = queue.get_workflow(run.id, tenant_id="rd-crash")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-crash")
    _assert_single_compensate(steps)
    _assert_no_pending(steps)


def test_run_deadline_releases_inflight_and_survives_reclaim(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-rc",
        max_attempts=5,
        visibility_timeout_seconds=1,
        deadline_seconds=0.35,
        steps=_reserve_then(
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [{"name": "slow", "kind": "effect", "payload": {"op": "slow"}}],
                    [{"name": "pause", "kind": "sleep", "seconds": 30}],
                ],
            }
        ),
    )
    _drive(queue, "rd-rc")
    slow = queue.lease(
        tenant_id="rd-rc",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert slow is not None
    assert slow.payload.get("step_name") == "slow"
    time.sleep(0.45)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-rc", limit=5) >= 1

    with pytest.raises(LeaseError):
        _ack(queue, slow, "dead", result={"stale": True})

    time.sleep(1.15)
    reclaimed = queue.lease(
        tenant_id="rd-rc",
        worker_id="alive",
        visibility_timeout_seconds=30,
    )
    if reclaimed is not None:
        assert reclaimed.id != slow.id or reclaimed.payload.get("direction") == "compensate"
        _ack(queue, reclaimed, "alive", result={"ok": True})
    _settle(queue, "rd-rc")
    done = queue.get_workflow(run.id, tenant_id="rd-rc")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-rc")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.SKIPPED.value
    slow_step = next(s for s in steps if s.name == "slow")
    assert slow_step.status == WorkflowStepStatus.SKIPPED.value
    _assert_single_compensate(steps)
    _assert_no_pending(steps)


def test_run_deadline_does_not_touch_completed_or_other_tenant(queue: JobQueue):
    done = queue.start_workflow(
        tenant_id="rd-ok",
        max_attempts=1,
        deadline_seconds=30,
        steps=[{"name": "only", "payload": {"op": "only"}}],
    )
    _drive(queue, "rd-ok")
    finished = queue.get_workflow(done.id, tenant_id="rd-ok")
    assert finished is not None
    assert finished.status == WorkflowRunStatus.COMPLETED.value

    other = queue.start_workflow(
        tenant_id="rd-b",
        max_attempts=1,
        deadline_seconds=0.15,
        steps=_reserve_then({"name": "pause", "kind": "sleep", "seconds": 30}),
    )
    _drive(queue, "rd-b")
    time.sleep(0.25)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-ok", limit=10) == 0
    still = queue.get_workflow(done.id, tenant_id="rd-ok")
    assert still is not None
    assert still.status == WorkflowRunStatus.COMPLETED.value
    assert queue.process_workflow_run_deadlines(tenant_id="rd-b", limit=10) >= 1
    _settle(queue, "rd-b")
    cancelled = queue.get_workflow(other.id, tenant_id="rd-b")
    assert cancelled is not None
    assert cancelled.status == WorkflowRunStatus.CANCELLED.value


def test_run_deadline_future_does_not_fire(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="rd-fut",
        max_attempts=1,
        deadline_seconds=30,
        steps=_reserve_then({"name": "pause", "kind": "sleep", "seconds": 30}),
    )
    _drive(queue, "rd-fut")
    assert queue.process_workflow_run_deadlines(tenant_id="rd-fut", limit=10) == 0
    parked = queue.get_workflow(run.id, tenant_id="rd-fut")
    assert parked is not None
    assert parked.status == WorkflowRunStatus.RUNNING.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-fut")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.AWAITING_SLEEP.value


def test_run_deadline_fails_started_activity_not_left_running(queue: JobQueue):
    """A started activity must not stay running after the run deadline."""
    run = queue.start_workflow(
        tenant_id="rd-act",
        max_attempts=1,
        deadline_seconds=0.25,
        steps=_reserve_then(
            {
                "name": "charge",
                "kind": "activity",
                "activity": "charge",
                "timeout_seconds": 30,
                "heartbeat_timeout_seconds": 10,
                "payload": {"op": "charge"},
                "compensate": {"kind": "effect", "payload": {"op": "undo_charge"}},
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ),
    )
    _drive(queue, "rd-act")
    job = queue.lease(tenant_id="rd-act", worker_id="w")
    assert job is not None
    assert job.payload["step"]["kind"] == "activity"
    started = queue.start_workflow_activity(
        job.id,
        tenant_id="rd-act",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert started.status == "running"

    time.sleep(0.35)
    assert queue.process_workflow_run_deadlines(tenant_id="rd-act", limit=5) >= 1
    assert queue.process_workflow_run_deadlines(tenant_id="rd-act", limit=5) == 0
    # Due-activity sweep must not see a leftover running row.
    assert queue.process_workflow_activities(tenant_id="rd-act", limit=10) == 0

    act = queue.get_workflow_activity(job.id, tenant_id="rd-act")
    assert act is not None
    assert act.status == "failed"
    assert act.error is not None
    assert "deadline" in act.error or "cancelled" in act.error
    assert act.completed_at is not None

    with pytest.raises(LeaseError):
        queue.heartbeat_workflow_activity(
            job.id,
            tenant_id="rd-act",
            worker_id="w",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError):
        queue.complete_workflow_activity(
            job.id,
            tenant_id="rd-act",
            worker_id="w",
            lease_token=job.lease_token,
            result={"late": True},
        )
    with pytest.raises(LeaseError):
        _ack(queue, job, "w", result={"stale": True})

    steps = queue.list_workflow_steps(run.id, tenant_id="rd-act")
    charge = next(s for s in steps if s.name == "charge")
    assert charge.status == WorkflowStepStatus.SKIPPED.value

    _settle(queue, "rd-act")
    done = queue.get_workflow(run.id, tenant_id="rd-act")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="rd-act")
    _assert_no_pending(steps)
    _assert_single_compensate(steps)
    assert not any(
        s.direction == "compensate" and s.name == "charge"
        for s in steps
    )
    still = queue.get_workflow_activity(job.id, tenant_id="rd-act")
    assert still is not None
    assert still.status == "failed"


def test_run_deadline_requires_non_negative(queue: JobQueue):
    with pytest.raises(ValueError, match="deadline_seconds"):
        queue.start_workflow(
            tenant_id="rd-bad",
            deadline_seconds=-1,
            steps=[{"name": "x", "payload": {}}],
        )
