"""Durable sleep and timer workflow steps: wake, timeout undo, reclaim."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, LeaseError
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack


def _drive(queue: JobQueue, tenant_id: str):
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})
    return j


def _sleep_steps(*, seconds: float) -> list[dict]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        {"name": "pause", "kind": "sleep", "seconds": seconds},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def _timer_steps(*, timeout: float) -> list[dict]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        {"name": "watch", "kind": "timer", "timeout_seconds": timeout},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def test_sleep_zero_completes_and_continues(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sl-0",
        max_attempts=1,
        steps=_sleep_steps(seconds=0),
    )
    _drive(queue, "sl-0")
    # Zero-second sleep fires in the same enqueue txn; ship is already pending.
    j = _drive(queue, "sl-0")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="sl-0")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="sl-0")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.COMPLETED.value
    assert pause.job_id is None
    assert pause.result["kind"] == "sleep"


def test_sleep_parks_then_wakes_without_a_job(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sl-park",
        max_attempts=1,
        steps=_sleep_steps(seconds=0.25),
    )
    _drive(queue, "sl-park")
    steps = queue.list_workflow_steps(run.id, tenant_id="sl-park")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.AWAITING_SLEEP.value
    assert pause.job_id is None
    assert pause.deadline_at is not None
    assert queue.lease(tenant_id="sl-park", worker_id="w") is None

    assert queue.process_workflow_timers(tenant_id="sl-park", limit=10) == 0
    time.sleep(0.35)
    n = queue.process_workflow_timers(tenant_id="sl-park", limit=10)
    assert n >= 1
    # Second fire is a no-op (crash-safe / double tick).
    assert queue.process_workflow_timers(tenant_id="sl-park", limit=10) == 0

    j = _drive(queue, "sl-park")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="sl-park")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_sleep_survives_missed_ticks_like_a_dead_coordinator(queue: JobQueue):
    """Wake time is on the row; a late tick still completes once."""
    run = queue.start_workflow(
        tenant_id="sl-crash",
        max_attempts=1,
        steps=_sleep_steps(seconds=0.15),
    )
    _drive(queue, "sl-crash")
    time.sleep(0.3)
    # Coordinator was "dead" for the whole window; one tick wakes it.
    assert queue.process_workflow_timers(tenant_id="sl-crash", limit=5) >= 1
    _drive(queue, "sl-crash")
    done = queue.get_workflow(run.id, tenant_id="sl-crash")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_sleep_survives_sibling_lease_reclaim(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sl-reclaim",
        max_attempts=5,
        visibility_timeout_seconds=1,
        steps=[
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "name": "slow",
                            "kind": "effect",
                            "payload": {"op": "slow"},
                        }
                    ],
                    [{"name": "pause", "kind": "sleep", "seconds": 0.2}],
                ],
            }
        ],
    )
    slow = queue.lease(
        tenant_id="sl-reclaim",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert slow is not None
    assert slow.payload.get("step_name") == "slow"
    steps = queue.list_workflow_steps(run.id, tenant_id="sl-reclaim")
    pause = next(s for s in steps if s.name == "pause")
    deadline = pause.deadline_at
    assert pause.status == WorkflowStepStatus.AWAITING_SLEEP.value

    time.sleep(1.15)
    reclaimed = queue.lease(
        tenant_id="sl-reclaim",
        worker_id="alive",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None
    assert reclaimed.id == slow.id
    with pytest.raises(LeaseError):
        _ack(queue, slow, "dead", result={"stale": True})

    still = queue.list_workflow_steps(run.id, tenant_id="sl-reclaim")
    pause2 = next(s for s in still if s.name == "pause")
    assert pause2.deadline_at == deadline
    time.sleep(0.15)
    queue.process_workflow_timers(tenant_id="sl-reclaim", limit=10)
    _ack(queue, reclaimed, "alive", result={"ok": True})
    done = queue.get_workflow(run.id, tenant_id="sl-reclaim")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_timer_zero_fails_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="tm-0",
        max_attempts=1,
        steps=_timer_steps(timeout=0),
    )
    _drive(queue, "tm-0")
    done = queue.get_workflow(run.id, tenant_id="tm-0")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPENSATING.value
    assert "timer" in (done.fail_reason or "")
    j = queue.lease(tenant_id="tm-0", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    assert j.payload.get("step_name") == "reserve"
    _ack(queue, j, "w", result={"undid": "reserve"})
    failed = queue.get_workflow(run.id, tenant_id="tm-0")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="tm-0")
    watch = next(s for s in steps if s.name == "watch")
    assert watch.status == WorkflowStepStatus.FAILED.value
    assert watch.job_id is None
    comps = [s for s in steps if s.direction == "compensate"]
    assert any(s.status == WorkflowStepStatus.COMPENSATED.value for s in comps)
    assert all(s.name != "ship" or s.status == WorkflowStepStatus.SKIPPED.value
               or s.direction == "compensate" for s in steps)


def test_timer_processor_fails_and_undoes(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="tm-dl",
        max_attempts=1,
        steps=_timer_steps(timeout=0.2),
    )
    _drive(queue, "tm-dl")
    steps = queue.list_workflow_steps(run.id, tenant_id="tm-dl")
    watch = next(s for s in steps if s.name == "watch")
    assert watch.status == WorkflowStepStatus.AWAITING_TIMER.value
    time.sleep(0.35)
    n = queue.process_workflow_timers(tenant_id="tm-dl", limit=10)
    assert n >= 1
    j = queue.lease(tenant_id="tm-dl", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w", result={"undid": True})
    failed = queue.get_workflow(run.id, tenant_id="tm-dl")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_timer_timeout_after_reclaim_still_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="tm-reclaim",
        max_attempts=5,
        visibility_timeout_seconds=1,
        steps=[
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve"},
                "compensate": {"kind": "effect", "payload": {"op": "release"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "name": "slow",
                            "kind": "effect",
                            "payload": {"op": "slow"},
                        }
                    ],
                    [{"name": "watch", "kind": "timer", "timeout_seconds": 0.4}],
                ],
            },
        ],
    )
    _drive(queue, "tm-reclaim")  # reserve
    slow = queue.lease(
        tenant_id="tm-reclaim",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert slow is not None
    assert slow.payload.get("step_name") == "slow"
    time.sleep(0.5)
    assert queue.process_workflow_timers(tenant_id="tm-reclaim", limit=10) >= 1
    parked = queue.get_workflow(run.id, tenant_id="tm-reclaim")
    assert parked is not None
    assert parked.status == WorkflowRunStatus.COMPENSATING.value

    time.sleep(1.0)
    # Stale holder cannot finish the cancelled-path sibling after reclaim.
    reclaimed = queue.lease(
        tenant_id="tm-reclaim",
        worker_id="alive",
        visibility_timeout_seconds=30,
    )
    # Either the slow job is still leased-until-expire then pending, or
    # cancel-unstarted skipped it if it was not leased... it was leased so
    # it remains; reclaim should yield it or it may already be skipped after
    # expire+settle. Drive whatever is claimable until the run is terminal.
    if reclaimed is not None:
        with pytest.raises(LeaseError):
            _ack(queue, slow, "dead", result={"stale": True})
        _ack(queue, reclaimed, "alive", result={"ok": True})
    # Compensate reserve.
    deadline = time.time() + 5
    while time.time() < deadline:
        got = queue.get_workflow(run.id, tenant_id="tm-reclaim")
        if got is not None and got.status == WorkflowRunStatus.FAILED.value:
            break
        j = queue.lease(tenant_id="tm-reclaim", worker_id="w")
        if j is None:
            queue.process_workflow_compensation(tenant_id="tm-reclaim", limit=10)
            time.sleep(0.05)
            continue
        _ack(queue, j, "w", result={"driven": j.payload.get("direction")})
    failed = queue.get_workflow(run.id, tenant_id="tm-reclaim")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="tm-reclaim")
    comps = [s for s in steps if s.direction == "compensate"]
    assert any(s.name == "reserve" and s.status == WorkflowStepStatus.COMPENSATED.value
               for s in comps)


def test_sleep_cancel_skips_parked_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sl-can",
        max_attempts=1,
        steps=_sleep_steps(seconds=30),
    )
    _drive(queue, "sl-can")
    out = queue.cancel_workflow(run.id, tenant_id="sl-can", reason="stop")
    assert out["run"].status in (
        WorkflowRunStatus.COMPENSATING.value,
        WorkflowRunStatus.CANCELLED.value,
    )
    steps = queue.list_workflow_steps(run.id, tenant_id="sl-can")
    pause = next(s for s in steps if s.name == "pause")
    assert pause.status == WorkflowStepStatus.SKIPPED.value
    j = queue.lease(tenant_id="sl-can", worker_id="w")
    if j is not None:
        assert j.payload.get("direction") == "compensate"
        _ack(queue, j, "w", result={"undid": True})
    done = queue.get_workflow(run.id, tenant_id="sl-can")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value


def test_timer_tenant_isolation(queue: JobQueue):
    a = queue.start_workflow(
        tenant_id="tm-a",
        max_attempts=1,
        steps=_sleep_steps(seconds=0.2),
    )
    b = queue.start_workflow(
        tenant_id="tm-b",
        max_attempts=1,
        steps=_timer_steps(timeout=0.2),
    )
    _drive(queue, "tm-a")
    _drive(queue, "tm-b")
    time.sleep(0.35)
    n = queue.process_workflow_timers(tenant_id="tm-a", limit=10)
    assert n >= 1
    still_b = queue.get_workflow(b.id, tenant_id="tm-b")
    assert still_b is not None
    assert still_b.status == WorkflowRunStatus.RUNNING.value
    steps_b = queue.list_workflow_steps(b.id, tenant_id="tm-b")
    assert any(s.status == WorkflowStepStatus.AWAITING_TIMER.value for s in steps_b)
    queue.process_workflow_timers(tenant_id="tm-b", limit=10)
    woke_a = queue.get_workflow(a.id, tenant_id="tm-a")
    assert woke_a is not None
    assert woke_a.status == WorkflowRunStatus.RUNNING.value
    timed_b = queue.get_workflow(b.id, tenant_id="tm-b")
    assert timed_b is not None
    assert timed_b.status == WorkflowRunStatus.COMPENSATING.value


def test_sleep_requires_duration(queue: JobQueue):
    with pytest.raises(ValueError, match="seconds|deadline"):
        queue.start_workflow(
            tenant_id="sl-bad",
            steps=[{"name": "pause", "kind": "sleep"}],
        )


def test_compensate_cannot_be_a_timer(queue: JobQueue):
    with pytest.raises(ValueError, match="compensate"):
        queue.start_workflow(
            tenant_id="tm-badc",
            steps=[
                {
                    "name": "work",
                    "kind": "effect",
                    "payload": {},
                    "compensate": {"kind": "timer", "timeout_seconds": 1},
                }
            ],
        )
