"""Split matching schedules including a live multi-step tick."""

from __future__ import annotations

from datetime import timedelta

import pytest

from job_queue import JobQueue, JobStatus, TenantSplitError
from job_queue.crypto import schedule_generation_aad
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from job_queue.queue._common import _utcnow
from tests.helpers_queue import _ack


def _wf_wait_steps():
    return {
        "name": "tick-wf",
        "steps": [
            {
                "name": "reserve",
                "payload": {"op": "reserve"},
                "compensate": {"payload": {"op": "release"}},
            },
            {
                "name": "gate",
                "kind": "wait_signal",
                "signal": "go",
                "timeout_seconds": 60,
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    }


def test_split_moves_live_workflow_tick_and_leftover_keeps_working(
    queue: JobQueue,
):
    moving = queue.create_schedule(
        tenant_id="src",
        queue_name="email",
        interval_seconds=3600,
        payload={"kind": "wf", "secret": "tick"},
        workflow=_wf_wait_steps(),
        name="move-me",
        labels=["xfer"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    leftover = queue.create_schedule(
        tenant_id="src",
        queue_name="default",
        interval_seconds=3600,
        payload={"stay": True},
        name="stay",
        start_at=_utcnow() + timedelta(hours=1),
    )
    stay_job = queue.enqueue(
        tenant_id="src", queue_name="default", payload={"keep": True}
    )
    fire = queue.fire_schedule(moving.id, tenant_id="src")
    assert fire.workflow_run_id is not None
    rid = fire.workflow_run_id
    queue.send_workflow_signal(rid, tenant_id="src", name="early", payload={"n": 1})
    leased = queue.lease(tenant_id="src", worker_id="w1", queue_name="email")
    assert leased is not None
    assert leased.payload.get("step_name") == "reserve"

    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert plan.ok
    assert moving.id in plan.move_schedule_ids
    assert leftover.id not in plan.move_schedule_ids
    assert leased.id in plan.move_job_ids

    result = queue.split_tenant("src", "dst", queue_name="email")
    assert result.schedules_moved == 1
    assert result.schedule_ids == (moving.id,)
    assert queue.get_schedule(moving.id, tenant_id="src") is None
    assert queue.get_schedule(moving.id, tenant_id="dst") is not None
    assert queue.get_schedule(leftover.id, tenant_id="src") is not None
    assert queue.get_workflow(rid, tenant_id="src") is None
    run = queue.get_workflow(rid, tenant_id="dst")
    assert run is not None and run.status == WorkflowRunStatus.RUNNING.value
    assert queue.get(stay_job.id, tenant_id="src") is not None
    held = queue.get(leased.id, tenant_id="dst")
    assert held is not None and held.status == JobStatus.LEASED
    assert held.schedule_id == moving.id
    assert held.lease_tenant_id == "src"
    sigs = queue.list_workflow_signals(rid, tenant_id="dst")
    assert [s.name for s in sigs] == ["early"]

    acked = queue.ack(
        leased.id,
        tenant_id="src",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": "reserve"},
    )
    assert acked.tenant_id == "dst"
    parked = queue.list_workflow_steps(rid, tenant_id="dst")
    gate = next(s for s in parked if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_SIGNAL.value
    queue.send_workflow_signal(rid, tenant_id="dst", name="go")
    ship = queue.lease(tenant_id="dst", worker_id="w2", queue_name="email")
    assert ship is not None
    assert ship.payload.get("step_name") == "ship"
    _ack(queue, ship, "w2", result={"ok": "ship"})
    done = queue.get_workflow(rid, tenant_id="dst")
    assert done is not None and done.status == WorkflowRunStatus.COMPLETED.value

    keep_fire = queue.fire_schedule(leftover.id, tenant_id="src")
    finished = 0
    for _ in range(4):
        kj = queue.lease(tenant_id="src", worker_id="wk", queue_name="default")
        if kj is None:
            break
        _ack(queue, kj, "wk")
        finished += 1
    assert finished >= 1
    done_keep = queue.get(keep_fire.job.id, tenant_id="src")
    assert done_keep is not None and done_keep.status == JobStatus.COMPLETED

    with queue._pool.connection() as conn:
        gen = conn.execute(
            """
            SELECT tenant_id, status, payload
            FROM schedule_generations
            WHERE schedule_id = %s AND iteration = 1
            """,
            (moving.id,),
        ).fetchone()
        assert gen is not None
        assert gen["tenant_id"] == "dst"
        assert gen["status"] == "completed"
        opened = queue._open_dict(
            gen["payload"],
            tenant_id="dst",
            aad=schedule_generation_aad(moving.id, 1, tenant_id="dst"),
            conn=conn,
        )
        assert opened.get("iteration") == 1


def test_split_refuses_when_leftover_waiter_needs_moved_upstream(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="src",
        queue_name="email",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_wf_wait_steps(),
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    waiter = queue.create_schedule(
        tenant_id="src",
        queue_name="default",
        interval_seconds=3600,
        payload={"n": 2},
        name="w",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="src")
    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert plan.ok is False
    assert any(
        b.reason == "leftover_schedule_depends_on_moved" for b in plan.blockers
    )
    with pytest.raises(TenantSplitError, match="not graph-safe"):
        queue.split_tenant("src", "dst", queue_name="email")
    assert queue.get_schedule(up.id, tenant_id="src") is not None
    assert queue.get_schedule(waiter.id, tenant_id="src") is not None


def test_split_by_labels_moves_closed_fanout_together(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        labels=["xfer"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    w1 = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "w1"},
        name="w1",
        labels=["xfer"],
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    stay = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "stay"},
        name="stay",
        labels=["keep"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="src")
    job = queue.lease(tenant_id="src", worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result={"secret": "closed", "n": 1})
    result = queue.split_tenant("src", "dst", labels=["xfer"])
    assert set(result.schedule_ids) == {up.id, w1.id}
    assert queue.get_schedule(stay.id, tenant_id="src") is not None
    assert queue.get_schedule(up.id, tenant_id="dst") is not None
    assert queue.get(fire.job.id, tenant_id="dst") is not None
    from tests.test_schedule_dag import _make_due

    _make_due(queue, w1.id)
    fired = queue.process_due_schedules(tenant_id="dst")
    assert [f.schedule.id for f in fired] == [w1.id]
    assert (fired[0].job.payload.get("upstream") or {}).get("result") == {
        "secret": "closed",
        "n": 1,
    }
