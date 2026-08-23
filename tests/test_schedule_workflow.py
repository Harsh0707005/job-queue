"""Schedule ticks that start a multi-step workflow."""

from __future__ import annotations

from datetime import timedelta

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.models import WorkflowRunStatus
from job_queue.queue._common import _utcnow
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _diamond, _make_due


def _steps():
    return {
        "name": "tick-wf",
        "steps": [
            {"name": "a", "payload": {"op": "a"}},
            {"name": "b", "payload": {"op": "b"}},
        ],
    }


def _drive(queue: JobQueue, tenant_id: str, result: dict | None = None) -> None:
    job = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result=result or {"ok": job.payload.get("step_name")})


def test_workflow_exclusive_with_dag_jobs(queue: JobQueue):
    with pytest.raises(ValueError, match="mutually exclusive"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={"n": 1},
            dag_jobs=_diamond(),
            workflow=_steps(),
        )
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        queue.update_schedule(sched.id, tenant_id="t1", workflow=_steps())


def test_workflow_tick_stamps_schedule_and_iteration(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"kind": "wf"},
        workflow=_steps(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    assert sched.workflow is not None
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert fire.workflow_run_id is not None
    assert fire.job.schedule_id == sched.id
    assert fire.job.tick_iteration == 1
    assert fire.job.payload.get("kind") == "workflow.step"
    assert fire.job.payload.get("step_name") == "a"
    assert queue.process_due_schedules(tenant_id="t1") == []
    _drive(queue, "t1", {"ok": "a"})
    second = queue.lease(tenant_id="t1", worker_id="w")
    assert second is not None
    assert second.schedule_id == sched.id
    assert second.tick_iteration == 1
    assert second.payload.get("step_name") == "b"
    assert queue.process_due_schedules(tenant_id="t1") == []
    _ack(queue, second, "w", result={"ok": "b"})
    run = queue.get_workflow(fire.workflow_run_id, tenant_id="t1")
    assert run is not None
    assert run.status == WorkflowRunStatus.COMPLETED.value
    assert run.schedule_id == sched.id
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.last_completed_iteration == 1


def test_workflow_tick_blocks_mutex_peer(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_steps(),
        mutex_group="pipe",
        start_at=_utcnow() + timedelta(hours=1),
    )
    b = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        workflow=_steps(),
        mutex_group="pipe",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(a.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(b.id, tenant_id="t1")
    _drive(queue, "t1")
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(b.id, tenant_id="t1")
    _drive(queue, "t1")
    later = queue.fire_schedule(b.id, tenant_id="t1")
    assert later.workflow_run_id is not None


def test_workflow_tick_blocks_waiter_until_all_steps_complete(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_steps(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    _drive(queue, "t1", {"ok": "a"})
    assert queue.process_due_schedules(tenant_id="t1") == []
    _drive(queue, "t1", {"ok": "b"})
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == down.id
    up_payload = later[0].job.payload.get("upstream") or {}
    assert up_payload.get("iteration") == 1
    results = up_payload.get("result")
    assert isinstance(results, list)
    assert len(results) == 2


def _gen_status(queue: JobQueue, schedule_id, iteration: int, tenant_id: str):
    with queue._pool.connection() as conn:
        return conn.execute(
            """
            SELECT status, payload
            FROM schedule_generations
            WHERE schedule_id = %s AND iteration = %s AND tenant_id = %s
            """,
            (schedule_id, iteration, tenant_id),
        ).fetchone()


def test_workflow_does_not_seal_generation_after_first_step(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_steps(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _drive(queue, "t1", {"ok": "a"})
    gen = _gen_status(queue, up.id, 1, "t1")
    assert gen is not None
    assert gen["status"] == "pending"
    got = queue.get_schedule(up.id, tenant_id="t1")
    assert got is not None
    assert got.last_completed_iteration is None
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    _drive(queue, "t1", {"ok": "b"})
    sealed = _gen_status(queue, up.id, 1, "t1")
    assert sealed is not None
    assert sealed["status"] == "completed"
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    results = (later[0].job.payload.get("upstream") or {}).get("result")
    assert isinstance(results, list)
    assert len(results) == 2


def test_failed_workflow_tick_blocks_join_no_partial(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        max_attempts=1,
        workflow={
            "steps": [
                {"name": "ok", "payload": {"op": "ok"}},
                {"name": "boom", "payload": {"op": "boom"}, "fail": True},
            ]
        },
        start_at=_utcnow() + timedelta(hours=1),
    )
    w1 = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"w": 1},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    join = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"j": 1},
        joins_on=[w1.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _drive(queue, "t1", {"ok": True})
    boom = queue.lease(tenant_id="t1", worker_id="w")
    assert boom is not None
    from tests.helpers_queue import _nack

    _nack(queue, boom, "w", error="boom", requeue=False)
    _make_due(queue, w1.id)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="upstream|generation"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")
    assert all(
        j.schedule_id != join.id for j in queue.list_jobs(tenant_id="t1")
    )


def test_two_workflow_waiters_join_sees_both(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_steps(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    w1 = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"w": 1},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    w2 = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"w": 2},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    join = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"j": 1},
        joins_on=[w1.id, w2.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _drive(queue, "t1", {"ok": "a"})
    _drive(queue, "t1", {"ok": "b"})
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    assert {f.schedule.id for f in fired} == {w1.id, w2.id}
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result={"w": 1})
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result={"w": 2})
    _make_due(queue, join.id)
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [join.id]
    waiters = later[0].job.payload.get("waiters") or []
    assert len(waiters) == 2


def test_cancel_workflow_tick_fails_generation_waiters_and_join_not_stuck(
    queue: JobQueue,
):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow=_steps(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    w1 = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"w": 1},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    join = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"j": 1},
        joins_on=[w1.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    assert fire.workflow_run_id is not None
    _drive(queue, "t1", {"ok": "a"})
    assert _gen_status(queue, up.id, 1, "t1")["status"] == "pending"

    out = queue.cancel_workflow(
        fire.workflow_run_id, tenant_id="t1", compensate=False
    )
    assert out["run"].status == WorkflowRunStatus.CANCELLED.value
    gen = _gen_status(queue, up.id, 1, "t1")
    assert gen is not None
    assert gen["status"] == "failed"
    assert gen["payload"] is None

    _make_due(queue, w1.id)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="upstream|generation"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")

    # A later successful tick unblocks waiters/joins on the new generation.
    again = queue.fire_schedule(up.id, tenant_id="t1")
    assert again.workflow_run_id is not None
    _drive(queue, "t1", {"ok": "a2"})
    _drive(queue, "t1", {"ok": "b2"})
    assert _gen_status(queue, up.id, 1, "t1")["status"] == "failed"
    sealed = _gen_status(queue, up.id, 2, "t1")
    assert sealed is not None
    assert sealed["status"] == "completed"
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [w1.id]
    assert (later[0].job.payload.get("upstream") or {}).get("iteration") == 2
    wj = queue.lease(tenant_id="t1", worker_id="w")
    assert wj is not None and wj.id == later[0].job.id
    _ack(queue, wj, "w", result={"w": 1})
    _make_due(queue, join.id)
    joined = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in joined] == [join.id]
    waiters = joined[0].job.payload.get("waiters") or []
    assert len(waiters) == 1
    assert waiters[0].get("iteration") == 2


def test_cancel_workflow_tick_with_compensate_fails_generation(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        workflow={
            "steps": [
                {
                    "name": "a",
                    "payload": {"op": "a"},
                    "compensate": {"payload": {"op": "undo_a"}},
                },
                {"name": "b", "payload": {"op": "b"}},
            ]
        },
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    _drive(queue, "t1", {"ok": "a"})
    queue.cancel_workflow(fire.workflow_run_id, tenant_id="t1", compensate=True)
    assert _gen_status(queue, up.id, 1, "t1")["status"] == "failed"
    undo = queue.lease(tenant_id="t1", worker_id="w")
    assert undo is not None
    assert undo.payload.get("direction") == "compensate"
    _ack(queue, undo, "w")
    done = queue.get_workflow(fire.workflow_run_id, tenant_id="t1")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    assert _gen_status(queue, up.id, 1, "t1")["status"] == "failed"
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="upstream|generation"):
        queue.fire_schedule(down.id, tenant_id="t1")
    _ = JobStatus

