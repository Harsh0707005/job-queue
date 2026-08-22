"""Closed generation payload from an upstream tick to waiters and join."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, ScheduleError
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _make_due
from tests.test_schedule_join import _pipe


def _finish_with(
    queue: JobQueue, tenant_id: str, result: dict, worker: str = "w"
) -> int:
    job = queue.lease(tenant_id=tenant_id, worker_id=worker)
    if job is None:
        return 0
    _ack(queue, job, worker, result=result)
    return 1


def test_never_run_upstream_blocks_waiters_and_join(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")
    assert queue.count(tenant_id="t1") == 0
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "first", "n": 1}) == 1
    fired = queue.process_due_schedules(tenant_id="t1")
    waiters = [f.job for f in fired if f.schedule.id in {w1.id, w2.id}]
    assert len(waiters) == 2
    assert all(f.schedule.id != join.id for f in fired)
    for job in waiters:
        assert (job.payload.get("upstream") or {}).get("result") == {
            "secret": "first",
            "n": 1,
        }
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    assert _finish_with(queue, "t1", {"w": 2}) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == join.id
    waiters_payload = later[0].job.payload.get("waiters") or []
    assert sorted((w.get("result") or {})["w"] for w in waiters_payload) == [1, 2]
    assert all(w.get("iteration") == 1 for w in waiters_payload)
    assert (later[0].job.payload.get("upstream") or {}).get("result") == {
        "secret": "first",
        "n": 1,
    }


def test_waiters_and_join_see_upstream_result(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "pipe-1", "n": 1}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    waiters = [f.job for f in fired if f.schedule.id in {w1.id, w2.id}]
    assert len(waiters) == 2
    for job in waiters:
        up_payload = job.payload.get("upstream") or {}
        assert up_payload.get("iteration") == 1
        assert up_payload.get("result") == {"secret": "pipe-1", "n": 1}
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    assert _finish_with(queue, "t1", {"w": 2}) == 1
    _make_due(queue, join.id)
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == join.id
    waiters_payload = later[0].job.payload.get("waiters") or []
    assert len(waiters_payload) == 2
    assert {w.get("schedule_id") for w in waiters_payload} == {
        str(w1.id),
        str(w2.id),
    }
    assert sorted((w.get("result") or {})["w"] for w in waiters_payload) == [1, 2]
    joined = later[0].job.payload.get("upstream") or {}
    assert joined.get("iteration") == 1
    assert joined.get("result") == {"secret": "pipe-1", "n": 1}


def test_cancelled_generation_gives_nothing(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    queue.cancel(fire.job.id, tenant_id="t1")
    _make_due(queue, w1.id)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="upstream"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    again = queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "ok", "n": 2}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    waiters = [f.job for f in fired if f.schedule.id in {w1.id, w2.id}]
    assert len(waiters) == 2
    for job in waiters:
        assert (job.payload.get("upstream") or {}).get("result") == {
            "secret": "ok",
            "n": 2,
        }
        assert (job.payload.get("upstream") or {}).get("iteration") == 2


def test_move_live_upstream_does_not_leak_to_source_waiters(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    queue.freeze(fire.job.id, tenant_id="t1", reason="hold")
    with pytest.raises(ScheduleError, match="fan-out generation is still live"):
        queue.move_schedule(up.id, tenant_id="t1", to_tenant_id="dst")
    queue.unfreeze(fire.job.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "stay", "n": 1}) == 1
    moved = queue.move_schedule(up.id, tenant_id="t1", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    _make_due(queue, w1.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(w1.id, tenant_id="t1")


def test_cancelled_waiter_blocks_join_no_partial(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "x", "n": 1}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    assert {f.schedule.id for f in fired} == {w1.id, w2.id}
    for _ in range(2):
        job = queue.lease(tenant_id="t1", worker_id="w")
        assert job is not None
        if job.schedule_id == w1.id:
            _ack(queue, job, "w", result={"w": 1})
        else:
            queue.cancel(job.id, tenant_id="t1")
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")
    assert all(j.schedule_id != join.id for j in queue.list_jobs(tenant_id="t1"))


def test_failed_waiter_blocks_join_no_partial(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "x", "n": 1}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    w2_job = next(f.job for f in fired if f.schedule.id == w2.id)
    queue.freeze(w2_job.id, tenant_id="t1", reason="hold")
    queue.dead_letter_frozen(w2_job.id, tenant_id="t1", error="waiter dead")
    leftover = queue.lease(tenant_id="t1", worker_id="w")
    assert leftover is not None
    _ack(queue, leftover, "w", result={"w": 1})
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")
    assert all(j.schedule_id != join.id for j in queue.list_jobs(tenant_id="t1"))


def test_delete_waiter_blocks_partial_join(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "x", "n": 1}) == 1
    _make_due(queue, w1.id)
    queue.process_due_schedules(tenant_id="t1")
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result={"w": 1})
    queue.delete_schedule(w2.id, tenant_id="t1")
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="join"):
        queue.fire_schedule(join.id, tenant_id="t1")


def test_move_waiter_does_not_partial_join_or_leak(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "keep", "n": 1}) == 1
    _make_due(queue, w1.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    w1_fire = next(f for f in fired if f.schedule.id == w1.id)
    queue.freeze(w1_fire.job.id, tenant_id="t1", reason="hold w1")
    with pytest.raises(ScheduleError, match="fan-out generation is still live"):
        queue.move_schedule(w1.id, tenant_id="t1", to_tenant_id="dst")
    queue.unfreeze(w1_fire.job.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    moved = queue.move_schedule(w1.id, tenant_id="t1", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    leftover = queue.lease(tenant_id="t1", worker_id="w")
    if leftover is not None:
        _ack(queue, leftover, "w", result={"w": 2})
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="join"):
        queue.fire_schedule(join.id, tenant_id="t1")
