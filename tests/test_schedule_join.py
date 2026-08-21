"""Fan-out waiters plus a join that waits for the same iteration."""

from __future__ import annotations

from datetime import timedelta

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.queue._common import _utcnow
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _make_due
from tests.test_schedule_misfire import _backdate


def _finish(queue: JobQueue, tenant_id: str, n: int, worker: str = "w") -> int:
    done = 0
    while done < n:
        job = queue.lease(tenant_id=tenant_id, worker_id=worker)
        if job is None:
            break
        _ack(queue, job, worker)
        done += 1
    return done


def _pipe(queue: JobQueue, tenant_id: str = "t1"):
    up = queue.create_schedule(
        tenant_id=tenant_id,
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    w1 = queue.create_schedule(
        tenant_id=tenant_id,
        interval_seconds=3600,
        payload={"who": "w1"},
        name="w1",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    w2 = queue.create_schedule(
        tenant_id=tenant_id,
        interval_seconds=3600,
        payload={"who": "w2"},
        name="w2",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    join = queue.create_schedule(
        tenant_id=tenant_id,
        interval_seconds=3600,
        payload={"who": "join"},
        name="join",
        joins_on=[w1.id, w2.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    return up, w1, w2, join


def test_join_waits_until_every_waiter_completes(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    assert join.joins_on == (w1.id, w2.id)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    sids = {f.schedule.id for f in fired}
    assert w1.id in sids and w2.id in sids
    assert join.id not in sids
    with pytest.raises(ScheduleError, match="waiting on a join"):
        queue.fire_schedule(join.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [join.id]
    assert later[0].schedule.tick_iteration == 1


def test_paused_waiter_blocks_join(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    queue.pause_schedule(w2.id, tenant_id="t1")
    _make_due(queue, w1.id)
    _make_due(queue, join.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in fired] == [w1.id]
    assert _finish(queue, "t1", 1) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on a join"):
        queue.fire_schedule(join.id, tenant_id="t1")
    queue.resume_schedule(w2.id, tenant_id="t1")
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in fired] == [w2.id]
    assert _finish(queue, "t1", 1) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [join.id]


def test_mutex_stuck_waiter_blocks_join(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    peer = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "peer"},
        name="peer",
        mutex_group="pipe",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.update_schedule(w2.id, tenant_id="t1", mutex_group="pipe")
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    held = queue.fire_schedule(peer.id, tenant_id="t1")
    queue.freeze(held.job.id, tenant_id="t1", reason="hold peer")
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    sids = {f.schedule.id for f in fired}
    assert w1.id in sids
    assert w2.id not in sids
    assert join.id not in sids
    assert _finish(queue, "t1", 1) == 1
    with pytest.raises(ScheduleError, match="waiting on a join"):
        queue.fire_schedule(join.id, tenant_id="t1")
    queue.unfreeze(held.job.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    fired = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in fired] == [w2.id]
    assert _finish(queue, "t1", 1) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [join.id]


def test_join_one_generation_per_upstream_tick(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish(queue, "t1", 2) == 2
    _make_due(queue, join.id)
    first = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in first] == [join.id]
    assert first[0].schedule.tick_iteration == 1
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 2) == 2
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish(queue, "t1", 2) == 2
    _make_due(queue, join.id)
    second = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in second] == [join.id]
    assert second[0].schedule.tick_iteration == 2


def test_join_skip_does_not_dump(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.update_schedule(join.id, tenant_id="t1", misfire_policy="skip")
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish(queue, "t1", 2) == 2
    _backdate(queue, join.id, seconds=5.2)
    # Waiters done; join is overdue. skip records misses and does not dump.
    fires = queue.process_due_schedules(tenant_id="t1")
    # One generation is ready and due — skip with a single slot still fires
    # when there is only one missed slot; extra misses jump without enqueue.
    assert len(fires) <= 1
    got = queue.get_schedule(join.id, tenant_id="t1")
    assert got is not None
    assert got.next_run_at > _utcnow()


def test_joins_on_rejects_self_and_missing(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1", interval_seconds=60, payload={"a": 1}, name="up"
    )
    from uuid import uuid4

    self_id = uuid4()
    with pytest.raises(ValueError, match="itself"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            name="self",
            schedule_id=self_id,
            joins_on=self_id,
        )
    with pytest.raises(ValueError, match="not found"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            name="ghost",
            joins_on=uuid4(),
        )
    join = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={},
        name="join",
        joins_on=up.id,
    )
    assert join.joins_on == (up.id,)


def test_join_catch_up_one_consumes_extra_generations(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.update_schedule(join.id, tenant_id="t1", misfire_policy="catch_up_one")
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE schedules
            SET last_completed_iteration = 3, tick_iteration = 3
            WHERE id = ANY(%s)
            """,
            ([w1.id, w2.id],),
        )
        conn.commit()
    _make_due(queue, join.id)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == join.id
    assert fires[0].schedule.tick_iteration == 3
    assert fires[0].schedule.missed_ticks >= 2
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_join_catch_up_drains_one_generation(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.update_schedule(join.id, tenant_id="t1", misfire_policy="catch_up")
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE schedules
            SET last_completed_iteration = 3, tick_iteration = 3
            WHERE id = ANY(%s)
            """,
            ([w1.id, w2.id],),
        )
        conn.commit()
    _make_due(queue, join.id)
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    assert first[0].schedule.tick_iteration == 1
    assert _finish(queue, "t1", 1) == 1
    second = queue.process_due_schedules(tenant_id="t1")
    assert len(second) == 1
    assert second[0].schedule.tick_iteration == 2


def test_other_tenant_keeps_firing_while_join_blocked(queue: JobQueue):
    _pipe(queue, "t1")
    live = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=60,
        payload={"ok": True},
        name="live",
    )
    fires = queue.process_due_schedules()
    assert any(f.schedule.id == live.id for f in fires)
    assert all(f.schedule.name != "join" for f in fires)
