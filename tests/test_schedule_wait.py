"""One schedule waits for another tick (and paused mutex peers) to clear."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _diamond, _make_due
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


def test_downstream_waits_for_upstream_tick(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    assert down.waits_on == (up.id,)
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    assert queue.count(tenant_id="t1") == 1
    assert _finish(queue, "t1", 1) == 1
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == down.id


def test_downstream_waits_for_upstream_dag(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="up",
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="down",
        waits_on=[up.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 4) == 4
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == down.id


def test_paused_upstream_blocks_and_mutex_peer_pause_blocks(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        mutex_group="pipe",
        start_at=_utcnow() + timedelta(hours=1),
    )
    peer = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "peer"},
        name="peer",
        mutex_group="pipe",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.pause_schedule(up.id, tenant_id="t1")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    queue.resume_schedule(up.id, tenant_id="t1")
    queue.pause_schedule(peer.id, tenant_id="t1")
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    queue.resume_schedule(peer.id, tenant_id="t1")
    # Pause/mutex cleared, but upstream still has never ticked.
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(down.id, tenant_id="t1")
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == down.id


def test_wait_skip_does_not_dump(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _backdate(queue, down.id, seconds=5.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    still = queue.get_schedule(down.id, tenant_id="t1")
    assert still is not None and still.next_run_at < _utcnow()
    assert _finish(queue, "t1", 1) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    got = queue.get_schedule(down.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 5
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 1


def test_wait_catch_up_one_after_release(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    _backdate(queue, down.id, seconds=5.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == down.id
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 2


def test_other_tenant_and_ungrouped_sibling_keep_firing(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"u": 1},
        name="up",
    )
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"d": 1},
        name="down",
        waits_on=up.id,
    )
    free = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"f": 1},
        name="free",
    )
    live = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=60,
        payload={"ok": True},
        name="live",
    )
    queue.fire_schedule(up.id, tenant_id="t1")
    fires = queue.process_due_schedules()
    sids = {f.schedule.id for f in fires}
    assert free.id in sids
    assert live.id in sids
    assert all(f.schedule.name != "down" for f in fires)


def test_waits_on_rejects_cycle_and_missing(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="t1", interval_seconds=60, payload={"a": 1}, name="a"
    )
    b = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"b": 1},
        name="b",
        waits_on=a.id,
    )
    with pytest.raises(ValueError, match="cycle"):
        queue.update_schedule(a.id, tenant_id="t1", waits_on=[b.id])
    from uuid import uuid4

    self_id = uuid4()
    with pytest.raises(ValueError, match="itself"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            name="self",
            schedule_id=self_id,
            waits_on=self_id,
        )
    with pytest.raises(ValueError, match="not found"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            name="ghost",
            waits_on=uuid4(),
        )


def test_wait_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        up = client.create_schedule(
            tenant_id="http-w",
            interval_seconds=3600,
            payload={"u": 1},
            name="up",
        )
        down = client.create_schedule(
            tenant_id="http-w",
            interval_seconds=3600,
            payload={"d": 1},
            name="down",
            waits_on=[up["id"]],
        )
        assert down["waits_on"] == [up["id"]]
        client.fire_schedule(up["id"], tenant_id="http-w")
        with pytest.raises(ScheduleError, match="waiting on an upstream"):
            client.fire_schedule(down["id"], tenant_id="http-w")
        cleared = client.update_schedule(
            down["id"], tenant_id="http-w", waits_on=[]
        )
        assert cleared["waits_on"] == []
        client.fire_schedule(down["id"], tenant_id="http-w")
    finally:
        server.shutdown()


def test_both_due_fires_upstream_before_downstream(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    # Downstream more overdue so ORDER BY next_run_at would claim it first.
    _backdate(queue, down.id, seconds=10)
    _backdate(queue, up.id, seconds=1)
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in fires] == [up.id]
    assert queue.count(tenant_id="t1") == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == down.id


def test_move_live_upstream_still_blocks_downstream(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="src")
    queue.freeze(fire.job.id, tenant_id="src", reason="hold")
    with pytest.raises(ScheduleError, match="fan-out generation is still live"):
        queue.move_schedule(up.id, tenant_id="src", to_tenant_id="dst")
    queue.unfreeze(fire.job.id, tenant_id="src")
    assert _finish(queue, "src", 1) == 1
    moved = queue.move_schedule(up.id, tenant_id="src", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="src") == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(down.id, tenant_id="src")


def test_deleted_upstream_row_still_blocks_while_tick_live(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(up.id, tenant_id="t1")
    # Row gone, tick still live — waiter must not treat that as clear.
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM schedules WHERE id = %s", (up.id,))
        conn.commit()
    assert queue.get_schedule(up.id, tenant_id="t1") is None
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    later = queue.process_due_schedules(tenant_id="t1")
    assert later == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(down.id, tenant_id="t1")


def test_cancelled_upstream_tick_still_blocks_until_success(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    cancelled = queue.cancel(fire.job.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    again = queue.fire_schedule(up.id, tenant_id="t1")
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    assert queue.get(again.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == down.id


def test_dead_lettered_upstream_tick_still_blocks_until_success(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    queue.freeze(fire.job.id, tenant_id="t1", reason="hold")
    dead = queue.dead_letter_frozen(fire.job.id, tenant_id="t1", error="nope")
    assert dead.status == JobStatus.DEAD
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    again = queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    assert queue.get(again.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == down.id


def test_cancelled_dag_tick_blocks_until_next_completes(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="up",
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="down",
        waits_on=[up.id],
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="t1")
    queue.cancel(fire.jobs[0].id, tenant_id="t1")
    for job in fire.jobs[1:]:
        current = queue.get(job.id, tenant_id="t1")
        if current is not None and current.status not in (
            JobStatus.CANCELLED,
            JobStatus.COMPLETED,
            JobStatus.DEAD,
        ):
            queue.cancel(job.id, tenant_id="t1")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(down.id, tenant_id="t1")
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish(queue, "t1", 4) == 4
    later = queue.process_due_schedules(tenant_id="t1")
    assert len(later) == 1
    assert later[0].schedule.id == down.id


def test_cancel_after_move_or_delete_still_blocks(queue: JobQueue):
    up = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "up"},
        name="up",
        start_at=_utcnow() + timedelta(hours=1),
    )
    down = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "down"},
        name="down",
        waits_on=up.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(up.id, tenant_id="src")
    queue.freeze(fire.job.id, tenant_id="src", reason="hold")
    with pytest.raises(ScheduleError, match="fan-out generation is still live"):
        queue.move_schedule(up.id, tenant_id="src", to_tenant_id="dst")
    queue.unfreeze(fire.job.id, tenant_id="src")
    queue.cancel(fire.job.id, tenant_id="src")
    _make_due(queue, down.id)
    assert queue.process_due_schedules(tenant_id="src") == []
    with pytest.raises(ScheduleError, match="upstream"):
        queue.fire_schedule(down.id, tenant_id="src")

    other = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "up2"},
        name="up2",
        start_at=_utcnow() + timedelta(hours=1),
    )
    waiter = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "down2"},
        name="down2",
        waits_on=other.id,
        start_at=_utcnow() + timedelta(hours=1),
    )
    gone = queue.fire_schedule(other.id, tenant_id="t1")
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM schedules WHERE id = %s", (other.id,))
        conn.commit()
    queue.cancel(gone.job.id, tenant_id="t1")
    _make_due(queue, waiter.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="waiting on an upstream"):
        queue.fire_schedule(waiter.id, tenant_id="t1")
