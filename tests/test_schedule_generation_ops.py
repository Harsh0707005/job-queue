"""Cancel and replay one fan-out generation as a closed set."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _make_due
from tests.test_schedule_generation import _finish_with
from tests.test_schedule_join import _pipe


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


def test_cancel_generation_fails_payload_and_cancels_set(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "n1"}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    assert {f.schedule.id for f in fired} == {w1.id, w2.id}
    for f in fired:
        queue.freeze(f.job.id, tenant_id="t1", reason="hold")
    op = queue.cancel_schedule_generation(up.id, tenant_id="t1", iteration=1)
    assert op.action == "cancel"
    assert op.iteration == 1
    assert set(op.members) == {up.id, w1.id, w2.id, join.id}
    assert len(op.cancelled_job_ids) == 2
    gen = _gen_status(queue, up.id, 1, "t1")
    assert gen is not None and gen["status"] == "failed"
    assert gen["payload"] is None
    for job in queue.list_jobs(tenant_id="t1"):
        if job.schedule_id in {w1.id, w2.id, join.id}:
            assert job.status == JobStatus.CANCELLED
            assert job.tick_iteration == 1
    _make_due(queue, w1.id)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="generation"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="join|generation"):
        queue.fire_schedule(join.id, tenant_id="t1")


def test_replay_generation_rebrings_closed_set(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "old"}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    leftover = queue.lease(tenant_id="t1", worker_id="w")
    assert leftover is not None
    queue.freeze(leftover.id, tenant_id="t1", reason="hold w2")
    queue.cancel_schedule_generation(up.id, tenant_id="t1", iteration=1)
    replayed = queue.replay_schedule_generation(up.id, tenant_id="t1", iteration=1)
    assert replayed.action == "replay"
    assert replayed.iteration == 1
    assert replayed.job is not None
    assert replayed.job.tick_iteration == 1
    assert replayed.job.schedule_id == up.id
    assert _finish_with(queue, "t1", {"secret": "new"}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    fired = queue.process_due_schedules(tenant_id="t1")
    waiters = [f for f in fired if f.schedule.id in {w1.id, w2.id}]
    assert len(waiters) == 2
    assert all(f.schedule.id != join.id for f in fired)
    for f in waiters:
        assert f.job.tick_iteration == 1
        assert (f.job.payload.get("upstream") or {}).get("result") == {
            "secret": "new"
        }
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish_with(queue, "t1", {"w": 2}) == 1
    later = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in later] == [join.id]
    assert later[0].job.tick_iteration == 1
    assert (later[0].job.payload.get("upstream") or {}).get("result") == {
        "secret": "new"
    }


def test_replay_does_not_dump_backlog(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.update_schedule(join.id, tenant_id="t1", misfire_policy="catch_up")
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"n": 1}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    assert _finish_with(queue, "t1", {"w": 2}) == 1
    queue.cancel_schedule_generation(up.id, tenant_id="t1", iteration=1)
    queue.replay_schedule_generation(up.id, tenant_id="t1", iteration=1)
    assert _finish_with(queue, "t1", {"n": 1}) == 1
    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    _make_due(queue, join.id)
    queue.process_due_schedules(tenant_id="t1")
    assert _finish_with(queue, "t1", {"w": 1}) == 1
    assert _finish_with(queue, "t1", {"w": 2}) == 1
    first = queue.process_due_schedules(tenant_id="t1")
    assert [f.schedule.id for f in first] == [join.id]
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 7


def test_replay_incomplete_set_refuses_partial_join(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"n": 1}) == 1
    queue.delete_schedule(w2.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="incomplete"):
        queue.replay_schedule_generation(up.id, tenant_id="t1", iteration=1)
    _make_due(queue, join.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="join"):
        queue.fire_schedule(join.id, tenant_id="t1")


def test_move_during_replay_does_not_leak(queue: JobQueue):
    up, w1, w2, join = _pipe(queue)
    queue.fire_schedule(up.id, tenant_id="t1")
    queue.freeze(
        queue.list_jobs(tenant_id="t1")[0].id, tenant_id="t1", reason="hold"
    )
    queue.cancel_schedule_generation(up.id, tenant_id="t1", iteration=1)
    replayed = queue.replay_schedule_generation(up.id, tenant_id="t1", iteration=1)
    queue.freeze(replayed.job.id, tenant_id="t1", reason="hold replay")
    with pytest.raises(ScheduleError, match="fan-out generation is still live"):
        queue.move_schedule(up.id, tenant_id="t1", to_tenant_id="dst")
    queue.unfreeze(replayed.job.id, tenant_id="t1")
    assert _finish_with(queue, "t1", {"secret": "stay"}) == 1
    moved = queue.move_schedule(up.id, tenant_id="t1", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    _make_due(queue, w1.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="generation|incomplete"):
        queue.fire_schedule(w1.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="incomplete"):
        queue.replay_schedule_generation(w1.id, tenant_id="t1", iteration=1)


def test_cancel_and_replay_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        up = client.create_schedule(
            tenant_id="http-g",
            interval_seconds=3600,
            payload={"u": 1},
            name="up",
        )
        w1 = client.create_schedule(
            tenant_id="http-g",
            interval_seconds=3600,
            payload={"w": 1},
            name="w1",
            waits_on=[up["id"]],
        )
        join = client.create_schedule(
            tenant_id="http-g",
            interval_seconds=3600,
            payload={"j": 1},
            name="join",
            joins_on=[w1["id"]],
        )
        fire = client.fire_schedule(up["id"], tenant_id="http-g")
        cancelled = client.cancel_schedule_generation(
            up["id"], tenant_id="http-g", iteration=1
        )
        assert cancelled["action"] == "cancel"
        assert cancelled["iteration"] == 1
        assert fire["job"]["id"] in cancelled["cancelled_job_ids"]
        replayed = client.replay_schedule_generation(
            up["id"], tenant_id="http-g", iteration=1
        )
        assert replayed["action"] == "replay"
        assert replayed["job"]["tick_iteration"] == 1
        assert join["id"] in replayed["members"]
    finally:
        server.shutdown()
