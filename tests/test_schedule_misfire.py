"""Schedule miss / catch-up policy after pause or a coordinator gap."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def _backdate(queue: JobQueue, schedule_id, *, seconds: float) -> None:
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE schedules
            SET next_run_at = NOW() - (%s * INTERVAL '1 second')
            WHERE id = %s
            """,
            (seconds, schedule_id),
        )
        conn.commit()


def test_create_schedule_default_policy_is_catch_up_one(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1", interval_seconds=30, payload={"n": 1}
    )
    assert sched.misfire_policy == "catch_up_one"
    assert sched.catch_up_max == 8
    assert sched.missed_ticks == 0
    assert sched.last_misfire_at is None


def test_skip_records_misses_and_does_not_enqueue(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=5.2)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert fires == []
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 5
    assert got.last_misfire_at is not None
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 0
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_catch_up_one_fires_once_and_records_extra_misses(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=5.2)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 4
    assert got.next_run_at > _utcnow()
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_catch_up_drains_one_at_a_time_then_skips_rest(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up",
        catch_up_max=3,
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=8.2)
    ids: list = []
    for i in range(3):
        fires = queue.process_due_schedules(tenant_id="t1")
        assert len(fires) == 1, i
        job = fires[0].job
        ids.append(job.id)
        leased = queue.lease(tenant_id="t1", worker_id="w")
        assert leased is not None and leased.id == job.id
        _ack(queue, leased, "w")
    assert queue.process_due_schedules(tenant_id="t1") == []
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.next_run_at > _utcnow()
    assert got.missed_ticks >= 5
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 3
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 0


def test_catch_up_never_overlaps_in_flight(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up",
        catch_up_max=8,
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=4)
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM jobs
            WHERE schedule_id = %s AND status IN ('pending', 'leased')
            """,
            (sched.id,),
        ).fetchone()
    assert int(row["n"]) == 1


def test_pause_then_skip_does_not_flood_or_fire(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="skip",
        name="skip-me",
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    j = queue.lease(tenant_id="t1", worker_id="w")
    assert j is not None
    _ack(queue, j, "w")
    queue.pause_tenant("t1")
    time.sleep(2.4)
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.resume_tenant("t1")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert fires == []
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 2
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 0


def test_other_tenant_keeps_firing_during_skip_recovery(queue: JobQueue):
    queue.create_schedule(
        tenant_id="paused",
        interval_seconds=1,
        payload={"p": 1},
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
        misfire_policy="skip",
    )
    _backdate(queue, queue.list_schedules(tenant_id="paused")[0].id, seconds=4)
    queue.pause_tenant("paused")
    live_fires = queue.process_due_schedules(tenant_id="live")
    assert len(live_fires) == 1
    assert queue.process_due_schedules(tenant_id="paused") == []
    queue.resume_tenant("paused")
    assert queue.process_due_schedules(tenant_id="paused") == []
    assert queue.count(tenant_id="paused") == 0
    assert live_fires[0].schedule.id == live.id


def test_invalid_policy_rejected(queue: JobQueue):
    with pytest.raises(ValueError, match="misfire_policy"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=10,
            payload={},
            misfire_policy="explode",
        )
    with pytest.raises(ValueError, match="catch_up_max"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=10,
            payload={},
            misfire_policy="catch_up",
            catch_up_max=0,
        )


def test_misfire_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-s",
            interval_seconds=60,
            payload={"n": 1},
            misfire_policy="skip",
            catch_up_max=4,
            name="http-skip",
        )
        assert raw["misfire_policy"] == "skip"
        assert raw["catch_up_max"] == 4
        updated = client.set_schedule_misfire(
            raw["id"], tenant_id="http-s", misfire_policy="catch_up", catch_up_max=6
        )
        assert updated["misfire_policy"] == "catch_up"
        assert updated["catch_up_max"] == 6
    finally:
        server.shutdown()


def test_set_schedule_misfire_missing(queue: JobQueue):
    from uuid import uuid4

    with pytest.raises(ScheduleError, match="not found"):
        queue.set_schedule_misfire(uuid4(), tenant_id="t1", misfire_policy="skip")
