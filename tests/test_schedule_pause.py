"""Pause / resume one schedule without pausing the tenant."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _diamond, _make_due


def test_pause_stops_enqueue_inflight_finishes(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    paused = queue.pause_schedule(sched.id, tenant_id="t1")
    assert paused.enabled is False
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="disabled"):
        queue.fire_schedule(sched.id, tenant_id="t1")
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None and job.id == fire.job.id
    _ack(queue, job, "w")
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    assert queue.count(tenant_id="t1") == 1


def test_pause_one_schedule_sibling_keeps_firing(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"which": "held"},
        name="held",
    )
    live = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"which": "live"},
        name="live",
    )
    queue.pause_schedule(held.id, tenant_id="t1")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == live.id
    assert queue.count(tenant_id="t1") == 1


def test_resume_skip_does_not_flood(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    queue.pause_schedule(sched.id, tenant_id="t1")
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at = NOW() - INTERVAL '8 seconds' "
            "WHERE id = %s",
            (sched.id,),
        )
        conn.commit()
    before = queue.get_schedule(sched.id, tenant_id="t1")
    assert before is not None and before.next_run_at < _utcnow()
    resumed = queue.resume_schedule(sched.id, tenant_id="t1")
    assert resumed.enabled is True
    assert resumed.next_run_at == before.next_run_at
    fires = queue.process_due_schedules(tenant_id="t1")
    assert fires == []
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 7
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 0


def test_resume_catch_up_one_fires_once(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.pause_schedule(sched.id, tenant_id="t1")
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at = NOW() - INTERVAL '5 seconds' "
            "WHERE id = %s",
            (sched.id,),
        )
        conn.commit()
    queue.resume_schedule(sched.id, tenant_id="t1")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 1


def test_pause_dag_no_second_tick(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    first = queue.fire_schedule(sched.id, tenant_id="t1")
    assert len(first.jobs) == 4
    queue.pause_schedule(sched.id, tenant_id="t1")
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 4
    done = 0
    while done < 4:
        job = queue.lease(tenant_id="t1", worker_id="w")
        if job is None:
            break
        _ack(queue, job, "w")
        done += 1
    assert done == 4
    queue.resume_schedule(sched.id, tenant_id="t1")
    second = queue.process_due_schedules(tenant_id="t1")
    assert len(second) == 1
    assert len(second[0].jobs) == 4


def test_pause_missing_and_other_tenant_live(queue: JobQueue):
    with pytest.raises(ScheduleError, match="not found"):
        queue.pause_schedule(uuid4(), tenant_id="t1")
    with pytest.raises(ScheduleError, match="not found"):
        queue.resume_schedule(uuid4(), tenant_id="t1")
    paused = queue.create_schedule(
        tenant_id="paused",
        interval_seconds=60,
        payload={"p": 1},
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    queue.pause_schedule(paused.id, tenant_id="paused")
    fires = queue.process_due_schedules()
    assert [f.schedule.id for f in fires] == [live.id]


def test_pause_resume_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-p",
            interval_seconds=3600,
            payload={"n": 1},
            name="http-pause",
        )
        paused = client.pause_schedule(raw["id"], tenant_id="http-p")
        assert paused["enabled"] is False
        next_before = paused["next_run_at"]
        resumed = client.resume_schedule(raw["id"], tenant_id="http-p")
        assert resumed["enabled"] is True
        assert resumed["next_run_at"] == next_before
    finally:
        server.shutdown()
