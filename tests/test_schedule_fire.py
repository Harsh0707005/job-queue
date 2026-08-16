"""Manual one-shot schedule fire."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError, SpendCapError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_fire_enqueues_one_without_waiting_for_tick(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 7},
        queue_name="metrics",
        priority=9,
        max_attempts=4,
        visibility_timeout_seconds=20,
        start_at=_utcnow() + timedelta(hours=1),
    )
    before = sched.next_run_at
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert fire.schedule.id == sched.id
    assert fire.schedule.next_run_at == before
    assert fire.schedule.last_enqueued_at is not None
    assert fire.job.payload == {"n": 7}
    assert fire.job.queue_name == "metrics"
    assert fire.job.priority == 9
    assert fire.job.max_attempts == 4
    assert fire.job.visibility_timeout_seconds == 20
    assert fire.job.schedule_id == sched.id
    assert fire.job.status == JobStatus.PENDING
    assert queue.count(tenant_id="t1") == 1
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_fire_refuses_in_flight_then_works_after_ack(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    first = queue.fire_schedule(sched.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.fire_schedule(sched.id, tenant_id="t1")
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == first.job.id
    _ack(queue, leased, "w")
    second = queue.fire_schedule(sched.id, tenant_id="t1")
    assert second.job.id != first.job.id
    assert queue.count(tenant_id="t1") == 2


def test_fire_refuses_paused_blackout_disabled_and_drain(queue: JobQueue):
    paused = queue.create_schedule(
        tenant_id="paused",
        interval_seconds=3600,
        payload={"p": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.pause_tenant("paused")
    with pytest.raises(ScheduleError, match="paused|breaker"):
        queue.fire_schedule(paused.id, tenant_id="paused")

    blocked = queue.create_schedule(
        tenant_id="blk",
        interval_seconds=3600,
        payload={"b": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.create_blackout(
        tenant_id="blk",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(minutes=10),
    )
    with pytest.raises(ScheduleError, match="blackout"):
        queue.fire_schedule(blocked.id, tenant_id="blk")

    off = queue.create_schedule(
        tenant_id="off",
        interval_seconds=3600,
        payload={"o": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.pause_schedule(off.id, tenant_id="off")
    with pytest.raises(ScheduleError, match="disabled"):
        queue.fire_schedule(off.id, tenant_id="off")

    drain_s = queue.create_schedule(
        tenant_id="dr",
        interval_seconds=3600,
        payload={"d": 1},
        queue_name="email",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.drain_tenant_queue("dr", "email")
    with pytest.raises(ScheduleError, match="draining"):
        queue.fire_schedule(drain_s.id, tenant_id="dr")

    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=3600,
        payload={"ok": True},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(live.id, tenant_id="live")
    assert fire.job.payload == {"ok": True}


def test_fire_missing_and_cross_tenant(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    with pytest.raises(ScheduleError, match="not found"):
        queue.fire_schedule(sched.id, tenant_id="other")
    with pytest.raises(ScheduleError, match="not found"):
        queue.fire_schedule(uuid4(), tenant_id="t1")
    with pytest.raises(ValueError, match="tenant_id"):
        queue.fire_schedule(sched.id, tenant_id="")


def test_fire_honors_spend_cap(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-f", window_seconds=3600, max_enqueues=1)
    sched = queue.create_schedule(
        tenant_id="cap-f",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(sched.id, tenant_id="cap-f")
    leased = queue.lease(tenant_id="cap-f", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    with pytest.raises(SpendCapError):
        queue.fire_schedule(sched.id, tenant_id="cap-f")
    assert queue.count(tenant_id="cap-f") == 1


def test_concurrent_fire_one_job(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    hits: list = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            hits.append(queue.fire_schedule(sched.id, tenant_id="t1"))
        except ScheduleError as exc:
            errors.append(exc)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert len(hits) == 1
    assert queue.count(tenant_id="t1") == 1
    assert all(isinstance(e, ScheduleError) for e in errors)
    assert len(errors) == 7


def test_other_tenant_process_due_during_manual_fire(queue: JobQueue):
    manual = queue.create_schedule(
        tenant_id="manual",
        interval_seconds=3600,
        payload={"m": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    fire = queue.fire_schedule(manual.id, tenant_id="manual")
    due = queue.process_due_schedules(tenant_id="live")
    assert len(due) == 1
    assert due[0].schedule.id == live.id
    assert fire.job.tenant_id == "manual"
    assert queue.count(tenant_id="manual") == 1
    assert queue.count(tenant_id="live") == 1


def test_fire_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        sched = client.create_schedule(
            tenant_id="http-f",
            interval_seconds=3600,
            payload={"n": 1},
            name="http-fire",
        )
        # start_at not on HTTP create; next_run may be now. Fire still works
        # as long as nothing is in flight.
        raw = client.fire_schedule(sched["id"], tenant_id="http-f")
        assert raw["job"]["schedule_id"] == sched["id"]
        assert raw["job"]["payload"] == {"n": 1}
        assert raw["schedule"]["id"] == sched["id"]
        with pytest.raises(ScheduleError, match="in-flight"):
            client.fire_schedule(sched["id"], tenant_id="http-f")
        with pytest.raises(ScheduleError, match="not found"):
            client.fire_schedule(uuid4(), tenant_id="http-f")
    finally:
        server.shutdown()
