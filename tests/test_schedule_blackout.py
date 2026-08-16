"""Maintenance blackout windows for tenants and individual schedules."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import BlackoutError, JobQueue, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack
from tests.test_schedule_misfire import _backdate


def test_tenant_blackout_blocks_and_skip_after(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=4)
    win = queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=8),
        reason="deploy",
    )
    assert win.active is True
    assert queue.is_schedule_blacked_out(tenant_id="t1", schedule_id=sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.next_run_at < _utcnow()
    assert got.missed_ticks == 0

    queue.update_blackout(win.id, tenant_id="t1", ends_at=_utcnow())
    fires = queue.process_due_schedules(tenant_id="t1")
    assert fires == []
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 4
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 0


def test_catch_up_one_after_blackout(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=5.2)
    win = queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=8),
    )
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.delete_blackout(win.id, tenant_id="t1")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 4
    assert queue.count(tenant_id="t1") == 1


def test_catch_up_after_blackout_drains_cap(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up",
        catch_up_max=3,
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=8.2)
    win = queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=8),
    )
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.delete_blackout(win.id, tenant_id="t1")
    ids = []
    for i in range(3):
        fires = queue.process_due_schedules(tenant_id="t1")
        assert len(fires) == 1, i
        job = fires[0].job
        ids.append(job.id)
        leased = queue.lease(tenant_id="t1", worker_id="w")
        assert leased is not None and leased.id == job.id
        _ack(queue, leased, "w")
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 3


def test_schedule_blackout_does_not_block_sibling(queue: JobQueue):
    blocked = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"which": "blocked"},
        name="blocked",
        misfire_policy="skip",
    )
    live = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"which": "live"},
        name="live",
        misfire_policy="skip",
    )
    queue.create_blackout(
        tenant_id="t1",
        schedule_id=blocked.id,
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=30),
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == live.id
    assert queue.count(tenant_id="t1") == 1


def test_overlapping_tenant_and_schedule_union(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        misfire_policy="catch_up_one",
    )
    a = queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=30),
        reason="tenant",
    )
    b = queue.create_blackout(
        tenant_id="t1",
        schedule_id=sched.id,
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=40),
        reason="sched",
    )
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.delete_blackout(a.id, tenant_id="t1")
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.delete_blackout(b.id, tenant_id="t1", schedule_id=sched.id)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1


def test_daily_quiet_hours_block(queue: JobQueue):
    now = _utcnow()
    start = (now - timedelta(minutes=2)).time().replace(microsecond=0)
    end = (now + timedelta(minutes=10)).time().replace(microsecond=0)
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        misfire_policy="skip",
    )
    win = queue.create_blackout(
        tenant_id="t1",
        kind="daily",
        daily_start=start,
        daily_end=end,
        timezone="UTC",
        reason="quiet hours",
    )
    assert win.kind == "daily"
    assert win.active is True
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0
    listed = queue.list_blackouts(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].id == win.id


def test_future_once_window_does_not_block(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        misfire_policy="catch_up_one",
    )
    queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() + timedelta(hours=1),
        ends_at=_utcnow() + timedelta(hours=2),
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == sched.id


def test_other_tenant_keeps_firing_during_blackout(queue: JobQueue):
    queue.create_schedule(
        tenant_id="paused",
        interval_seconds=60,
        payload={"p": 1},
        misfire_policy="skip",
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
        misfire_policy="skip",
    )
    queue.create_blackout(
        tenant_id="paused",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=30),
    )
    fires = queue.process_due_schedules()
    assert [f.schedule.id for f in fires] == [live.id]
    assert queue.count(tenant_id="paused") == 0
    assert queue.count(tenant_id="live") == 1


def test_invalid_blackout_rejected(queue: JobQueue):
    with pytest.raises(ValueError, match="ends_at"):
        queue.create_blackout(
            tenant_id="t1",
            starts_at=_utcnow() + timedelta(seconds=10),
            ends_at=_utcnow(),
        )
    with pytest.raises(ValueError, match="daily"):
        queue.create_blackout(
            tenant_id="t1",
            kind="daily",
            daily_start="09:00",
            daily_end="09:00",
        )
    with pytest.raises(ValueError, match="kind"):
        queue.create_blackout(tenant_id="t1", kind="explode")
    with pytest.raises(ScheduleError, match="not found"):
        queue.create_blackout(
            tenant_id="t1",
            schedule_id=uuid4(),
            starts_at=_utcnow(),
            ends_at=_utcnow() + timedelta(seconds=10),
        )
    with pytest.raises(BlackoutError, match="not found"):
        queue.update_blackout(uuid4(), tenant_id="t1", reason="x")
    assert queue.delete_blackout(uuid4(), tenant_id="t1") is False


def test_concurrent_overlap_updates_do_not_double_fire(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    _backdate(queue, sched.id, seconds=6)
    win = queue.create_blackout(
        tenant_id="t1",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=8),
    )
    extra = queue.create_blackout(
        tenant_id="t1",
        schedule_id=sched.id,
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(seconds=8),
    )
    stop = threading.Event()
    errors: list[BaseException] = []

    def flipper() -> None:
        try:
            while not stop.is_set():
                queue.update_blackout(
                    win.id,
                    tenant_id="t1",
                    ends_at=_utcnow() + timedelta(seconds=6),
                )
                queue.update_blackout(
                    extra.id,
                    tenant_id="t1",
                    schedule_id=sched.id,
                    ends_at=_utcnow() + timedelta(seconds=7),
                )
                time.sleep(0.01)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def ticker() -> None:
        try:
            while not stop.is_set():
                queue.process_due_schedules(tenant_id="t1")
                time.sleep(0.01)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=flipper),
        threading.Thread(target=flipper),
        threading.Thread(target=ticker),
        threading.Thread(target=ticker),
    ]
    for t in threads:
        t.start()
    time.sleep(0.4)
    stop.set()
    for t in threads:
        t.join(timeout=3)
    assert errors == []
    assert queue.count(tenant_id="t1") == 0
    queue.delete_blackout(win.id, tenant_id="t1")
    queue.delete_blackout(extra.id, tenant_id="t1", schedule_id=sched.id)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 1


def test_blackout_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        sched = client.create_schedule(
            tenant_id="http-b",
            interval_seconds=60,
            payload={"n": 1},
            name="http-sched",
        )
        now = _utcnow()
        tenant_win = client.create_blackout(
            tenant_id="http-b",
            starts_at=(now - timedelta(seconds=1)).isoformat(),
            ends_at=(now + timedelta(seconds=30)).isoformat(),
            reason="deploy",
        )
        assert tenant_win["kind"] == "once"
        assert tenant_win["active"] is True
        listed = client.list_blackouts(tenant_id="http-b")
        assert len(listed) == 1
        sched_win = client.create_blackout(
            tenant_id="http-b",
            schedule_id=sched["id"],
            kind="daily",
            daily_start="22:00",
            daily_end="06:00",
            timezone="UTC",
            reason="quiet",
        )
        assert sched_win["kind"] == "daily"
        assert sched_win["daily_start"] == "22:00:00"
        both = client.list_blackouts(tenant_id="http-b", schedule_id=sched["id"])
        assert len(both) == 2
        updated = client.update_blackout(
            tenant_win["id"],
            tenant_id="http-b",
            ends_at=(now + timedelta(seconds=5)).isoformat(),
        )
        assert updated["id"] == tenant_win["id"]
        assert client.delete_blackout(tenant_win["id"], tenant_id="http-b") is True
        assert client.delete_blackout(
            sched_win["id"], tenant_id="http-b", schedule_id=sched["id"]
        )
        assert client.list_blackouts(tenant_id="http-b") == []
    finally:
        server.shutdown()


def test_export_restore_copies_blackouts(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src-b",
        name="tick",
        payload={"from": "sched"},
        interval_seconds=60,
    )
    queue.create_blackout(
        tenant_id="src-b",
        starts_at=_utcnow() - timedelta(seconds=1),
        ends_at=_utcnow() + timedelta(minutes=10),
        reason="tenant-win",
    )
    queue.create_blackout(
        tenant_id="src-b",
        schedule_id=sched.id,
        kind="daily",
        daily_start="22:00",
        daily_end="06:00",
        reason="quiet",
    )
    snap = queue.export_tenant("src-b")
    assert snap.counts.get("schedule_blackouts", 0) == 2
    rst = queue.restore_tenant(
        "dst-b", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    dst_scheds = queue.list_schedules(tenant_id="dst-b")
    assert len(dst_scheds) == 1
    tenant_wins = queue.list_blackouts(tenant_id="dst-b")
    assert len(tenant_wins) == 1
    assert tenant_wins[0].reason == "tenant-win"
    sched_wins = queue.list_blackouts(
        tenant_id="dst-b",
        schedule_id=dst_scheds[0].id,
        include_tenant=False,
    )
    assert len(sched_wins) == 1
    assert sched_wins[0].kind == "daily"
    assert sched_wins[0].id != tenant_wins[0].id
