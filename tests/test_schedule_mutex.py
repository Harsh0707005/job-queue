"""Mutually exclusive schedule groups: one inflight tick per group."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _SCHED_MUTEX_LOCK_CLASS, _utcnow
from job_queue.queue.schedules_crud import SchedulesCrudMixin
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


def test_mutex_group_process_due_fires_one(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"who": "a"},
        name="a",
        mutex_group="deploy",
    )
    b = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"who": "b"},
        name="b",
        mutex_group="deploy",
    )
    assert a.mutex_group == "deploy"
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    winner = fires[0].schedule.id
    assert winner in {a.id, b.id}
    assert queue.count(tenant_id="t1") == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    loser = b.id if winner == a.id else a.id
    lost = queue.get_schedule(loser, tenant_id="t1")
    assert lost is not None
    assert lost.next_run_at <= _utcnow()


def test_mutex_group_concurrent_process_due_one_tick(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"who": "a"},
        name="a",
        mutex_group="g",
    )
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"who": "b"},
        name="b",
        mutex_group="g",
    )
    hits: list = []

    def worker() -> None:
        hits.extend(queue.process_due_schedules(tenant_id="t1"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    assert len(hits) == 1
    assert queue.count(tenant_id="t1") == 1
    assert len({j.schedule_id for j in queue.list_jobs(tenant_id="t1")}) == 1


def test_mutex_group_concurrent_dag_one_graph(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        name="a",
        dag_jobs=_diamond(),
        mutex_group="pipe",
    )
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 2},
        name="b",
        dag_jobs=_diamond(),
        mutex_group="pipe",
    )
    hits: list = []

    def worker() -> None:
        hits.extend(queue.process_due_schedules(tenant_id="t1"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    assert len(hits) == 1
    assert len(hits[0].jobs) == 4
    assert queue.count(tenant_id="t1") == 4
    sids = {j.schedule_id for j in hits[0].jobs}
    assert len(sids) == 1


def test_mutex_blocked_skip_does_not_dump(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "held"},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    wait = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"who": "wait"},
        name="wait",
        mutex_group="g",
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(held.id, tenant_id="t1")
    _backdate(queue, wait.id, seconds=5.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    still = queue.get_schedule(wait.id, tenant_id="t1")
    assert still is not None and still.next_run_at < _utcnow()
    assert queue.count(tenant_id="t1") == 1
    assert _finish(queue, "t1", 1) == 1
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.COMPLETED
    assert queue.process_due_schedules(tenant_id="t1") == []
    got = queue.get_schedule(wait.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 5
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 1


def test_mutex_blocked_catch_up_one_after_release(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "held"},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    wait = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"who": "wait"},
        name="wait",
        mutex_group="g",
        misfire_policy="catch_up_one",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(held.id, tenant_id="t1")
    _backdate(queue, wait.id, seconds=5.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == wait.id
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 2


def test_mutex_blocked_catch_up_drains_one_at_a_time(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "held"},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    wait = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"who": "wait"},
        name="wait",
        mutex_group="g",
        misfire_policy="catch_up",
        catch_up_max=4,
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(held.id, tenant_id="t1")
    _backdate(queue, wait.id, seconds=5.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    second = queue.process_due_schedules(tenant_id="t1")
    assert len(second) == 1
    assert {first[0].job.id, second[0].job.id} != {first[0].job.id}


def test_mutex_cron_skip_after_hold_does_not_flood(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        cron="*/2 * * * * *",
        payload={"who": "held"},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    wait = queue.create_schedule(
        tenant_id="t1",
        cron="*/2 * * * * *",
        payload={"who": "wait"},
        name="wait",
        mutex_group="g",
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(held.id, tenant_id="t1")
    _backdate(queue, wait.id, seconds=8.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert _finish(queue, "t1", 1) == 1
    assert queue.process_due_schedules(tenant_id="t1") == []
    got = queue.get_schedule(wait.id, tenant_id="t1")
    assert got is not None
    assert got.missed_ticks >= 3
    assert got.next_run_at > _utcnow()
    assert queue.count(tenant_id="t1") == 1


def test_other_group_and_other_tenant_still_fire(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"g": "blue"},
        name="blue-a",
        mutex_group="blue",
    )
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"g": "blue"},
        name="blue-b",
        mutex_group="blue",
    )
    other = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"g": "red"},
        name="red",
        mutex_group="red",
    )
    free = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"g": None},
        name="free",
    )
    peer = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=60,
        payload={"g": "blue"},
        name="peer",
        mutex_group="blue",
    )
    fires = queue.process_due_schedules()
    sids = {f.schedule.id for f in fires}
    assert other.id in sids
    assert free.id in sids
    assert peer.id in sids
    blues = [f for f in fires if f.schedule.mutex_group == "blue" and f.schedule.tenant_id == "t1"]
    assert len(blues) == 1


def test_fire_refuses_when_peer_inflight(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="a",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    b = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="b",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(a.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(b.id, tenant_id="t1")
    assert queue.count(tenant_id="t1") == 1
    assert _finish(queue, "t1", 1) == 1
    second = queue.fire_schedule(b.id, tenant_id="t1")
    assert second.job.schedule_id == b.id


def test_pause_and_delete_do_not_deadlock_group(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    idle = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="idle",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    live = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=60,
        payload={"ok": True},
        name="live",
    )
    fire = queue.fire_schedule(held.id, tenant_id="t1")
    errors: list[BaseException] = []
    done: list[str] = []

    def pause_held() -> None:
        try:
            queue.pause_schedule(held.id, tenant_id="t1")
            done.append("pause")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def delete_idle() -> None:
        try:
            assert queue.delete_schedule(idle.id, tenant_id="t1") is True
            done.append("delete")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def tick_live() -> None:
        try:
            fires = queue.process_due_schedules(tenant_id="t2")
            assert len(fires) == 1
            done.append("live")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def hold_row() -> None:
        try:
            with queue._pool.connection() as conn:
                conn.execute(
                    "SELECT id FROM schedules WHERE id = %s FOR UPDATE",
                    (idle.id,),
                )
                time.sleep(0.2)
                conn.commit()
            done.append("lock")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=fn)
        for fn in (pause_held, delete_idle, tick_live, hold_row)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    assert errors == []
    assert "pause" in done and "live" in done
    # delete_idle may wait on hold_row then succeed, or hold_row may run
    # after delete (False). Either way the tenant is not stuck.
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.get_schedule(held.id, tenant_id="t1") is not None
    assert queue.get_schedule(held.id, tenant_id="t1").enabled is False


def test_delete_waiter_does_not_block_holder(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="held",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    wait = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="wait",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(held.id, tenant_id="t1")
    assert queue.delete_schedule(wait.id, tenant_id="t1") is True
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == fire.job.id
    _ack(queue, leased, "w")
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.COMPLETED


def test_skills_and_frozen_peer_block_group(queue: JobQueue):
    parked = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="park",
        mutex_group="g",
        required_skills=["tpu"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    other = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="other",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(parked.id, tenant_id="t1")
    assert fire.job.status == JobStatus.AWAITING_SKILLS
    _make_due(queue, other.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(other.id, tenant_id="t1")

    frozen_s = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 3},
        name="held",
        mutex_group="g2",
        start_at=_utcnow() + timedelta(hours=1),
    )
    peer = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 4},
        name="peer",
        mutex_group="g2",
        start_at=_utcnow() + timedelta(hours=1),
    )
    held = queue.fire_schedule(frozen_s.id, tenant_id="t1")
    froze = queue.freeze(held.job.id, tenant_id="t1", reason="hold")
    assert froze.status == JobStatus.FROZEN
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(peer.id, tenant_id="t1")


def test_update_mutex_group_and_clear(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="a",
        start_at=_utcnow() + timedelta(hours=1),
    )
    b = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 2},
        name="b",
        mutex_group="g",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(b.id, tenant_id="t1")
    joined = queue.update_schedule(a.id, tenant_id="t1", mutex_group="g")
    assert joined.mutex_group == "g"
    with pytest.raises(ScheduleError, match="mutex"):
        queue.fire_schedule(a.id, tenant_id="t1")
    cleared = queue.update_schedule(a.id, tenant_id="t1", mutex_group=None)
    assert cleared.mutex_group is None
    extra = queue.fire_schedule(a.id, tenant_id="t1")
    assert extra.job.schedule_id == a.id


def test_invalid_mutex_group_rejected(queue: JobQueue):
    with pytest.raises(ValueError, match="mutex_group"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            mutex_group="x" * 200,
        )


def test_mutex_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        a = client.create_schedule(
            tenant_id="http-m",
            interval_seconds=3600,
            payload={"n": 1},
            name="a",
            mutex_group="blue",
        )
        b = client.create_schedule(
            tenant_id="http-m",
            interval_seconds=3600,
            payload={"n": 2},
            name="b",
            mutex_group="blue",
        )
        assert a["mutex_group"] == "blue"
        client.fire_schedule(a["id"], tenant_id="http-m")
        with pytest.raises(ScheduleError, match="mutex"):
            client.fire_schedule(b["id"], tenant_id="http-m")
        moved = client.update_schedule(
            b["id"], tenant_id="http-m", mutex_group="red"
        )
        assert moved["mutex_group"] == "red"
        client.fire_schedule(b["id"], tenant_id="http-m")
        with pytest.raises(ScheduleError, match="in-flight"):
            client.delete_schedule(b["id"], tenant_id="http-m")
        assert _finish(queue, "http-m", 2) == 2
        assert client.delete_schedule(b["id"], tenant_id="http-m") is True
        assert client.get_schedule(a["id"], tenant_id="http-m") is not None
    finally:
        server.shutdown()


def test_mutex_missing_schedule_unchanged(queue: JobQueue):
    with pytest.raises(ScheduleError, match="not found"):
        queue.update_schedule(uuid4(), tenant_id="t1", mutex_group="g")


def test_export_restore_keeps_mutex_group(queue: JobQueue):
    a = queue.create_schedule(
        tenant_id="src-mx",
        interval_seconds=60,
        payload={"who": "a"},
        name="a",
        mutex_group="deploy",
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    b = queue.create_schedule(
        tenant_id="src-mx",
        interval_seconds=60,
        payload={"who": "b"},
        name="b",
        mutex_group="deploy",
        misfire_policy="skip",
        start_at=_utcnow() + timedelta(hours=1),
    )
    snap = queue.export_tenant("src-mx")
    assert snap.status == "ready"
    rst = queue.restore_tenant(
        "dst-mx", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    dst = queue.list_schedules(tenant_id="dst-mx")
    assert len(dst) == 2
    assert {s.mutex_group for s in dst} == {"deploy"}
    assert {s.misfire_policy for s in dst} == {"skip"}
    assert {s.id for s in dst}.isdisjoint({a.id, b.id})
    for sched in dst:
        _make_due(queue, sched.id)
    fires = queue.process_due_schedules(tenant_id="dst-mx")
    assert len(fires) == 1
    assert queue.count(tenant_id="dst-mx") == 1
    assert queue.process_due_schedules(tenant_id="dst-mx") == []
    src = queue.get_schedule(a.id, tenant_id="src-mx")
    assert src is not None and src.mutex_group == "deploy"


def test_mutex_try_lock_does_not_stall_other_tenants(queue: JobQueue):
    queue.create_schedule(
        tenant_id="hold-mx",
        interval_seconds=60,
        payload={"who": "held"},
        name="held",
        mutex_group="g",
    )
    live = queue.create_schedule(
        tenant_id="live-mx",
        interval_seconds=60,
        payload={"ok": True},
        name="live",
    )
    key = SchedulesCrudMixin._mutex_lock_key("hold-mx", "g")
    held = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def locker() -> None:
        try:
            with queue._pool.connection() as conn:
                conn.execute(
                    "SELECT pg_advisory_lock(%s, hashtext(%s))",
                    (_SCHED_MUTEX_LOCK_CLASS, key),
                )
                conn.commit()
                held.set()
                release.wait(5)
                conn.execute(
                    "SELECT pg_advisory_unlock(%s, hashtext(%s))",
                    (_SCHED_MUTEX_LOCK_CLASS, key),
                )
                conn.commit()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
            held.set()

    t = threading.Thread(target=locker)
    t.start()
    assert held.wait(2)
    start = time.monotonic()
    fires = queue.process_due_schedules()
    elapsed = time.monotonic() - start
    release.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert errors == []
    assert elapsed < 0.5
    assert [f.schedule.id for f in fires] == [live.id]
    assert queue.count(tenant_id="hold-mx") == 0
    assert queue.count(tenant_id="live-mx") == 1
