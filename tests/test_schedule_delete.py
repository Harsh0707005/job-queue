"""Delete a schedule without orphaning live ticks or double-firing."""

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


def _finish(queue: JobQueue, tenant_id: str, n: int, worker: str = "w") -> int:
    done = 0
    while done < n:
        job = queue.lease(tenant_id=tenant_id, worker_id=worker)
        if job is None:
            break
        _ack(queue, job, worker)
        done += 1
    return done


def test_delete_refuses_pending_tick(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert fire.job.status == JobStatus.PENDING
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(sched.id, tenant_id="t1")
    assert queue.get_schedule(sched.id, tenant_id="t1") is not None
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.PENDING


def test_delete_refuses_leased_then_succeeds_after_ack(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == fire.job.id
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(sched.id, tenant_id="t1")
    _ack(queue, leased, "w")
    assert queue.delete_schedule(sched.id, tenant_id="t1") is True
    done = queue.get(fire.job.id, tenant_id="t1")
    assert done.status == JobStatus.COMPLETED
    assert done.schedule_id == sched.id
    assert queue.get_schedule(sched.id, tenant_id="t1") is None
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_delete_refuses_dag_and_parked_states(queue: JobQueue):
    dag = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    first = queue.fire_schedule(dag.id, tenant_id="t1")
    assert len(first.jobs) == 4
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(dag.id, tenant_id="t1")

    skills = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        required_skills=["tpu"],
        name="skills",
        start_at=_utcnow() + timedelta(hours=1),
    )
    parked = queue.fire_schedule(skills.id, tenant_id="t1")
    assert parked.job.status == JobStatus.AWAITING_SKILLS
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(skills.id, tenant_id="t1")

    frozen_s = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        name="frozen",
        start_at=_utcnow() + timedelta(hours=1),
    )
    held = queue.fire_schedule(frozen_s.id, tenant_id="t1")
    froze = queue.freeze(held.job.id, tenant_id="t1", reason="hold")
    assert froze.status == JobStatus.FROZEN
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.delete_schedule(frozen_s.id, tenant_id="t1")

    assert _finish(queue, "t1", 4) == 4
    assert queue.delete_schedule(dag.id, tenant_id="t1") is True
    for job in first.jobs:
        got = queue.get(job.id, tenant_id="t1")
        assert got.status == JobStatus.COMPLETED
        assert got.schedule_id == dag.id


def test_delete_one_schedule_sibling_keeps_firing(queue: JobQueue):
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
    assert queue.delete_schedule(held.id, tenant_id="t1") is True
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == live.id
    assert queue.count(tenant_id="t1") == 1
    assert queue.get_schedule(held.id, tenant_id="t1") is None


def test_delete_does_not_stop_other_tenant(queue: JobQueue):
    doomed = queue.create_schedule(
        tenant_id="doomed",
        interval_seconds=60,
        payload={"x": 1},
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    assert queue.delete_schedule(doomed.id, tenant_id="doomed") is True
    fires = queue.process_due_schedules()
    assert [f.schedule.id for f in fires] == [live.id]


def test_delete_wait_drains_then_removes(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            job = None
            for _ in range(40):
                job = queue.lease(tenant_id="t1", worker_id="w")
                if job is not None:
                    break
            assert job is not None
            _ack(queue, job, "w")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    assert queue.delete_schedule(
        sched.id, tenant_id="t1", wait=True, wait_timeout_seconds=5
    )
    t.join(timeout=5)
    assert not t.is_alive()
    assert errors == []
    assert queue.get_schedule(sched.id, tenant_id="t1") is None
    done = queue.get(fire.job.id, tenant_id="t1")
    assert done.status == JobStatus.COMPLETED
    assert done.schedule_id == sched.id


def test_delete_wait_timeout_disables_without_dropping(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    with pytest.raises(ScheduleError, match="after waiting"):
        queue.delete_schedule(
            sched.id, tenant_id="t1", wait=True, wait_timeout_seconds=0.15
        )
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.enabled is False
    assert queue.get(fire.job.id, tenant_id="t1").status == JobStatus.PENDING
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 1


def test_delete_wait_dag_finishes_closed(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    first = queue.fire_schedule(sched.id, tenant_id="t1")
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            assert _finish(queue, "t1", 4) == 4
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    assert queue.delete_schedule(
        sched.id, tenant_id="t1", wait=True, wait_timeout_seconds=5
    )
    t.join(timeout=5)
    assert not t.is_alive()
    assert errors == []
    assert queue.get_schedule(sched.id, tenant_id="t1") is None
    for job in first.jobs:
        got = queue.get(job.id, tenant_id="t1")
        assert got.status == JobStatus.COMPLETED
        assert got.schedule_id == sched.id
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_delete_missing_and_wrong_tenant(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={},
    )
    assert queue.delete_schedule(uuid4(), tenant_id="t1") is False
    assert queue.delete_schedule(sched.id, tenant_id="other") is False
    assert queue.get_schedule(sched.id, tenant_id="t1") is not None
    assert queue.delete_schedule(sched.id, tenant_id="t1") is True
    assert queue.delete_schedule(sched.id, tenant_id="t1") is False


def test_delete_cascades_blackouts_keeps_job_history(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert _finish(queue, "t1", 1) == 1
    start = _utcnow()
    queue.create_blackout(
        tenant_id="t1",
        schedule_id=sched.id,
        kind="once",
        starts_at=start,
        ends_at=start + timedelta(hours=1),
        reason="deploy",
    )
    assert queue.list_blackouts(
        tenant_id="t1", schedule_id=sched.id, include_tenant=False
    )
    assert queue.delete_schedule(sched.id, tenant_id="t1") is True
    assert (
        queue.list_blackouts(
            tenant_id="t1", schedule_id=sched.id, include_tenant=False
        )
        == []
    )
    done = queue.get(fire.job.id, tenant_id="t1")
    assert done.schedule_id == sched.id
    assert done.status == JobStatus.COMPLETED


def test_concurrent_delete_and_process_due(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
    )
    fires: list = []
    deleted: list[bool | str] = []

    def ticker() -> None:
        fires.extend(queue.process_due_schedules(tenant_id="t1"))

    def dropper() -> None:
        try:
            deleted.append(queue.delete_schedule(sched.id, tenant_id="t1"))
        except ScheduleError:
            deleted.append("inflight")

    threads = [threading.Thread(target=ticker) for _ in range(4)] + [
        threading.Thread(target=dropper) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    true_deletes = sum(1 for d in deleted if d is True)
    assert true_deletes <= 1
    if fires:
        assert len(fires) == 1
        assert len(fires[0].jobs) == 1
        assert "inflight" in deleted
        assert queue.get_schedule(sched.id, tenant_id="t1") is not None
        assert queue.count(tenant_id="t1") == 1
    else:
        assert true_deletes == 1
        assert queue.get_schedule(sched.id, tenant_id="t1") is None
        assert queue.count(tenant_id="t1") == 0


def test_delete_http_conflict_then_ok(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-d",
            interval_seconds=3600,
            payload={"n": 1},
            name="http-del",
        )
        fire = client.fire_schedule(raw["id"], tenant_id="http-d")
        with pytest.raises(ScheduleError, match="in-flight"):
            client.delete_schedule(raw["id"], tenant_id="http-d")
        assert client.get_schedule(raw["id"], tenant_id="http-d") is not None
        job = queue.lease(tenant_id="http-d", worker_id="w")
        assert job is not None and str(job.id) == fire["job"]["id"]
        _ack(queue, job, "w")
        assert client.delete_schedule(raw["id"], tenant_id="http-d") is True
        assert client.get_schedule(raw["id"], tenant_id="http-d") is None
        assert client.delete_schedule(raw["id"], tenant_id="http-d") is False
    finally:
        server.shutdown()


def test_delete_http_wait_timeout_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-w",
            interval_seconds=3600,
            payload={"n": 1},
            name="http-wait",
        )
        client.fire_schedule(raw["id"], tenant_id="http-w")
        with pytest.raises(ScheduleError, match="after waiting"):
            client.delete_schedule(
                raw["id"],
                tenant_id="http-w",
                wait=True,
                wait_timeout_seconds=0.2,
            )
        got = client.get_schedule(raw["id"], tenant_id="http-w")
        assert got is not None
        assert got["enabled"] is False
    finally:
        server.shutdown()
