"""Move a live schedule to another tenant without delete/recreate."""

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


def test_move_idle_schedule_reseals_and_ticks_on_dest(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"secret": "keep-me"},
        name="nightly",
        start_at=_utcnow() + timedelta(hours=1),
    )
    moved = queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    assert moved.id == sched.id
    assert moved.payload == {"secret": "keep-me"}
    assert queue.get_schedule(sched.id, tenant_id="src") is None
    got = queue.get_schedule(sched.id, tenant_id="dst")
    assert got is not None and got.payload["secret"] == "keep-me"
    _make_due(queue, sched.id)
    fires = queue.process_due_schedules(tenant_id="dst")
    assert len(fires) == 1
    assert fires[0].job.tenant_id == "dst"
    assert fires[0].job.payload == {"secret": "keep-me"}
    assert queue.process_due_schedules(tenant_id="src") == []


def test_move_inflight_dag_stays_closed_and_decrypts(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"kind": "pipe"},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="src")
    assert len(fire.jobs) == 4
    queue.freeze(fire.jobs[1].id, tenant_id="src", reason="hold")
    moved = queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    dest_jobs = queue.list_jobs(tenant_id="dst")
    assert len(dest_jobs) == 4
    assert {j.schedule_id for j in dest_jobs} == {sched.id}
    assert all(j.tenant_id == "dst" for j in dest_jobs)
    left = next(j for j in dest_jobs if j.payload.get("step") == "left")
    right = next(j for j in dest_jobs if j.payload.get("step") == "right")
    prep = next(j for j in dest_jobs if j.payload.get("step") == "prep")
    join = next(j for j in dest_jobs if j.payload.get("step") == "join")
    assert left.depends_on == (prep.id,)
    assert right.depends_on == (prep.id,)
    assert set(join.depends_on) == {left.id, right.id}
    assert queue.list_jobs(tenant_id="src") == []
    assert queue.process_due_schedules(tenant_id="dst") == []
    assert queue.process_due_schedules(tenant_id="src") == []
    with pytest.raises(ScheduleError, match="in-flight|mutex"):
        queue.fire_schedule(sched.id, tenant_id="dst")
    queue.unfreeze(fire.jobs[1].id, tenant_id="dst")
    assert _finish(queue, "dst", 4) == 4
    for job in dest_jobs:
        done = queue.get(job.id, tenant_id="dst")
        assert done.status == JobStatus.COMPLETED
        assert done.schedule_id == sched.id
    second = queue.fire_schedule(sched.id, tenant_id="dst")
    assert len(second.jobs) == 4
    assert {j.id for j in second.jobs}.isdisjoint({j.id for j in dest_jobs})


def test_move_reseals_completed_result_on_dest(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"secret": "sched"},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="src")
    job = queue.lease(tenant_id="src", worker_id="w")
    assert job is not None
    _ack(queue, job, "w", result={"out": "secret", "n": 3})
    queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
    assert queue.get(fire.job.id, tenant_id="src") is None
    done = queue.get(fire.job.id, tenant_id="dst")
    assert done is not None
    assert done.result == {"out": "secret", "n": 3}
    assert queue.get_result(fire.job.id, tenant_id="dst") == {"out": "secret", "n": 3}


def test_move_mutex_stays_exclusive_on_dest_and_frees_source(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="dst",
        interval_seconds=3600,
        payload={"who": "held"},
        name="held",
        mutex_group="blue",
        start_at=_utcnow() + timedelta(hours=1),
    )
    queue.fire_schedule(held.id, tenant_id="dst")
    incoming = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "in"},
        name="in",
        mutex_group="blue",
        start_at=_utcnow() + timedelta(hours=1),
    )
    sib = queue.create_schedule(
        tenant_id="src",
        interval_seconds=60,
        payload={"who": "sib"},
        name="sib",
        mutex_group="blue",
    )
    with pytest.raises(ScheduleError, match="mutex group"):
        queue.move_schedule(incoming.id, tenant_id="src", to_tenant_id="dst")
    assert queue.get_schedule(incoming.id, tenant_id="src") is not None

    idle = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "idle"},
        name="idle",
        mutex_group="red",
        start_at=_utcnow() + timedelta(hours=1),
    )
    moved = queue.move_schedule(idle.id, tenant_id="src", to_tenant_id="dst")
    assert moved.mutex_group == "red"
    assert moved.tenant_id == "dst"
    fires = queue.process_due_schedules(tenant_id="src")
    assert [f.schedule.id for f in fires] == [sib.id]
    assert queue.count(tenant_id="src") == 1


def test_move_refuses_foreign_dependent_and_does_not_decrypt_on_dest(
    queue: JobQueue,
):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"secret": "tick"},
        name="tick",
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="src")
    extra = queue.enqueue(
        tenant_id="src",
        payload={"secret": "foreign", "role": "hook"},
        depends_on=fire.job.id,
    )
    with pytest.raises(ScheduleError, match="still depends"):
        queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
    assert queue.get_schedule(sched.id, tenant_id="src") is not None
    assert queue.get_schedule(sched.id, tenant_id="dst") is None
    assert queue.get(fire.job.id, tenant_id="src") is not None
    assert queue.get(extra.id, tenant_id="src") is not None
    assert extra.tenant_id == "src"
    assert queue.get(extra.id, tenant_id="dst") is None
    assert queue.get(fire.job.id, tenant_id="dst") is None


def test_move_refuses_when_it_would_drag_another_schedule_tick(queue: JobQueue):
    held = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"who": "held"},
        name="held",
        mutex_group="blue",
        start_at=_utcnow() + timedelta(hours=1),
    )
    sib = queue.create_schedule(
        tenant_id="src",
        interval_seconds=60,
        payload={"who": "sib"},
        name="sib",
        mutex_group="blue",
    )
    fire = queue.fire_schedule(held.id, tenant_id="src")
    hooked = queue.enqueue(
        tenant_id="src",
        payload={"who": "other-tick"},
        depends_on=fire.job.id,
    )
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE jobs SET schedule_id = %s WHERE id = %s",
            (sib.id, hooked.id),
        )
        conn.commit()
    with pytest.raises(ScheduleError, match="still depends"):
        queue.move_schedule(held.id, tenant_id="src", to_tenant_id="dst")
    assert queue.get(fire.job.id, tenant_id="src") is not None
    assert queue.get(hooked.id, tenant_id="src") is not None
    assert queue.get(hooked.id, tenant_id="dst") is None
    assert queue.process_due_schedules(tenant_id="src") == []
    assert queue.count(tenant_id="src") == 2
    assert queue.get_schedule(held.id, tenant_id="src") is not None


def test_move_takes_spawn_children_of_the_tick(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="src")
    leased = queue.lease(tenant_id="src", worker_id="w")
    assert leased is not None and leased.id == fire.job.id
    assert leased.lease_token is not None
    spawned = queue.spawn_children(
        leased.id,
        tenant_id="src",
        worker_id="w",
        lease_token=leased.lease_token,
        children=[{"payload": {"role": "child", "secret": "kid"}}],
    )
    assert spawned.parent.status == JobStatus.AWAITING_CHILDREN
    parent = queue.get(leased.id, tenant_id="src")
    assert parent is not None
    assert parent.status == JobStatus.AWAITING_CHILDREN
    kids = [j for j in queue.list_jobs(tenant_id="src") if j.id != parent.id]
    assert len(kids) == 1
    moved = queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
    assert moved.tenant_id == "dst"
    dest = queue.list_jobs(tenant_id="dst")
    assert {j.id for j in dest} == {parent.id, kids[0].id}
    child = queue.get(kids[0].id, tenant_id="dst")
    assert child is not None
    assert child.payload == {"role": "child", "secret": "kid"}
    assert child.parent_job_id == parent.id
    assert queue.list_jobs(tenant_id="src") == []


def test_move_same_tenant_is_noop(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1", interval_seconds=60, payload={"n": 1}
    )
    again = queue.move_schedule(sched.id, tenant_id="t1", to_tenant_id="t1")
    assert again.id == sched.id
    assert again.tenant_id == "t1"


def test_move_missing_and_name_conflict(queue: JobQueue):
    with pytest.raises(ScheduleError, match="not found"):
        queue.move_schedule(uuid4(), tenant_id="src", to_tenant_id="dst")
    queue.create_schedule(
        tenant_id="dst",
        interval_seconds=60,
        payload={},
        name="dup",
    )
    src = queue.create_schedule(
        tenant_id="src",
        interval_seconds=60,
        payload={},
        name="dup",
    )
    with pytest.raises(ScheduleError, match="already exists"):
        queue.move_schedule(src.id, tenant_id="src", to_tenant_id="dst")
    assert queue.get_schedule(src.id, tenant_id="src") is not None


def test_concurrent_process_due_and_move_one_tick(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="src",
        interval_seconds=60,
        payload={"n": 1},
    )
    hits: list = []
    errors: list[BaseException] = []

    def ticker() -> None:
        try:
            hits.extend(queue.process_due_schedules())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def mover() -> None:
        try:
            queue.move_schedule(sched.id, tenant_id="src", to_tenant_id="dst")
        except ScheduleError:
            # Lost the race to a completed move or missing row.
            pass
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=ticker) for _ in range(4)] + [
        threading.Thread(target=mover) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    assert errors == []
    dest = queue.get_schedule(sched.id, tenant_id="dst")
    src = queue.get_schedule(sched.id, tenant_id="src")
    assert dest is not None
    assert src is None
    assert queue.count(tenant_id="src") == 0
    assert queue.count(tenant_id="dst") <= 1
    if queue.count(tenant_id="dst") == 1:
        assert queue.process_due_schedules(tenant_id="dst") == []
    assert queue.process_due_schedules(tenant_id="src") == []


def test_move_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-src",
            interval_seconds=3600,
            payload={"secret": "via-http"},
            name="http-mv",
        )
        moved = client.move_schedule(
            raw["id"], tenant_id="http-src", to_tenant_id="http-dst"
        )
        assert moved["tenant_id"] == "http-dst"
        assert moved["payload"]["secret"] == "via-http"
        assert client.get_schedule(raw["id"], tenant_id="http-src") is None
        got = client.get_schedule(raw["id"], tenant_id="http-dst")
        assert got is not None and got["id"] == raw["id"]
        with pytest.raises(ScheduleError, match="not found"):
            client.move_schedule(
                raw["id"], tenant_id="http-src", to_tenant_id="http-dst"
            )
    finally:
        server.shutdown()
