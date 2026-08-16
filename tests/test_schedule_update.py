"""Live schedule edits: cadence and DAG template without delete."""

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


def _line() -> list[dict]:
    return [
        {"ref": "only", "payload": {"step": "only"}},
        {"ref": "after", "payload": {"step": "after"}, "depends_on": ["only"]},
    ]


def test_update_cron_jumps_to_next_future_not_backfill(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    # Overdue by many 1s ticks — changing to hourly must not catch-up flood.
    got = queue.update_schedule(
        sched.id, tenant_id="t1", cron="0 * * * *", interval_seconds=None
    )
    assert got.cron_expr == "0 * * * *"
    assert got.interval_seconds is None
    assert got.next_run_at > _utcnow()
    assert got.catch_up_remaining is None
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0


def test_update_interval_does_not_flood(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        misfire_policy="catch_up",
        catch_up_max=8,
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    got = queue.update_schedule(
        sched.id, tenant_id="t1", interval_seconds=3600
    )
    assert got.interval_seconds == 3600
    assert got.next_run_at > _utcnow()
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0


def test_update_dag_applies_on_next_tick_not_inflight(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert fire.refs == ("prep", "left", "right", "join")
    got = queue.update_schedule(sched.id, tenant_id="t1", dag_jobs=_line())
    assert [n["ref"] for n in got.dag_jobs] == ["only", "after"]
    # In-flight diamond is unchanged.
    assert queue.count(tenant_id="t1") == 4
    assert queue.process_due_schedules(tenant_id="t1") == []
    done = 0
    while done < 4:
        job = queue.lease(tenant_id="t1", worker_id="w")
        if job is None:
            break
        _ack(queue, job, "w")
        done += 1
    assert done == 4
    second = queue.fire_schedule(sched.id, tenant_id="t1")
    assert second.refs == ("only", "after")
    assert len(second.jobs) == 2
    assert queue.count(tenant_id="t1") == 6


def test_update_clears_dag_to_single_job(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    got = queue.update_schedule(sched.id, tenant_id="t1", dag_jobs=[])
    assert got.dag_jobs == ()
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert fire.refs == ()
    assert fire.jobs == (fire.job,)
    assert fire.job.payload["n"] == 1


def test_concurrent_update_and_process_due_no_mixed_graph(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    errors: list[BaseException] = []
    fires: list = []

    def ticker() -> None:
        try:
            fires.extend(queue.process_due_schedules(tenant_id="t1"))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def editor() -> None:
        try:
            queue.update_schedule(sched.id, tenant_id="t1", dag_jobs=_line())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=ticker),
        threading.Thread(target=ticker),
        threading.Thread(target=editor),
        threading.Thread(target=editor),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert errors == []
    if fires:
        assert len(fires) == 1
        refs = fires[0].refs
        assert refs in (("prep", "left", "right", "join"), ("only", "after"))
        assert queue.count(tenant_id="t1") == len(refs)
    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert [n["ref"] for n in got.dag_jobs] in (
        ["prep", "left", "right", "join"],
        ["only", "after"],
    )


def test_update_rejected_and_missing(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        cron="0 * * * *",
    )
    before = queue.get_schedule(sched.id, tenant_id="t1")
    with pytest.raises(ValueError, match="cron and/or interval"):
        queue.update_schedule(
            sched.id, tenant_id="t1", cron=None, interval_seconds=None
        )
    with pytest.raises(ValueError, match="cycle"):
        queue.update_schedule(
            sched.id,
            tenant_id="t1",
            dag_jobs=[
                {"ref": "a", "payload": {}, "depends_on": ["b"]},
                {"ref": "b", "payload": {}, "depends_on": ["a"]},
            ],
        )
    after = queue.get_schedule(sched.id, tenant_id="t1")
    assert after is not None and before is not None
    assert after.cron_expr == before.cron_expr
    assert after.dag_jobs == before.dag_jobs
    with pytest.raises(ScheduleError, match="not found"):
        queue.update_schedule(uuid4(), tenant_id="t1", interval_seconds=30)
    with pytest.raises(ValueError, match="at least one field"):
        queue.update_schedule(sched.id, tenant_id="t1")


def test_other_tenant_keeps_firing_during_update(queue: JobQueue):
    queue.create_schedule(
        tenant_id="edit",
        interval_seconds=3600,
        payload={"e": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    edited = queue.list_schedules(tenant_id="edit")[0]
    queue.update_schedule(
        edited.id, tenant_id="edit", cron="0 0 1 1 *", interval_seconds=None
    )
    fires = queue.process_due_schedules()
    assert [f.schedule.id for f in fires] == [live.id]


def test_update_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-u",
            interval_seconds=60,
            payload={"n": 1},
            name="http-upd",
            dag_jobs=_diamond(),
        )
        updated = client.update_schedule(
            raw["id"],
            tenant_id="http-u",
            cron="*/30 * * * * *",
            interval_seconds=None,
            dag_jobs=_line(),
            payload={"n": 2},
        )
        assert updated["cron_expr"] == "*/30 * * * * *"
        assert [n["ref"] for n in updated["dag_jobs"]] == ["only", "after"]
        assert updated["payload"]["n"] == 2
        with pytest.raises(ScheduleError, match="not found"):
            client.update_schedule(uuid4(), tenant_id="http-u", interval_seconds=10)
    finally:
        server.shutdown()
