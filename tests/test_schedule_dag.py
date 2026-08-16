"""Schedule ticks that materialize a whole DAG atomically."""

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


def _diamond() -> list[dict]:
    return [
        {"ref": "prep", "payload": {"step": "prep"}},
        {"ref": "left", "payload": {"step": "left"}, "depends_on": ["prep"]},
        {"ref": "right", "payload": {"step": "right"}, "depends_on": ["prep"]},
        {
            "ref": "join",
            "payload": {"step": "join"},
            "depends_on": ["left", "right"],
        },
    ]


def test_schedule_dag_tick_enqueues_closed_graph(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"kind": "pipeline"},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    assert len(sched.dag_jobs) == 4
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at = NOW() - INTERVAL '1 second' "
            "WHERE id = %s",
            (sched.id,),
        )
        conn.commit()
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    fire = fires[0]
    assert len(fire.jobs) == 4
    assert fire.refs == ("prep", "left", "right", "join")
    assert fire.batch_id is not None
    assert fire.batch_key.startswith(f"sched:{sched.id}:tick:")
    by_ref = {r: j for r, j in zip(fire.refs, fire.jobs, strict=True)}
    assert by_ref["left"].depends_on == (by_ref["prep"].id,)
    assert set(by_ref["join"].depends_on) == {
        by_ref["left"].id,
        by_ref["right"].id,
    }
    assert all(j.schedule_id == sched.id for j in fire.jobs)
    assert queue.count(tenant_id="t1") == 4
    # No second tick while the graph is live.
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_schedule_dag_tick_is_atomic_on_failure(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-dag-s", window_seconds=3600, max_enqueues=2)
    sched = queue.create_schedule(
        tenant_id="cap-dag-s",
        interval_seconds=60,
        payload={"n": 1},
        dag_jobs=_diamond(),
    )
    assert queue.process_due_schedules(tenant_id="cap-dag-s") == []
    assert queue.count(tenant_id="cap-dag-s") == 0


def test_concurrent_process_due_one_dag(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        dag_jobs=_diamond(),
    )
    hits: list = []

    def worker() -> None:
        hits.extend(queue.process_due_schedules(tenant_id="t1"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert len(hits) == 1
    assert len(hits[0].jobs) == 4
    assert queue.count(tenant_id="t1") == 4
    ids = [j.id for j in hits[0].jobs]
    assert len(set(ids)) == 4


def test_manual_fire_dag_and_overlap(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert len(fire.jobs) == 4
    assert fire.batch_key is not None
    assert "manual" in fire.batch_key
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.fire_schedule(sched.id, tenant_id="t1")
    # Finish the graph, then fire again.
    pending = list(queue.list_jobs(tenant_id="t1"))
    # Lease roots first then children.
    done = 0
    while done < 4:
        job = queue.lease(tenant_id="t1", worker_id="w")
        if job is None:
            break
        _ack(queue, job, "w")
        done += 1
    assert done == 4
    second = queue.fire_schedule(sched.id, tenant_id="t1")
    assert len(second.jobs) == 4
    assert {j.id for j in second.jobs}.isdisjoint({j.id for j in fire.jobs})


def test_other_tenant_single_job_keeps_firing(queue: JobQueue):
    queue.create_schedule(
        tenant_id="dag",
        interval_seconds=60,
        payload={"d": 1},
        dag_jobs=_diamond(),
    )
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    fires = queue.process_due_schedules()
    tenants = {f.schedule.tenant_id for f in fires}
    assert tenants == {"dag", "live"}
    dag_f = next(f for f in fires if f.schedule.tenant_id == "dag")
    live_f = next(f for f in fires if f.schedule.tenant_id == "live")
    assert len(dag_f.jobs) == 4
    assert live_f.jobs == (live_f.job,)
    assert live_f.job.schedule_id == live.id


def test_invalid_dag_rejected_at_create(queue: JobQueue):
    with pytest.raises(ValueError, match="cycle"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            dag_jobs=[
                {"ref": "a", "payload": {"n": 1}, "depends_on": ["b"]},
                {"ref": "b", "payload": {"n": 2}, "depends_on": ["a"]},
            ],
        )
    with pytest.raises(ValueError, match="unknown ref"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={},
            dag_jobs=[{"ref": "a", "payload": {}, "depends_on": ["nope"]}],
        )


def test_crash_before_commit_leaves_nothing(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    with queue._pool.connection() as conn:
        conn.execute("BEGIN")
        conn.execute(
            "SELECT * FROM schedules WHERE id = %s FOR UPDATE",
            (sched.id,),
        )
        conn.execute("ROLLBACK")
    assert queue.count(tenant_id="t1") == 0
    fire = queue.fire_schedule(sched.id, tenant_id="t1")
    assert len(fire.jobs) == 4


def test_schedule_dag_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        raw = client.create_schedule(
            tenant_id="http-dag",
            interval_seconds=3600,
            payload={"kind": "pipe"},
            name="http-dag",
            dag_jobs=_diamond(),
        )
        assert len(raw["dag_jobs"]) == 4
        fired = client.fire_schedule(raw["id"], tenant_id="http-dag")
        assert len(fired["jobs"]) == 4
        assert fired["refs"] == ["prep", "left", "right", "join"]
        assert fired["batch_key"]
        with pytest.raises(ScheduleError, match="in-flight"):
            client.fire_schedule(raw["id"], tenant_id="http-dag")
    finally:
        server.shutdown()


def test_same_tick_batch_key_is_idempotent(queue: JobQueue):
    """If a batch_key is already committed, a retry returns the same graph."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    first = queue.fire_schedule(sched.id, tenant_id="t1")
    assert first.batch_key is not None
    # Simulate a retry of the same batch_key on the open connection path.
    specs = queue._normalize_dag_specs(
        [
            {
                "ref": n["ref"],
                "payload": dict(n["payload"]),
                "depends_on": list(n.get("depends_on") or []),
            }
            for n in _diamond()
        ]
    )
    with queue._pool.connection() as conn:
        again = queue._enqueue_dag_on_conn(
            conn,
            tenant_id="t1",
            queue_name="default",
            batch_key=first.batch_key,
            specs=specs,
            now=_utcnow(),
            schedule_id=sched.id,
            bypass_pending_and_rate=True,
        )
        conn.commit()
    assert again.created is False
    assert again.batch_id == first.batch_id
    assert [j.id for j in again.jobs] == [j.id for j in first.jobs]
    assert queue.count(tenant_id="t1") == 4


def _make_due(queue: JobQueue, schedule_id) -> None:
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at = NOW() - INTERVAL '1 second' "
            "WHERE id = %s",
            (schedule_id,),
        )
        conn.commit()


def test_process_due_does_not_refire_while_awaiting_skills(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        dag_jobs=_diamond(),
        required_skills=["tpu"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    assert len(first[0].jobs) == 4
    assert all(j.status == JobStatus.AWAITING_SKILLS for j in first[0].jobs)
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 4
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.fire_schedule(sched.id, tenant_id="t1")


def test_process_due_does_not_refire_single_job_awaiting_skills(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        required_skills=["tpu"],
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    assert first[0].job.status == JobStatus.AWAITING_SKILLS
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 1


def test_process_due_does_not_refire_while_frozen(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        dag_jobs=_diamond(),
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first[0].jobs) == 4
    held = queue.freeze(first[0].jobs[0].id, tenant_id="t1", reason="hold tick")
    assert held.status == JobStatus.FROZEN
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 4
    with pytest.raises(ScheduleError, match="in-flight"):
        queue.fire_schedule(sched.id, tenant_id="t1")
    # Other tenant still fires.
    live = queue.create_schedule(
        tenant_id="live",
        interval_seconds=60,
        payload={"ok": True},
    )
    live_fires = queue.process_due_schedules(tenant_id="live")
    assert len(live_fires) == 1
    assert live_fires[0].job.schedule_id == live.id
    # After unfreeze + complete, a new tick is allowed.
    queue.unfreeze(held.id, tenant_id="t1")
    done = 0
    while done < 4:
        job = queue.lease(tenant_id="t1", worker_id="w")
        if job is None:
            break
        _ack(queue, job, "w")
        done += 1
    assert done == 4
    _make_due(queue, sched.id)
    second = queue.process_due_schedules(tenant_id="t1")
    assert len(second) == 1
    assert len(second[0].jobs) == 4
    assert {j.id for j in second[0].jobs}.isdisjoint({j.id for j in first[0].jobs})


def test_process_due_does_not_refire_single_job_frozen(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    first = queue.process_due_schedules(tenant_id="t1")
    queue.freeze(first[0].job.id, tenant_id="t1")
    _make_due(queue, sched.id)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 1
