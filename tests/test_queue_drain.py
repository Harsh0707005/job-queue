"""Per-tenant per-queue drain: stop intake, finish existing, isolate queues."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, QueueDrainError, ReplayError, SpawnError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _hb, _nack


def test_drain_blocks_new_enqueue_other_queue_works(queue: JobQueue):
    waiting = queue.enqueue(
        tenant_id="t1", queue_name="email", payload={"old": True}
    )
    drained = queue.drain_tenant_queue("t1", "email")
    assert drained == ["email"]
    assert queue.is_queue_draining("t1", "email") is True
    assert queue.is_queue_draining("t1", "sms") is False
    assert queue.is_tenant_paused("t1") is False

    with pytest.raises(QueueDrainError, match="draining"):
        queue.enqueue(tenant_id="t1", queue_name="email", payload={"new": True})
    sms = queue.enqueue(tenant_id="t1", queue_name="sms", payload={"ok": True})
    other = queue.enqueue(tenant_id="t2", queue_name="email", payload={"peer": True})

    first = queue.lease(tenant_id="t1", worker_id="w", queue_name="email")
    assert first is not None and first.id == waiting.id
    _ack(queue, first, "w")
    got_sms = queue.lease(tenant_id="t1", worker_id="w", queue_name="sms")
    assert got_sms is not None and got_sms.id == sms.id
    _ack(queue, got_sms, "w")
    got_other = queue.lease(tenant_id="t2", worker_id="w", queue_name="email")
    assert got_other is not None and got_other.id == other.id
    _ack(queue, got_other, "w")


def test_drain_allows_in_flight_and_fair_lease(queue: JobQueue):
    live = queue.enqueue(tenant_id="t1", queue_name="email", payload={"live": True})
    leased = queue.lease(tenant_id="t1", worker_id="holder", queue_name="email")
    assert leased is not None
    pending = queue.enqueue(tenant_id="t1", queue_name="email", payload={"pend": True})
    queue.drain_tenant_queue("t1", "email")

    extended = _hb(queue, leased, "holder", extend_by_seconds=30)
    assert extended.lease_expires_at is not None
    done = _ack(queue, leased, "holder", result={"ok": True})
    assert done.status == JobStatus.COMPLETED
    assert queue.get_result(live.id, tenant_id="t1") == {"ok": True}

    leftover = queue.lease(worker_id="fair", queue_name="email")
    assert leftover is not None and leftover.id == pending.id
    _ack(queue, leftover, "fair")


def test_undrain_restores_enqueue(queue: JobQueue):
    queue.drain_tenant_queue("t1", "email")
    with pytest.raises(QueueDrainError):
        queue.enqueue(tenant_id="t1", queue_name="email", payload={"x": 1})
    assert queue.undrain_tenant_queue("t1", "email") == []
    assert queue.is_queue_draining("t1", "email") is False
    job = queue.enqueue(tenant_id="t1", queue_name="email", payload={"x": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w", queue_name="email")
    assert leased is not None and leased.id == job.id
    _ack(queue, leased, "w")


def test_drain_skips_schedule_for_that_queue_only(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        queue_name="email",
        interval_seconds=60,
        payload={"q": "email"},
        name="email-tick",
    )
    queue.create_schedule(
        tenant_id="t1",
        queue_name="sms",
        interval_seconds=60,
        payload={"q": "sms"},
        name="sms-tick",
    )
    queue.drain_tenant_queue("t1", "email")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].job.queue_name == "sms"
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    assert queue.list_draining_queues("t1") == ["email"]


def test_undrain_aligns_overdue_schedule_once(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        queue_name="email",
        interval_seconds=60,
        payload={"q": "email"},
        name="email-tick",
    )
    queue.drain_tenant_queue("t1", "email")
    assert queue.process_due_schedules(tenant_id="t1") == []
    queue.undrain_tenant_queue("t1", "email")
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].schedule.id == sched.id
    assert fires[0].job.queue_name == "email"
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_drain_blocks_dag_and_spawn_and_replay(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", queue_name="email", payload={"p": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w", queue_name="email")
    assert leased is not None
    dead = queue.enqueue(
        tenant_id="t1",
        queue_name="email",
        payload={"d": 1},
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    # Finish the extra job as dead so replay can be tested after drain.
    extra = queue.lease(tenant_id="t1", worker_id="w2", queue_name="email")
    assert extra is not None and extra.id == dead.id
    from tests.helpers_queue import _nack

    _nack(queue, extra, "w2", error="boom", requeue=True)
    assert queue.get(dead.id, tenant_id="t1").status == JobStatus.DEAD

    queue.drain_tenant_queue("t1", "email")
    with pytest.raises(QueueDrainError):
        queue.enqueue_dag(
            tenant_id="t1",
            queue_name="email",
            batch_key="b1",
            jobs=[{"ref": "a", "payload": {"n": 1}}],
        )
    with pytest.raises(SpawnError, match="draining"):
        queue.spawn_children(
            parent.id,
            tenant_id="t1",
            worker_id="w",
            lease_token=leased.lease_token,
            children=[{"payload": {"c": 1}}],
        )
    with pytest.raises(ReplayError, match="draining"):
        queue.replay_dead_letter(dead.id, tenant_id="t1")
    _ack(queue, leased, "w")


def test_drain_dedupe_existing_still_returns(queue: JobQueue):
    first = queue.enqueue(
        tenant_id="t1",
        queue_name="email",
        payload={"n": 1},
        dedupe_key="same",
    )
    queue.drain_tenant_queue("t1", "email")
    again = queue.enqueue(
        tenant_id="t1",
        queue_name="email",
        payload={"n": 2},
        dedupe_key="same",
    )
    assert again.id == first.id
    with pytest.raises(QueueDrainError):
        queue.enqueue(
            tenant_id="t1",
            queue_name="email",
            payload={"n": 3},
            dedupe_key="other",
        )


def test_drain_validation_and_stats(queue: JobQueue):
    with pytest.raises(ValueError, match="tenant_id"):
        queue.drain_tenant_queue("", "email")
    with pytest.raises(ValueError, match="queue_name"):
        queue.drain_tenant_queue("t1", "  ")
    queue.drain_tenant_queue("t1", "email")
    queue.drain_tenant_queue("t1", "sms")
    assert queue.list_draining_queues("t1") == ["email", "sms"]
    stats = queue.get_tenant_stats("t1")
    assert stats.paused is False
    assert tuple(stats.draining_queues) == ("email", "sms")
    queue.undrain_tenant_queue("t1", "email")
    assert queue.list_draining_queues("t1") == ["sms"]


def test_drain_export_restore(queue: JobQueue):
    queue.drain_tenant_queue("src-d", "email")
    queue.enqueue(tenant_id="src-d", queue_name="sms", payload={"n": 1})
    snap = queue.export_tenant("src-d")
    rst = queue.restore_tenant(
        "dst-d", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    assert queue.list_draining_queues("dst-d") == ["email"]
    with pytest.raises(QueueDrainError):
        queue.enqueue(tenant_id="dst-d", queue_name="email", payload={"x": 1})
    sms = queue.enqueue(tenant_id="dst-d", queue_name="sms", payload={"ok": True})
    assert sms.queue_name == "sms"


def test_drain_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{server.server_address[1]}")
        old = client.enqueue(
            tenant_id="http-d", queue_name="email", payload={"old": True}
        )
        drained = client.drain_tenant_queue("http-d", "email")
        assert drained["draining"] is True
        assert drained["draining_queues"] == ["email"]
        listed = client.list_draining_queues("http-d")
        assert listed["draining_queues"] == ["email"]
        with pytest.raises(QueueDrainError):
            client.enqueue(
                tenant_id="http-d", queue_name="email", payload={"new": True}
            )
        sms = client.enqueue(
            tenant_id="http-d", queue_name="sms", payload={"ok": True}
        )
        first = client.lease(
            tenant_id="http-d", worker_id="w", queue_name="email"
        )
        assert first is not None and first.id == old.id
        assert first.lease_token is not None
        client.ack(
            first.id,
            tenant_id="http-d",
            worker_id="w",
            lease_token=first.lease_token,
        )
        other = client.lease(tenant_id="http-d", worker_id="w", queue_name="sms")
        assert other is not None and other.id == sms.id
        client.undrain_tenant_queue("http-d", "email")
        again = client.enqueue(
            tenant_id="http-d", queue_name="email", payload={"after": True}
        )
        assert again.queue_name == "email"
    finally:
        server.shutdown()


def _wf_steps(*, fail_last: bool = False) -> list[dict]:
    steps = [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        {
            "name": "charge",
            "kind": "effect",
            "payload": {"op": "charge"},
            "compensate": {"kind": "effect", "payload": {"op": "refund"}},
        },
    ]
    if fail_last:
        steps[-1]["fail"] = True
        steps[-1]["fail_error"] = "forced"
    return steps


def test_drain_blocks_start_workflow_no_run_no_jobs(queue: JobQueue):
    queue.drain_tenant_queue("wf-d", "email")
    with pytest.raises(QueueDrainError, match="draining"):
        queue.start_workflow(
            tenant_id="wf-d",
            queue_name="email",
            max_attempts=1,
            steps=_wf_steps(),
        )
    assert queue.count(tenant_id="wf-d") == 0
    with queue._pool.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*)::int AS n FROM workflow_runs WHERE tenant_id = %s",
            ("wf-d",),
        ).fetchone()["n"]
    assert n == 0


def test_drain_workflow_other_queue_and_tenant_still_start(queue: JobQueue):
    queue.drain_tenant_queue("wf-d", "email")
    ok = queue.start_workflow(
        tenant_id="wf-d",
        queue_name="sms",
        max_attempts=1,
        steps=_wf_steps(),
    )
    assert ok.status == WorkflowRunStatus.RUNNING.value
    assert queue.count(tenant_id="wf-d", queue_name="sms") == 1
    assert queue.count(tenant_id="wf-d", queue_name="email") == 0
    peer = queue.start_workflow(
        tenant_id="wf-other",
        queue_name="email",
        max_attempts=1,
        steps=_wf_steps(),
    )
    assert peer.status == WorkflowRunStatus.RUNNING.value
    assert queue.count(tenant_id="wf-other", queue_name="email") == 1


def test_drain_blocks_next_workflow_step_but_finished_step_acks(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf-mid",
        queue_name="email",
        max_attempts=1,
        steps=_wf_steps(),
    )
    assert queue.count(tenant_id="wf-mid", queue_name="email") == 1
    queue.drain_tenant_queue("wf-mid", "email")
    first = queue.lease(tenant_id="wf-mid", worker_id="w", queue_name="email")
    assert first is not None
    _ack(queue, first, "w", result={"ok": True})
    # Step 1 finished; drain must not insert step 2.
    assert queue.count(tenant_id="wf-mid", queue_name="email", status=JobStatus.PENDING) == 0
    assert queue.count(tenant_id="wf-mid", queue_name="email", status=JobStatus.LEASED) == 0
    still = queue.get_workflow(run.id, tenant_id="wf-mid")
    assert still is not None
    assert still.status == WorkflowRunStatus.RUNNING.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-mid")
    forwards = [s for s in steps if s.direction == "forward"]
    assert len(forwards) == 1
    assert forwards[0].status == WorkflowStepStatus.COMPLETED.value
    # Redrive while still draining must not sneak a job in.
    assert queue.process_workflow_compensation(tenant_id="wf-mid", limit=20) == 0
    assert queue.count(tenant_id="wf-mid", queue_name="email", status=JobStatus.PENDING) == 0


def test_undrain_redrives_blocked_workflow_next_step(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf-go",
        queue_name="email",
        max_attempts=1,
        steps=_wf_steps(),
    )
    queue.drain_tenant_queue("wf-go", "email")
    first = queue.lease(tenant_id="wf-go", worker_id="w", queue_name="email")
    assert first is not None
    _ack(queue, first, "w", result={"ok": True})
    assert queue.count(tenant_id="wf-go", queue_name="email", status=JobStatus.PENDING) == 0
    queue.undrain_tenant_queue("wf-go", "email")
    n = queue.process_workflow_compensation(tenant_id="wf-go", limit=20)
    assert n >= 1
    assert queue.count(tenant_id="wf-go", queue_name="email", status=JobStatus.PENDING) == 1
    second = queue.lease(tenant_id="wf-go", worker_id="w", queue_name="email")
    assert second is not None
    assert second.payload.get("step_name") == "charge"
    _ack(queue, second, "w", result={"ok": True})
    done = queue.get_workflow(run.id, tenant_id="wf-go")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_drain_allows_workflow_compensate_cleanup(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf-c",
        queue_name="email",
        max_attempts=1,
        steps=_wf_steps(fail_last=True),
    )
    first = queue.lease(tenant_id="wf-c", worker_id="w", queue_name="email")
    assert first is not None
    _ack(queue, first, "w", result={"ok": True})
    queue.drain_tenant_queue("wf-c", "email")
    boom = queue.lease(tenant_id="wf-c", worker_id="w", queue_name="email")
    assert boom is not None
    _nack(queue, boom, "w", error="forced", requeue=True)
    # Compensate for the completed first step is cleanup, not intake.
    pending = queue.count(
        tenant_id="wf-c", queue_name="email", status=JobStatus.PENDING
    )
    assert pending >= 1
    undo = queue.lease(tenant_id="wf-c", worker_id="w", queue_name="email")
    assert undo is not None
    assert undo.payload.get("direction") == "compensate"
    _ack(queue, undo, "w", result={"undone": True})
    done = queue.get_workflow(run.id, tenant_id="wf-c")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value


def test_drain_workflow_http_start_rejected(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{server.server_address[1]}")
        client.drain_tenant_queue("http-wf", "email")
        with pytest.raises(QueueDrainError):
            client.start_workflow(
                tenant_id="http-wf",
                queue_name="email",
                max_attempts=1,
                steps=_wf_steps(),
            )
        sms = client.start_workflow(
            tenant_id="http-wf",
            queue_name="sms",
            max_attempts=1,
            steps=_wf_steps(),
        )
        assert sms["run"]["status"] == WorkflowRunStatus.RUNNING.value
        client.undrain_tenant_queue("http-wf", "email")
        again = client.start_workflow(
            tenant_id="http-wf",
            queue_name="email",
            max_attempts=1,
            steps=_wf_steps(),
        )
        assert again["run"]["status"] == WorkflowRunStatus.RUNNING.value
    finally:
        server.shutdown()
