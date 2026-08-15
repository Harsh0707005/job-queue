"""Move a live job to another queue in place (same tenant, same id)."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, QueueDrainError, QueueMoveError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_move_pending_keeps_id_history_and_pointers(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"kind": "parent"})
    src = queue.enqueue(
        tenant_id="t1",
        queue_name="wrong",
        payload={"kind": "render", "n": 1},
        priority=7,
        labels=["vip"],
        prefer_last_worker=True,
        max_worker_hops=3,
        depends_on=parent.id,
    )
    clone = queue.clone_job(src.id, tenant_id="t1")
    moved = queue.move_job_queue(src.id, tenant_id="t1", queue_name="render")
    assert moved.id == src.id
    assert moved.queue_name == "render"
    assert moved.status == JobStatus.PENDING
    assert moved.priority == 7
    assert moved.labels == ("vip",)
    assert moved.prefer_last_worker is True
    assert moved.max_worker_hops == 3
    assert moved.depends_on == (parent.id,)
    assert moved.cloned_from_job_id is None
    assert moved.payload == {"kind": "render", "n": 1}
    still = queue.get(src.id, tenant_id="t1")
    assert still is not None and still.queue_name == "render"
    clone_now = queue.get(clone.id, tenant_id="t1")
    assert clone_now is not None
    assert clone_now.cloned_from_job_id == src.id
    assert clone_now.queue_name == "wrong"
    events = queue.list_job_audit(src.id, tenant_id="t1")
    assert [e.event_type for e in events] == [
        "enqueued",
        "cloned",
        "queue_moved",
    ]
    moved_ev = events[-1]
    assert moved_ev.detail["from_queue"] == "wrong"
    assert moved_ev.detail["to_queue"] == "render"
    assert moved_ev.detail["job"]["queue_name"] == "render"


def test_move_same_queue_is_noop(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1", queue_name="email", payload={"kind": "mail"}
    )
    again = queue.move_job_queue(job.id, tenant_id="t1", queue_name="email")
    assert again.id == job.id
    assert again.queue_name == "email"
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued"]


def test_moved_job_leases_from_new_queue_only(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1", queue_name="wrong", payload={"kind": "render"}
    )
    assert queue.lease(tenant_id="t1", worker_id="w", queue_name="default") is None
    moved = queue.move_job_queue(job.id, tenant_id="t1", queue_name="default")
    assert moved.queue_name == "default"
    got = queue.lease(tenant_id="t1", worker_id="w", queue_name="default")
    assert got is not None and got.id == job.id
    assert queue.lease(tenant_id="t1", worker_id="w", queue_name="wrong") is None
    _ack(queue, got, "w", result={"ok": True})
    done = queue.get(job.id, tenant_id="t1")
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.queue_name == "default"


def test_move_rejects_leased_and_terminal(queue: JobQueue):
    live = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    with pytest.raises(QueueMoveError, match="cannot move"):
        queue.move_job_queue(live.id, tenant_id="t1", queue_name="email")
    _ack(queue, leased, "w")
    with pytest.raises(QueueMoveError, match="cannot move"):
        queue.move_job_queue(live.id, tenant_id="t1", queue_name="email")


def test_move_rejects_missing_and_cross_tenant(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", queue_name="wrong", payload={"x": 1})
    with pytest.raises(QueueMoveError, match="not found"):
        queue.move_job_queue(job.id, tenant_id="t2", queue_name="default")
    with pytest.raises(QueueMoveError, match="not found"):
        queue.move_job_queue(uuid4(), tenant_id="t1", queue_name="default")


def test_move_blocked_by_dest_drain_source_ok(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1", queue_name="wrong", payload={"kind": "mail"}
    )
    queue.drain_tenant_queue("t1", "email")
    with pytest.raises(QueueDrainError, match="draining"):
        queue.move_job_queue(job.id, tenant_id="t1", queue_name="email")
    leftover = queue.get(job.id, tenant_id="t1")
    assert leftover is not None and leftover.queue_name == "wrong"
    queue.drain_tenant_queue("t1", "wrong")
    moved = queue.move_job_queue(job.id, tenant_id="t1", queue_name="default")
    assert moved.queue_name == "default"


def test_move_dedupe_conflict_on_dest(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        queue_name="email",
        payload={"a": 1},
        dedupe_key="once",
    )
    other = queue.enqueue(
        tenant_id="t1",
        queue_name="wrong",
        payload={"a": 2},
        dedupe_key="once",
    )
    with pytest.raises(QueueMoveError, match="dedupe_key"):
        queue.move_job_queue(other.id, tenant_id="t1", queue_name="email")
    still = queue.get(other.id, tenant_id="t1")
    assert still is not None and still.queue_name == "wrong"


def test_move_keeps_dedupe_when_dest_is_free(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        queue_name="wrong",
        payload={"a": 1},
        dedupe_key="once",
    )
    moved = queue.move_job_queue(job.id, tenant_id="t1", queue_name="email")
    assert moved.dedupe_key == "once"
    assert moved.queue_name == "email"
    # Same key can still exist on the old queue.
    again = queue.enqueue(
        tenant_id="t1",
        queue_name="wrong",
        payload={"a": 2},
        dedupe_key="once",
    )
    assert again.id != job.id


def test_move_frozen_and_rebuild(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1", queue_name="wrong", payload={"kind": "render"}
    )
    frozen = queue.freeze(job.id, tenant_id="t1")
    moved = queue.move_job_queue(frozen.id, tenant_id="t1", queue_name="render")
    assert moved.status == JobStatus.FROZEN
    assert moved.queue_name == "render"
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (job.id,))
        conn.commit()
    rebuilt = queue.rebuild_job_from_audit(job.id, tenant_id="t1")
    assert rebuilt.queue_name == "render"
    assert rebuilt.status == JobStatus.FROZEN
    assert rebuilt.id == job.id


def test_move_http(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="http-move",
        queue_name="wrong",
        payload={"kind": "render"},
        labels=["hot"],
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        moved = client.move_job_queue(
            src.id, tenant_id="http-move", queue_name="default"
        )
        assert moved.id == src.id
        assert moved.queue_name == "default"
        listed = client.list_jobs(tenant_id="http-move", queue_name="default")
        assert [j.id for j in listed] == [src.id]
        with pytest.raises(QueueMoveError):
            client.move_job_queue(src.id, tenant_id="other", queue_name="default")
    finally:
        server.shutdown()
