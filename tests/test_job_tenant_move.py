"""Move a pending job to another tenant in place (same id, history)."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import (
    CancelError,
    JobQueue,
    JobStatus,
    QueueDrainError,
    TenantMoveError,
)
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_move_pending_flips_owner_keeps_id_and_history(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="alpha",
        payload={"kind": "render", "secret": "keep"},
        priority=6,
        labels=["vip"],
        prefer_last_worker=True,
        max_worker_hops=2,
    )
    moved = queue.move_job_tenant(src.id, tenant_id="alpha", to_tenant_id="beta")
    assert moved.id == src.id
    assert moved.tenant_id == "beta"
    assert moved.status == JobStatus.PENDING
    assert moved.payload == {"kind": "render", "secret": "keep"}
    assert moved.priority == 6
    assert moved.labels == ("vip",)
    assert moved.prefer_last_worker is True
    assert moved.max_worker_hops == 2
    assert queue.get(src.id, tenant_id="alpha") is None
    owned = queue.get(src.id, tenant_id="beta")
    assert owned is not None
    assert owned.tenant_id == "beta"
    assert owned.payload == {"kind": "render", "secret": "keep"}
    assert queue.list_jobs(tenant_id="alpha") == []
    betas = queue.list_jobs(tenant_id="beta")
    assert [j.id for j in betas] == [src.id]
    with pytest.raises(CancelError, match="not found"):
        queue.cancel(src.id, tenant_id="alpha")
    src_audit = queue.list_job_audit(src.id, tenant_id="alpha")
    assert src_audit == []
    events = queue.list_job_audit(src.id, tenant_id="beta")
    assert [e.event_type for e in events] == ["enqueued", "tenant_moved"]
    assert events[0].tenant_id == "alpha"
    moved_ev = events[-1]
    assert moved_ev.tenant_id == "beta"
    assert moved_ev.detail["from_tenant"] == "alpha"
    assert moved_ev.detail["to_tenant"] == "beta"
    assert moved_ev.detail["job"]["tenant_id"] == "beta"


def test_dest_tenant_can_lease_and_complete(queue: JobQueue):
    src = queue.enqueue(tenant_id="alpha", payload={"kind": "render"})
    queue.move_job_tenant(src.id, tenant_id="alpha", to_tenant_id="beta")
    assert queue.lease(tenant_id="alpha", worker_id="w") is None
    got = queue.lease(tenant_id="beta", worker_id="w")
    assert got is not None and got.id == src.id
    assert got.tenant_id == "beta"
    _ack(queue, got, "w", result={"ok": True})
    done = queue.get(src.id, tenant_id="beta")
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.result == {"ok": True}


def test_same_tenant_is_noop(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    again = queue.move_job_tenant(job.id, tenant_id="t1", to_tenant_id="t1")
    assert again.id == job.id
    assert again.tenant_id == "t1"
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued"]


def test_move_rejects_not_pending_and_missing(queue: JobQueue):
    live = queue.enqueue(tenant_id="alpha", payload={"kind": "render"})
    leased = queue.lease(tenant_id="alpha", worker_id="w")
    assert leased is not None
    with pytest.raises(TenantMoveError, match="cannot move"):
        queue.move_job_tenant(live.id, tenant_id="alpha", to_tenant_id="beta")
    _ack(queue, leased, "w")
    with pytest.raises(TenantMoveError, match="cannot move"):
        queue.move_job_tenant(live.id, tenant_id="alpha", to_tenant_id="beta")
    with pytest.raises(TenantMoveError, match="not found"):
        queue.move_job_tenant(live.id, tenant_id="beta", to_tenant_id="gamma")
    with pytest.raises(TenantMoveError, match="not found"):
        queue.move_job_tenant(uuid4(), tenant_id="alpha", to_tenant_id="beta")


def test_move_rejects_graph_links(queue: JobQueue):
    parent = queue.enqueue(tenant_id="alpha", payload={"kind": "parent"})
    child = queue.enqueue(
        tenant_id="alpha", payload={"kind": "child"}, depends_on=parent.id
    )
    with pytest.raises(TenantMoveError, match="depends"):
        queue.move_job_tenant(child.id, tenant_id="alpha", to_tenant_id="beta")
    still = queue.get(child.id, tenant_id="alpha")
    assert still is not None and still.tenant_id == "alpha"


def test_move_blocked_by_dest_drain_and_pending_cap(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="alpha", queue_name="email", payload={"kind": "mail"}
    )
    queue.drain_tenant_queue("beta", "email")
    with pytest.raises(QueueDrainError, match="draining"):
        queue.move_job_tenant(job.id, tenant_id="alpha", to_tenant_id="beta")
    leftover = queue.get(job.id, tenant_id="alpha")
    assert leftover is not None and leftover.tenant_id == "alpha"
    queue.undrain_tenant_queue("beta", "email")
    queue.set_pending_limit("beta", 1)
    queue.enqueue(tenant_id="beta", payload={"fill": True})
    with pytest.raises(TenantMoveError, match="pending job cap"):
        queue.move_job_tenant(job.id, tenant_id="alpha", to_tenant_id="beta")


def test_move_dedupe_conflict_on_dest(queue: JobQueue):
    queue.enqueue(
        tenant_id="beta", payload={"a": 1}, dedupe_key="once"
    )
    other = queue.enqueue(
        tenant_id="alpha", payload={"a": 2}, dedupe_key="once"
    )
    with pytest.raises(TenantMoveError, match="dedupe_key"):
        queue.move_job_tenant(other.id, tenant_id="alpha", to_tenant_id="beta")
    still = queue.get(other.id, tenant_id="alpha")
    assert still is not None and still.tenant_id == "alpha"


def test_move_clears_schedule_id_and_remaps_partition(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="alpha",
        interval_seconds=60,
        payload={"kind": "tick"},
        name="tick",
    )
    fires = queue.process_due_schedules(tenant_id="alpha", limit=1)
    assert fires
    job = fires[0].job
    assert job.schedule_id == sched.id
    moved = queue.move_job_tenant(job.id, tenant_id="alpha", to_tenant_id="beta")
    assert moved.schedule_id is None
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at = NOW() WHERE id = %s",
            (sched.id,),
        )
        conn.commit()
        expected = queue._compute_partition_id(
            conn, tenant_id="beta", job_id=job.id
        )
    more = queue.process_due_schedules(tenant_id="alpha", limit=1)
    # Source schedule can fire again because the moved job dropped schedule_id.
    assert more
    assert more[0].job.id != job.id
    assert more[0].job.tenant_id == "alpha"
    assert moved.partition_id == expected


def test_move_rebuild_keeps_dest_owner(queue: JobQueue):
    job = queue.enqueue(tenant_id="alpha", payload={"kind": "render"})
    queue.move_job_tenant(job.id, tenant_id="alpha", to_tenant_id="beta")
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (job.id,))
        conn.commit()
    rebuilt = queue.rebuild_job_from_audit(job.id, tenant_id="beta")
    assert rebuilt.tenant_id == "beta"
    assert rebuilt.id == job.id
    assert rebuilt.status == JobStatus.PENDING


def test_move_http(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="http-src",
        payload={"kind": "render", "n": 1},
        labels=["hot"],
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        moved = client.move_job_tenant(
            src.id, tenant_id="http-src", to_tenant_id="http-dst"
        )
        assert moved.id == src.id
        assert moved.tenant_id == "http-dst"
        assert moved.payload == {"kind": "render", "n": 1}
        assert client.get_job(src.id, tenant_id="http-src") is None
        listed = client.list_jobs(tenant_id="http-dst")
        assert [j.id for j in listed] == [src.id]
        with pytest.raises(TenantMoveError):
            client.move_job_tenant(
                src.id, tenant_id="http-src", to_tenant_id="other"
            )
    finally:
        server.shutdown()
