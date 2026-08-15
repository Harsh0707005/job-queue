"""Postpone a pending job without cancelling or cloning it."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, PostponeError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _as_utc, _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_postpone_keeps_id_and_skips_lease(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render", "n": 1},
        priority=9,
        labels=["hot"],
    )
    ready = queue.enqueue(tenant_id="t1", payload={"kind": "other"}, priority=0)
    later = queue.postpone_job(job.id, tenant_id="t1", delay_seconds=3600)
    assert later.id == job.id
    assert later.status == JobStatus.PENDING
    assert later.payload == {"kind": "render", "n": 1}
    assert later.priority == 9
    assert later.labels == ("hot",)
    assert later.available_at > job.available_at
    slack = (_as_utc(later.available_at) - _utcnow()).total_seconds()
    assert 3590 <= slack <= 3610
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None and first.id == ready.id
    _ack(queue, first, "w")
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.PENDING


def test_postpone_available_at_and_then_run_now(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    when = _utcnow() + timedelta(hours=2)
    delayed = queue.postpone_job(job.id, tenant_id="t1", available_at=when)
    assert abs((_as_utc(delayed.available_at) - when).total_seconds()) < 1
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    nowish = queue.postpone_job(job.id, tenant_id="t1", delay_seconds=0)
    assert _as_utc(nowish.available_at) <= _utcnow() + timedelta(seconds=1)
    got = queue.lease(tenant_id="t1", worker_id="w")
    assert got is not None and got.id == job.id
    _ack(queue, got, "w", result={"ok": True})
    done = queue.get(job.id, tenant_id="t1")
    assert done is not None and done.status == JobStatus.COMPLETED


def test_postpone_same_time_is_noop(queue: JobQueue):
    when = _utcnow() + timedelta(minutes=5)
    job = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, available_at=when
    )
    again = queue.postpone_job(job.id, tenant_id="t1", available_at=job.available_at)
    assert again.id == job.id
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued"]


def test_postpone_writes_audit(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    later = queue.postpone_job(job.id, tenant_id="t1", delay_seconds=30)
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued", "postponed"]
    ev = events[-1]
    assert ev.detail["from_available_at"]
    assert ev.detail["to_available_at"]
    assert ev.detail["job"]["status"] == "pending"
    assert later.status == JobStatus.PENDING


def test_postpone_rejects_leased_terminal_and_missing(queue: JobQueue):
    live = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    with pytest.raises(PostponeError, match="cannot be postponed"):
        queue.postpone_job(live.id, tenant_id="t1", delay_seconds=10)
    _ack(queue, leased, "w")
    with pytest.raises(PostponeError, match="cannot be postponed"):
        queue.postpone_job(live.id, tenant_id="t1", delay_seconds=10)
    frozen = queue.enqueue(tenant_id="t1", payload={"f": 1})
    queue.freeze(frozen.id, tenant_id="t1")
    with pytest.raises(PostponeError, match="cannot be postponed"):
        queue.postpone_job(frozen.id, tenant_id="t1", delay_seconds=10)
    with pytest.raises(PostponeError, match="not found"):
        queue.postpone_job(live.id, tenant_id="t2", delay_seconds=10)
    with pytest.raises(PostponeError, match="not found"):
        queue.postpone_job(uuid4(), tenant_id="t1", delay_seconds=10)


def test_postpone_arg_validation(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    with pytest.raises(ValueError, match="exactly one"):
        queue.postpone_job(job.id, tenant_id="t1")
    with pytest.raises(ValueError, match="exactly one"):
        queue.postpone_job(
            job.id,
            tenant_id="t1",
            delay_seconds=1,
            available_at=_utcnow(),
        )
    with pytest.raises(ValueError, match="delay_seconds"):
        queue.postpone_job(job.id, tenant_id="t1", delay_seconds=-1)
    with pytest.raises(ValueError, match="delay_seconds"):
        queue.postpone_job(job.id, tenant_id="t1", delay_seconds=400 * 24 * 3600)


def test_postpone_refuses_run_at_or_after_ttl(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, ttl_seconds=60
    )
    assert job.expires_at is not None
    with pytest.raises(PostponeError, match="expires_at"):
        queue.postpone_job(job.id, tenant_id="t1", delay_seconds=120)
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None
    assert still.available_at == job.available_at
    ok = queue.postpone_job(job.id, tenant_id="t1", delay_seconds=10)
    assert ok.available_at > job.available_at


def test_high_priority_postponed_does_not_starve(queue: JobQueue):
    hot = queue.enqueue(
        tenant_id="t1", payload={"hot": True}, priority=100
    )
    queue.postpone_job(hot.id, tenant_id="t1", delay_seconds=3600)
    ready = queue.enqueue(
        tenant_id="t1", payload={"ready": True}, priority=1
    )
    got = queue.lease(tenant_id="t1", worker_id="w")
    assert got is not None and got.id == ready.id
    _ack(queue, got, "w")


def test_postpone_then_deps_still_wait(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"kind": "parent"})
    child = queue.enqueue(
        tenant_id="t1", payload={"kind": "child"}, depends_on=parent.id
    )
    queue.postpone_job(child.id, tenant_id="t1", delay_seconds=0)
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None and first.id == parent.id
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="t1", worker_id="w")
    assert second is not None and second.id == child.id
    _ack(queue, second, "w")


def test_postpone_rebuild_keeps_new_available_at(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    later = queue.postpone_job(job.id, tenant_id="t1", delay_seconds=90)
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (job.id,))
        conn.commit()
    rebuilt = queue.rebuild_job_from_audit(job.id, tenant_id="t1")
    assert rebuilt.id == job.id
    assert rebuilt.status == JobStatus.PENDING
    assert abs(
        (_as_utc(rebuilt.available_at) - _as_utc(later.available_at)).total_seconds()
    ) < 1


def test_postpone_http(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="http-pp", payload={"kind": "render"}, priority=3
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        delayed = client.postpone_job(
            src.id, tenant_id="http-pp", delay_seconds=45
        )
        assert delayed.id == src.id
        assert delayed.status == JobStatus.PENDING
        slack = (_as_utc(delayed.available_at) - _utcnow()).total_seconds()
        assert 40 <= slack <= 50
        when = (_utcnow() + timedelta(minutes=3)).isoformat()
        again = client.postpone_job(
            src.id, tenant_id="http-pp", available_at=when
        )
        assert again.available_at > delayed.available_at
        with pytest.raises(PostponeError):
            client.postpone_job(src.id, tenant_id="other", delay_seconds=1)
        got = client.get_job(src.id, tenant_id="http-pp")
        assert got is not None and got.status == JobStatus.PENDING
    finally:
        server.shutdown()
