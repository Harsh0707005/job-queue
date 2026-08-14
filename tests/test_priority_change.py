"""Change priority on an already-enqueued job."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, PriorityError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_bump_pending_job_jumps_lease_order(queue: JobQueue):
    late = queue.enqueue(tenant_id="t1", payload={"who": "late"}, priority=0)
    for i in range(8):
        queue.enqueue(tenant_id="t1", payload={"who": "early", "i": i}, priority=0)
    bumped = queue.set_job_priority(late.id, tenant_id="t1", priority=50)
    assert bumped.priority == 50
    assert bumped.id == late.id
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None
    assert first.id == late.id
    assert first.priority == 50
    _ack(queue, first, "w")


def test_drop_priority_lets_others_go_first(queue: JobQueue):
    high = queue.enqueue(tenant_id="t1", payload={"who": "was-high"}, priority=20)
    low = queue.enqueue(tenant_id="t1", payload={"who": "stays"}, priority=5)
    dropped = queue.set_job_priority(high.id, tenant_id="t1", priority=1)
    assert dropped.priority == 1
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None
    assert first.id == low.id
    _ack(queue, first, "w")


def test_priority_delta_up_and_down(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1}, priority=10)
    up = queue.set_job_priority(job.id, tenant_id="t1", delta=7)
    assert up.priority == 17
    down = queue.set_job_priority(job.id, tenant_id="t1", delta=-4)
    assert down.priority == 13


def test_same_priority_is_noop(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1}, priority=3)
    again = queue.set_job_priority(job.id, tenant_id="t1", priority=3)
    assert again.priority == 3
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued"]


def test_priority_change_writes_audit(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1}, priority=1)
    queue.set_job_priority(job.id, tenant_id="t1", priority=9)
    events = queue.list_job_audit(job.id, tenant_id="t1")
    kinds = [e.event_type for e in events]
    assert kinds == ["enqueued", "priority_changed"]
    changed = events[-1]
    assert changed.detail["from_priority"] == 1
    assert changed.detail["to_priority"] == 9
    assert changed.detail["job"]["priority"] == 9


def test_priority_change_rebuild_keeps_new_value(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1}, priority=2)
    queue.set_job_priority(job.id, tenant_id="t1", priority=11)
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (job.id,))
        conn.commit()
    rebuilt = queue.rebuild_job_from_audit(job.id, tenant_id="t1")
    assert rebuilt.priority == 11
    assert rebuilt.status == JobStatus.PENDING


def test_other_tenant_cannot_change_priority(queue: JobQueue):
    job = queue.enqueue(tenant_id="owner", payload={"x": 1}, priority=1)
    with pytest.raises(PriorityError, match="not found"):
        queue.set_job_priority(job.id, tenant_id="intruder", priority=99)
    assert queue.get(job.id, tenant_id="owner").priority == 1


def test_terminal_job_cannot_change_priority(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"x": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    with pytest.raises(PriorityError, match="cannot change priority"):
        queue.set_job_priority(job.id, tenant_id="t1", priority=20)


def test_priority_validation(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"x": 1}, priority=0)
    with pytest.raises(ValueError, match="exactly one"):
        queue.set_job_priority(job.id, tenant_id="t1")
    with pytest.raises(ValueError, match="exactly one"):
        queue.set_job_priority(job.id, tenant_id="t1", priority=1, delta=1)
    with pytest.raises(ValueError, match="tenant_id"):
        queue.set_job_priority(job.id, tenant_id="", priority=1)
    with pytest.raises(ValueError, match="priority"):
        queue.set_job_priority(job.id, tenant_id="t1", delta=3_000_000_000)


def test_frozen_job_keeps_new_priority_after_unfreeze(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"x": 1}, priority=0)
    queue.enqueue(tenant_id="t1", payload={"noise": True}, priority=0)
    queue.freeze(job.id, tenant_id="t1")
    held = queue.set_job_priority(job.id, tenant_id="t1", priority=40)
    assert held.status == JobStatus.FROZEN
    assert held.priority == 40
    queue.unfreeze(job.id, tenant_id="t1")
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None
    assert first.id == job.id
    assert first.priority == 40
    _ack(queue, first, "w")


def test_bump_under_fair_lease_beats_low_backlog(queue: JobQueue):
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=0)
    late = queue.enqueue(tenant_id="quiet", payload={"urgent": True}, priority=0)
    queue.set_job_priority(late.id, tenant_id="quiet", priority=25)
    first = queue.lease(worker_id="fair")
    assert first is not None
    assert first.id == late.id
    _ack(queue, first, "fair")


def test_bumped_high_lane_still_before_heavy_weight_lows(queue: JobQueue):
    queue.set_tenant_lease_weight("heavy", 10)
    queue.set_tenant_lease_weight("light", 1)
    for i in range(20):
        queue.enqueue(tenant_id="heavy", payload={"i": i}, priority=0)
    late = queue.enqueue(tenant_id="light", payload={"urgent": True}, priority=0)
    queue.set_job_priority(late.id, tenant_id="light", priority=8)
    first = queue.lease(worker_id="fair")
    assert first is not None
    assert first.id == late.id
    assert first.tenant_id == "light"
    _ack(queue, first, "fair")


def test_bumped_jobs_rotate_by_weight_inside_high_lane(queue: JobQueue):
    queue.set_tenant_lease_weight("heavy", 4)
    queue.set_tenant_lease_weight("mid", 2)
    queue.set_tenant_lease_weight("light", 1)
    ids = {"heavy": [], "mid": [], "light": []}
    for tid in ("heavy", "mid", "light"):
        for i in range(20):
            job = queue.enqueue(tenant_id=tid, payload={"t": tid, "i": i}, priority=0)
            ids[tid].append(job.id)
        for jid in ids[tid]:
            queue.set_job_priority(jid, tenant_id=tid, priority=10)
    # leftover low noise must wait
    for i in range(10):
        queue.enqueue(tenant_id="noise", payload={"i": i}, priority=0)

    order: list[str] = []
    for _ in range(28):
        job = queue.lease(worker_id="wfq")
        assert job is not None
        assert job.priority == 10
        order.append(job.tenant_id)
        _ack(queue, job, "wfq")
    assert order.count("heavy") == 16
    assert order.count("mid") == 8
    assert order.count("light") == 4
    assert "noise" not in order


def test_priority_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{server.server_address[1]}")
        low = client.enqueue(tenant_id="http-p", payload={"n": 1}, priority=0)
        client.enqueue(tenant_id="http-p", payload={"n": 2}, priority=0)
        bumped = client.set_job_priority(low.id, tenant_id="http-p", priority=12)
        assert bumped.priority == 12
        with pytest.raises(PriorityError):
            client.set_job_priority(low.id, tenant_id="other", priority=99)
        first = client.lease(tenant_id="http-p", worker_id="w")
        assert first is not None
        assert first.id == low.id
        assert first.lease_token is not None
        client.ack(
            first.id,
            tenant_id="http-p",
            worker_id="w",
            lease_token=first.lease_token,
        )
        other = client.enqueue(tenant_id="http-p", payload={"n": 3}, priority=4)
        dropped = client.set_job_priority(other.id, tenant_id="http-p", delta=-3)
        assert dropped.priority == 1
    finally:
        server.shutdown()
