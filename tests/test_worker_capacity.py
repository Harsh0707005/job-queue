"""Worker advertised capacity vs job resource_cost."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, WorkerError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _hb


def test_enqueue_normalizes_resource_cost(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1",
        payload={"n": 1},
        resource_cost={" CPU ": 2, "memory": 1024, "cpu": 2},
    )
    assert j.resource_cost == (("cpu", 2), ("memory", 1024))
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None
    assert got.resource_cost == (("cpu", 2), ("memory", 1024))


def test_zero_cost_job_leases_on_any_worker(queue: JobQueue):
    queue.register_worker("plain")
    queue.register_worker("fat", capacity={"cpu": 1})
    j = queue.enqueue(tenant_id="t1", payload={"free": True})
    assert j.resource_cost == ()
    got = queue.lease(tenant_id="t1", worker_id="plain")
    assert got is not None and got.id == j.id
    _ack(queue, got, "plain")


def test_hot_worker_stops_when_capacity_full_peer_keeps_taking(queue: JobQueue):
    queue.register_worker("tiny", capacity={"cpu": 2, "memory": 1024})
    queue.register_worker("big", capacity={"cpu": 8, "memory": 8192})
    for i in range(6):
        queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            resource_cost={"cpu": 2, "memory": 512},
        )
    a = queue.lease(tenant_id="t1", worker_id="tiny")
    b = queue.lease(tenant_id="t1", worker_id="tiny")
    assert a is not None
    assert b is None
    snap = queue.get_worker("tiny")
    assert snap["capacity"] == {"cpu": 2, "memory": 1024}
    assert snap["used"]["cpu"] == 2
    assert snap["free"]["cpu"] == 0
    c = queue.lease(tenant_id="t1", worker_id="big")
    d = queue.lease(tenant_id="t1", worker_id="big")
    e = queue.lease(tenant_id="t1", worker_id="big")
    assert c is not None and d is not None and e is not None
    _ack(queue, a, "tiny")
    again = queue.lease(tenant_id="t1", worker_id="tiny")
    assert again is not None
    _ack(queue, again, "tiny")
    for j in (c, d, e):
        _ack(queue, j, "big")


def test_job_costing_unadvertised_resource_is_not_leased(queue: JobQueue):
    queue.register_worker("cpu-only", capacity={"cpu": 8})
    queue.enqueue(
        tenant_id="t1", payload={"gpu": 1}, resource_cost={"gpu": 1}
    )
    assert queue.lease(tenant_id="t1", worker_id="cpu-only") is None
    queue.register_worker("gpu", capacity={"gpu": 1})
    got = queue.lease(tenant_id="t1", worker_id="gpu")
    assert got is not None
    _ack(queue, got, "gpu")


def test_unconstrained_worker_cannot_take_costed_job(queue: JobQueue):
    queue.register_worker("open")
    queue.enqueue(tenant_id="t1", payload={"x": 1}, resource_cost={"cpu": 1})
    assert queue.lease(tenant_id="t1", worker_id="open") is None
    queue.set_worker_capacity("open", {"cpu": 1})
    got = queue.lease(tenant_id="t1", worker_id="open")
    assert got is not None
    _ack(queue, got, "open")


def test_batch_and_fair_path_respect_capacity(queue: JobQueue):
    queue.register_worker("mid", capacity={"cpu": 3})
    for i in range(5):
        queue.enqueue(
            tenant_id="t1", payload={"i": i}, resource_cost={"cpu": 2}
        )
    batch = queue.lease_batch(tenant_id="t1", worker_id="mid", limit=5)
    assert len(batch) == 1
    assert queue.lease_batch(worker_id="mid", limit=5) == []
    _ack(queue, batch[0], "mid")
    fair = queue.lease(worker_id="mid")
    assert fair is not None
    _ack(queue, fair, "mid")


def test_live_lower_capacity_keeps_inflight_and_blocks_new(queue: JobQueue):
    queue.register_worker("live", capacity={"cpu": 4, "memory": 4096})
    jobs = [
        queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            resource_cost={"cpu": 2, "memory": 1024},
        )
        for i in range(3)
    ]
    held = queue.lease_batch(tenant_id="t1", worker_id="live", limit=3)
    assert len(held) == 2
    tokens = {j.id: j.lease_token for j in held}
    out = queue.set_worker_capacity("live", {"cpu": 1, "memory": 256})
    assert out["capacity"] == {"cpu": 1, "memory": 256}
    assert out["used"]["cpu"] == 4
    assert out["free"]["cpu"] == 0
    for j in held:
        got = queue.get(j.id, tenant_id="t1")
        assert got is not None
        assert got.status == JobStatus.LEASED
        assert got.leased_by == "live"
        assert got.lease_token == tokens[j.id]
        _hb(queue, got, "live")
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held[0], "live", result={"kept": True})
    assert queue.get(held[0].id, tenant_id="t1").status == JobStatus.COMPLETED
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held[1], "live")
    # After holds finish, new cap=1 cannot take cpu=2 jobs.
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    leftover = [j for j in jobs if j.id not in {h.id for h in held}]
    assert leftover
    parked = queue.get(leftover[0].id, tenant_id="t1")
    assert parked is not None
    assert parked.status == JobStatus.AWAITING_CAPACITY
    assert parked.is_awaiting_capacity
    assert "capacity" in (parked.last_error or "")


def test_live_raise_capacity_allows_more(queue: JobQueue):
    queue.register_worker("grow", capacity={"cpu": 2})
    for i in range(3):
        queue.enqueue(tenant_id="t1", payload={"i": i}, resource_cost={"cpu": 2})
    first = queue.lease(tenant_id="t1", worker_id="grow")
    assert first is not None
    assert queue.lease(tenant_id="t1", worker_id="grow") is None
    queue.set_worker_capacity("grow", {"cpu": 6})
    extra = queue.lease_batch(tenant_id="t1", worker_id="grow", limit=5)
    assert len(extra) == 2
    for j in [first, *extra]:
        _ack(queue, j, "grow")


def test_register_preserves_capacity_unless_passed(queue: JobQueue):
    queue.register_worker("keep", skills=["gpu"], capacity={"cpu": 4})
    again = queue.register_worker("keep", skills=["gpu"])
    assert again["capacity"] == {"cpu": 4}
    hb = queue.worker_heartbeat("keep", skills=["gpu"])
    assert hb["capacity"] == {"cpu": 4}
    queue.register_worker("keep", skills=["gpu"], capacity={"cpu": 8, "memory": 1})
    assert queue.get_worker_capacity("keep") == {"cpu": 8, "memory": 1}


def test_unknown_worker_capacity_is_not_found(queue: JobQueue):
    with pytest.raises(WorkerError) as exc:
        queue.set_worker_capacity("ghost", {"cpu": 1})
    assert exc.value.code == "worker_not_found"
    with pytest.raises(ValueError):
        queue.register_worker("bad", capacity={"cpu": -1})


def test_http_capacity_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.register_worker(
            "http-tiny", skills=["cpu"], capacity={"cpu": 2, "memory": 1024}
        )
        client.register_worker(
            "http-big", skills=["cpu"], capacity={"cpu": 8, "memory": 8192}
        )
        job = client.enqueue(
            tenant_id="http-cap",
            payload={"n": 1},
            resource_cost={"cpu": 2, "memory": 512},
        )
        assert job.resource_cost == (("cpu", 2), ("memory", 512))
        first = client.lease(worker_id="http-tiny", tenant_id="http-cap")
        assert first is not None
        assert client.lease(worker_id="http-tiny", tenant_id="http-cap") is None
        extra = client.enqueue(
            tenant_id="http-cap",
            payload={"n": 2},
            resource_cost={"cpu": 2, "memory": 512},
        )
        peer = client.lease(worker_id="http-big", tenant_id="http-cap")
        assert peer is not None and peer.id == extra.id
        snap = client.get_worker("http-tiny")
        assert snap["capacity"]["cpu"] == 2
        assert snap["used"]["cpu"] == 2
        raised = client.set_worker_capacity(
            "http-tiny", {"cpu": 4, "memory": 1024}
        )
        assert raised["free"]["cpu"] == 2
        _ack(queue, first, "http-tiny")
        _ack(queue, peer, "http-big")
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_inprocess_worker_stops_when_resources_full(queue: JobQueue):
    from job_queue import Worker

    queue.register_worker("loop-cap", skills=["cpu"], capacity={"cpu": 1})
    seen: list[int] = []
    gate = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="loop-cap",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=30,
        skills=["cpu"],
        batch_size=4,
    )

    @worker.handler
    def _handle(job):
        seen.append(int(job.payload["n"]))
        gate.wait(2)

    for i in range(3):
        queue.enqueue(
            tenant_id="t1", payload={"n": i}, resource_cost={"cpu": 1}
        )
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    deadline = time.monotonic() + 5
    while not seen and time.monotonic() < deadline:
        time.sleep(0.05)
    time.sleep(0.2)
    assert len(seen) == 1
    leftover = queue.lease(tenant_id="t1", worker_id="peer")
    # unconstrained peer cannot take costed work
    assert leftover is None
    queue.register_worker("peer", capacity={"cpu": 4})
    leftover = queue.lease(tenant_id="t1", worker_id="peer")
    assert leftover is not None
    _ack(queue, leftover, "peer")
    gate.set()
    deadline = time.monotonic() + 5
    while len(seen) < 2 and time.monotonic() < deadline:
        time.sleep(0.05)
    worker.stop()
    t.join(timeout=3)
    assert len(seen) >= 2


def test_uncoverable_cost_parks_until_worker_covers(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "gpu"},
        resource_cost={"gpu": 1, "cpu": 2},
    )
    assert j.status == JobStatus.AWAITING_CAPACITY
    assert j.is_awaiting_capacity
    assert j.last_error == "waiting for worker capacity"
    assert queue.lease(tenant_id="t1", worker_id="anyone") is None
    parked = queue.list_awaiting_capacity(tenant_id="t1")
    assert [p.id for p in parked] == [j.id]
    stats = queue.get_tenant_stats("t1")
    assert stats.awaiting_capacity == 1
    assert stats.pending == 0

    queue.register_worker("almost", capacity={"cpu": 8})
    still = queue.get(j.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.AWAITING_CAPACITY

    queue.register_worker("fat", capacity={"gpu": 1, "cpu": 2})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="fat")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "fat")


def test_busy_large_worker_still_covers_pending(queue: JobQueue):
    """Advertised cover, not free remaining — do not park a job that can wait."""
    queue.register_worker("one", capacity={"cpu": 4})
    first = queue.enqueue(
        tenant_id="t1", payload={"i": 0}, resource_cost={"cpu": 4}
    )
    second = queue.enqueue(
        tenant_id="t1", payload={"i": 1}, resource_cost={"cpu": 4}
    )
    assert first.status == JobStatus.PENDING
    assert second.status == JobStatus.PENDING
    held = queue.lease(tenant_id="t1", worker_id="one")
    assert held is not None
    leftover = queue.get(second.id, tenant_id="t1")
    assert leftover is not None
    assert leftover.status == JobStatus.PENDING
    assert queue.list_awaiting_capacity(tenant_id="t1") == []
    assert queue.lease(tenant_id="t1", worker_id="one") is None
    _ack(queue, held, "one")
    nxt = queue.lease(tenant_id="t1", worker_id="one")
    assert nxt is not None and nxt.id == second.id
    _ack(queue, nxt, "one")


def test_last_covering_worker_leaving_reparks(queue: JobQueue):
    queue.register_worker("fat", capacity={"cpu": 8})
    j = queue.enqueue(
        tenant_id="t1", payload={"need": 8}, resource_cost={"cpu": 8}
    )
    assert j.status == JobStatus.PENDING
    queue.deregister_worker("fat")
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.AWAITING_CAPACITY
    assert queue.lease(tenant_id="t1", worker_id="tiny") is None


def test_drain_last_cover_reparks_resume_revives(queue: JobQueue):
    queue.register_worker("fat", capacity={"cpu": 8})
    j = queue.enqueue(
        tenant_id="t1", payload={"need": 8}, resource_cost={"cpu": 8}
    )
    assert j.status == JobStatus.PENDING
    queue.set_worker_maintenance("fat", draining=True)
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.AWAITING_CAPACITY
    queue.set_worker_maintenance("fat", draining=False)
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="fat")
    assert leased is not None
    _ack(queue, leased, "fat")


def test_raise_capacity_revives_parked(queue: JobQueue):
    queue.register_worker("tiny", capacity={"cpu": 1})
    j = queue.enqueue(
        tenant_id="t1", payload={"need": 4}, resource_cost={"cpu": 4}
    )
    assert j.status == JobStatus.AWAITING_CAPACITY
    queue.set_worker_capacity("tiny", {"cpu": 4})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="tiny")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "tiny")


def test_split_skills_and_capacity_parks_until_one_worker_covers(queue: JobQueue):
    """Skills on one node and capacity on another is not runnable."""
    queue.register_worker("skilled-small", skills=["gpu"], capacity={"cpu": 1})
    queue.register_worker("fat-unskilled", capacity={"cpu": 8, "memory": 8192})
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "gpu"},
        required_skills=["gpu"],
        resource_cost={"cpu": 8},
    )
    assert j.status == JobStatus.AWAITING_CAPACITY
    assert j.last_error == "waiting for worker capacity"
    assert queue.lease(tenant_id="t1", worker_id="skilled-small") is None
    assert queue.lease(tenant_id="t1", worker_id="fat-unskilled") is None
    assert queue.lease(worker_id="skilled-small") is None
    assert queue.list_awaiting_capacity(tenant_id="t1")[0].id == j.id
    assert queue.get_tenant_stats("t1").awaiting_capacity == 1
    assert queue.get_tenant_stats("t1").pending == 0

    queue.set_worker_capacity("fat-unskilled", {"cpu": 16})
    still = queue.get(j.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.AWAITING_CAPACITY

    queue.set_worker_capacity("skilled-small", {"cpu": 8})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="skilled-small")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "skilled-small")


def test_giving_fat_worker_the_skill_revives_split_cover(queue: JobQueue):
    queue.register_worker("gpu-tiny", skills=["gpu"], capacity={"cpu": 1})
    queue.register_worker("fat", capacity={"cpu": 8})
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "gpu"},
        required_skills=["gpu"],
        resource_cost={"cpu": 8},
    )
    assert j.status == JobStatus.AWAITING_CAPACITY
    queue.register_worker("fat", skills=["gpu"], capacity={"cpu": 8})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="fat")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "fat")


def test_split_cover_after_skilled_worker_joins_reparks(queue: JobQueue):
    queue.register_worker("fat", capacity={"cpu": 8})
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "tpu"},
        required_skills=["tpu"],
        resource_cost={"cpu": 8},
    )
    assert j.status == JobStatus.AWAITING_SKILLS
    queue.register_worker("tpu-tiny", skills=["tpu"], capacity={"cpu": 1})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.AWAITING_CAPACITY
    assert queue.lease(tenant_id="t1", worker_id="tpu-tiny") is None
    assert queue.lease(tenant_id="t1", worker_id="fat") is None
    queue.register_worker("tpu-tiny", skills=["tpu"], capacity={"cpu": 8})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="tpu-tiny")
    assert leased is not None
    _ack(queue, leased, "tpu-tiny")


def test_skills_win_when_both_uncoverable(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "tpu"},
        required_skills=["tpu"],
        resource_cost={"cpu": 99},
    )
    assert j.status == JobStatus.AWAITING_SKILLS
    assert queue.list_awaiting_capacity(tenant_id="t1") == []
    queue.register_worker("tpu-w", skills=["tpu"], capacity={"cpu": 1})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.AWAITING_CAPACITY
    queue.set_worker_capacity("tpu-w", {"cpu": 99})
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="tpu-w")
    assert leased is not None
    _ack(queue, leased, "tpu-w")


def test_awaiting_capacity_deadline_dead_letters(queue: JobQueue):
    from datetime import timedelta

    from job_queue.queue._common import _utcnow

    past = _utcnow() - timedelta(seconds=5)
    j = queue.enqueue(
        tenant_id="t1",
        payload={"need": "gpu"},
        resource_cost={"gpu": 4},
        deadline_at=past,
    )
    assert j.status == JobStatus.DEAD
    assert "resource cost" in (j.last_error or "")
    out = queue.process_awaiting_capacity()
    assert out["dead"] == 0
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status == JobStatus.DEAD


def test_cancel_awaiting_capacity_job(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t-ac",
        payload={"x": 1},
        resource_cost={"tpu": 1},
    )
    assert j.status == JobStatus.AWAITING_CAPACITY
    cancelled = queue.cancel(j.id, tenant_id="t-ac")
    assert cancelled.status == JobStatus.CANCELLED
    assert queue.list_awaiting_capacity(tenant_id="t-ac") == []


def test_http_awaiting_capacity_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        job = client.enqueue(
            tenant_id="http-ac",
            payload={"n": 1},
            resource_cost={"gpu": 8},
        )
        assert job.status == JobStatus.AWAITING_CAPACITY
        listed = client.list_jobs(tenant_id="http-ac", status="awaiting_capacity")
        assert [j.id for j in listed] == [job.id]
        stats = queue.get_tenant_stats("http-ac")
        assert stats.awaiting_capacity == 1
        client.register_worker("http-gpu", capacity={"gpu": 8})
        got = client.get_job(job.id, tenant_id="http-ac")
        assert got is not None and got.status == JobStatus.PENDING
        leased = client.lease(worker_id="http-gpu", tenant_id="http-ac")
        assert leased is not None and leased.id == job.id
        _ack(queue, leased, "http-gpu")
    finally:
        server.shutdown()
        t.join(timeout=5)
