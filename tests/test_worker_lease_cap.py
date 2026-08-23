"""Per-worker inflight lease cap."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, WorkerError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _hb


def test_worker_cap_stops_hot_worker_peer_takes_rest(queue: JobQueue):
    queue.register_worker("hot", skills=["cpu"])
    queue.register_worker("cool", skills=["cpu"])
    queue.set_worker_max_leases("hot", 1)
    assert queue.get_worker_max_leases("hot") == 1
    assert queue.get_worker("hot")["max_leases"] == 1
    for i in range(4):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    a = queue.lease(tenant_id="t1", worker_id="hot")
    b = queue.lease(tenant_id="t1", worker_id="hot")
    assert a is not None
    assert b is None
    c = queue.lease(tenant_id="t1", worker_id="cool")
    d = queue.lease(tenant_id="t1", worker_id="cool")
    assert c is not None and d is not None
    assert {c.id, d.id}.isdisjoint({a.id})
    assert queue.lease(tenant_id="t1", worker_id="hot") is None
    _ack(queue, a, "hot")
    again = queue.lease(tenant_id="t1", worker_id="hot")
    assert again is not None
    _ack(queue, again, "hot")
    _ack(queue, c, "cool")
    _ack(queue, d, "cool")


def test_worker_cap_applies_across_tenants_and_fair_path(queue: JobQueue):
    queue.register_worker("w", skills=["cpu"])
    queue.set_worker_max_leases("w", 1)
    queue.enqueue(tenant_id="ta", payload={"t": "a"})
    queue.enqueue(tenant_id="tb", payload={"t": "b"})
    first = queue.lease(tenant_id="ta", worker_id="w")
    assert first is not None
    assert queue.lease(tenant_id="tb", worker_id="w") is None
    assert queue.lease(worker_id="w") is None
    _ack(queue, first, "w")
    fair = queue.lease(worker_id="w")
    assert fair is not None
    assert fair.tenant_id == "tb"
    _ack(queue, fair, "w")


def test_lease_batch_respects_remaining_slots(queue: JobQueue):
    queue.register_worker("batch")
    queue.set_worker_max_leases("batch", 2)
    for i in range(6):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    got = queue.lease_batch(tenant_id="t1", worker_id="batch", limit=5)
    assert len(got) == 2
    assert queue.lease_batch(tenant_id="t1", worker_id="batch", limit=5) == []
    fair = queue.lease_batch(worker_id="batch", limit=5)
    assert fair == []
    _ack(queue, got[0], "batch")
    more = queue.lease_batch(tenant_id="t1", worker_id="batch", limit=5)
    assert len(more) == 1


def test_live_lower_cap_keeps_held_jobs_and_progress(queue: JobQueue):
    queue.register_worker("live")
    queue.set_worker_max_leases("live", 3)
    jobs = [queue.enqueue(tenant_id="t1", payload={"i": i}) for i in range(5)]
    held = queue.lease_batch(tenant_id="t1", worker_id="live", limit=3)
    assert len(held) == 3
    tokens = {j.id: j.lease_token for j in held}
    out = queue.set_worker_max_leases("live", 1)
    assert out["max_leases"] == 1
    assert out["leased"] == 3
    for j in held:
        got = queue.get(j.id, tenant_id="t1")
        assert got is not None
        assert got.status == JobStatus.LEASED
        assert got.leased_by == "live"
        assert got.lease_token == tokens[j.id]
        _hb(queue, got, "live")
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held[0], "live", result={"step": 1})
    assert queue.get(held[0].id, tenant_id="t1").status == JobStatus.COMPLETED
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held[1], "live")
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held[2], "live")
    nxt = queue.lease(tenant_id="t1", worker_id="live")
    assert nxt is not None
    assert nxt.id in {j.id for j in jobs}
    _ack(queue, nxt, "live")


def test_live_raise_and_clear_cap_does_not_halt(queue: JobQueue):
    queue.register_worker("grow")
    queue.set_worker_max_leases("grow", 1)
    for i in range(4):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    first = queue.lease(tenant_id="t1", worker_id="grow")
    assert first is not None
    assert queue.lease(tenant_id="t1", worker_id="grow") is None
    queue.set_worker_max_leases("grow", 3)
    extra = queue.lease_batch(tenant_id="t1", worker_id="grow", limit=5)
    assert len(extra) == 2
    queue.set_worker_max_leases("grow", None)
    assert queue.get_worker_max_leases("grow") is None
    last = queue.lease(tenant_id="t1", worker_id="grow")
    assert last is not None
    for j in [first, *extra, last]:
        _ack(queue, j, "grow")


def test_register_preserves_cap_unless_passed(queue: JobQueue):
    queue.register_worker("keep", skills=["gpu"], max_leases=2)
    assert queue.get_worker("keep")["max_leases"] == 2
    again = queue.register_worker("keep", skills=["gpu"])
    assert again["max_leases"] == 2
    assert again["skills"] == ["gpu"]
    hb = queue.worker_heartbeat("keep", skills=["gpu"])
    assert hb["max_leases"] == 2
    queue.register_worker("keep", skills=["gpu"], max_leases=4)
    assert queue.get_worker_max_leases("keep") == 4


def test_unknown_worker_cap_is_not_found(queue: JobQueue):
    with pytest.raises(WorkerError) as exc:
        queue.set_worker_max_leases("ghost", 1)
    assert exc.value.code == "worker_not_found"
    with pytest.raises(ValueError):
        queue.register_worker("bad", max_leases=0)


def test_unregistered_worker_is_uncapped(queue: JobQueue):
    for i in range(3):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    got = queue.lease_batch(tenant_id="t1", worker_id="anon", limit=3)
    assert len(got) == 3


def test_concurrent_claims_never_exceed_worker_cap(queue: JobQueue):
    queue.register_worker("race")
    queue.set_worker_max_leases("race", 3)
    for i in range(40):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    barrier = threading.Barrier(8)
    leased_ids: list[str] = []
    lock = threading.Lock()

    def _pull() -> None:
        barrier.wait()
        batch = queue.lease_batch(tenant_id="t1", worker_id="race", limit=5)
        with lock:
            leased_ids.extend(str(j.id) for j in batch)

    threads = [threading.Thread(target=_pull) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert len(leased_ids) == len(set(leased_ids))
    assert len(leased_ids) == 3
    assert queue.get_worker("race")["leased"] == 3


def test_skilled_hot_worker_leaves_work_for_peer(queue: JobQueue):
    queue.register_worker("gpu-hot", skills=["gpu"])
    queue.register_worker("gpu-cool", skills=["gpu"])
    queue.set_worker_max_leases("gpu-hot", 1)
    for i in range(3):
        queue.enqueue(
            tenant_id="t1", payload={"i": i}, required_skills=["gpu"]
        )
    a = queue.lease(tenant_id="t1", worker_id="gpu-hot")
    assert a is not None
    assert queue.lease(tenant_id="t1", worker_id="gpu-hot") is None
    b = queue.lease(tenant_id="t1", worker_id="gpu-cool")
    c = queue.lease(tenant_id="t1", worker_id="gpu-cool")
    assert b is not None and c is not None
    _ack(queue, a, "gpu-hot")
    _ack(queue, b, "gpu-cool")
    _ack(queue, c, "gpu-cool")


def test_http_worker_cap_roundtrip_and_live_change(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.register_worker("http-hot", skills=["cpu"], max_leases=1)
        client.register_worker("http-cool", skills=["cpu"])
        for i in range(3):
            client.enqueue(tenant_id="http-cap", payload={"i": i})
        first = client.lease(worker_id="http-hot", tenant_id="http-cap")
        assert first is not None
        assert client.lease(worker_id="http-hot", tenant_id="http-cap") is None
        peer = client.lease(worker_id="http-cool", tenant_id="http-cap")
        assert peer is not None
        got = client.get_worker("http-hot")
        assert got["max_leases"] == 1
        assert got["leased"] == 1
        raised = client.set_worker_max_leases("http-hot", 3)
        assert raised["max_leases"] == 3
        more = client.lease(worker_id="http-hot", tenant_id="http-cap")
        assert more is not None
        _ack(queue, first, "http-hot")
        _ack(queue, more, "http-hot")
        _ack(queue, peer, "http-cool")
        cleared = client.set_worker_max_leases("http-hot", None)
        assert cleared["max_leases"] is None
        with pytest.raises(WorkerError) as exc:
            client.set_worker_max_leases("missing", 1)
        assert exc.value.code == "worker_not_found"
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_inprocess_worker_stops_at_cap(queue: JobQueue):
    from job_queue import Worker

    queue.register_worker("loop-cap", skills=["cpu"])
    queue.set_worker_max_leases("loop-cap", 1)
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
        queue.enqueue(tenant_id="t1", payload={"n": i})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    deadline = time.monotonic() + 5
    while not seen and time.monotonic() < deadline:
        time.sleep(0.05)
    time.sleep(0.2)
    assert len(seen) == 1
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
