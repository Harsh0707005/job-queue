"""Prefer the last worker that successfully finished similar work."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def _reg(queue: JobQueue, *wids: str) -> None:
    for wid in wids:
        queue.register_worker(wid)


def test_affinity_off_by_default(queue: JobQueue):
    _reg(queue, "wa", "wb")
    first = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    nxt = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    other = queue.enqueue(tenant_id="t1", payload={"kind": "other"})
    got = queue.lease(tenant_id="t1", worker_id="wb")
    assert got is not None
    # No opt-in: oldest pending (nxt) is fine for any worker.
    assert got.id == nxt.id
    _ack(queue, got, "wb")
    leftover = queue.lease(tenant_id="t1", worker_id="wb")
    assert leftover is not None and leftover.id == other.id
    _ack(queue, leftover, "wb")
    assert first.prefer_last_worker is False


def test_queue_affinity_prefers_last_success_worker(queue: JobQueue):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    queue.set_prefer_last_worker_hold("t1", 30)
    seed = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    assert seed.payload_kind == "render"
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")

    similar = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    other = queue.enqueue(tenant_id="t1", payload={"kind": "other", "n": 1})
    # wb should skip the similar job while wa is live.
    picked = queue.lease(tenant_id="t1", worker_id="wb")
    assert picked is not None
    assert picked.id == other.id
    _ack(queue, picked, "wb")
    mine = queue.lease(tenant_id="t1", worker_id="wa")
    assert mine is not None
    assert mine.id == similar.id
    _ack(queue, mine, "wa")


def test_job_flag_opts_in_without_queue_setting(queue: JobQueue):
    _reg(queue, "wa", "wb")
    seed = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render"},
        prefer_last_worker=True,
    )
    assert seed.prefer_last_worker is True
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    similar = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render", "n": 2},
        prefer_last_worker=True,
    )
    other = queue.enqueue(tenant_id="t1", payload={"kind": "other"})
    picked = queue.lease(tenant_id="t1", worker_id="wb")
    assert picked is not None
    assert picked.id == other.id
    _ack(queue, picked, "wb")
    mine = queue.lease(tenant_id="t1", worker_id="wa")
    assert mine is not None
    assert mine.id == similar.id
    _ack(queue, mine, "wa")


def test_set_job_prefer_last_worker_on_existing(queue: JobQueue):
    _reg(queue, "wa", "wb")
    seed = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render"},
        prefer_last_worker=True,
    )
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    waiting = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    marked = queue.set_job_prefer_last_worker(
        waiting.id, tenant_id="t1", enabled=True
    )
    assert marked.prefer_last_worker is True
    other = queue.enqueue(tenant_id="t1", payload={"kind": "other"})
    picked = queue.lease(tenant_id="t1", worker_id="wb")
    assert picked is not None
    assert picked.id == other.id
    _ack(queue, picked, "wb")


def test_dead_preferred_worker_does_not_block(queue: JobQueue):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    seed = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    queue.deregister_worker("wa")
    nxt = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    got = queue.lease(tenant_id="t1", worker_id="wb")
    assert got is not None
    assert got.id == nxt.id
    _ack(queue, got, "wb")


def test_unskilled_preferred_worker_does_not_block_capable_peer(queue: JobQueue):
    queue.register_worker("cpu", skills=["cpu"])
    queue.register_worker("gpu", skills=["gpu"])
    queue.set_queue_worker_affinity("t1", "default", True)
    seed = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render"},
        required_skills=["cpu"],
    )
    j = queue.lease(tenant_id="t1", worker_id="cpu")
    assert j is not None
    _ack(queue, j, "cpu")
    nxt = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render", "n": 2},
        required_skills=["gpu"],
    )
    got = queue.lease(tenant_id="t1", worker_id="gpu")
    assert got is not None
    assert got.id == nxt.id
    _ack(queue, got, "gpu")


def test_other_worker_still_takes_affinity_job_when_it_is_all_that_is_left(
    queue: JobQueue,
):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    seed = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    nxt = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    # wa is still alive but not leasing — wb must not starve the job.
    got = queue.lease(tenant_id="t1", worker_id="wb")
    assert got is not None
    assert got.id == nxt.id
    _ack(queue, got, "wb")


def test_hold_expiry_equalizes_ranking(queue: JobQueue):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    queue.set_prefer_last_worker_hold("t1", 1)
    seed = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    similar = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    time.sleep(1.15)
    other = queue.enqueue(tenant_id="t1", payload={"kind": "other"})
    # Hold expired: oldest pending (similar) ranks with others by created_at.
    got = queue.lease(tenant_id="t1", worker_id="wb")
    assert got is not None
    assert got.id == similar.id
    _ack(queue, got, "wb")
    leftover = queue.lease(tenant_id="t1", worker_id="wb")
    assert leftover is not None and leftover.id == other.id
    _ack(queue, leftover, "wb")


def test_affinity_isolated_per_tenant_and_kind(queue: JobQueue):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    queue.set_queue_worker_affinity("t2", "default", True)
    a = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    ja = queue.lease(tenant_id="t1", worker_id="wa")
    assert ja is not None
    _ack(queue, ja, "wa")
    b = queue.enqueue(tenant_id="t2", payload={"kind": "render"})
    jb = queue.lease(tenant_id="t2", worker_id="wb")
    assert jb is not None
    _ack(queue, jb, "wb")
    t1n = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2})
    t2n = queue.enqueue(tenant_id="t2", payload={"kind": "render", "n": 2})
    # wb should take t2's similar job, not t1's.
    got = queue.lease(worker_id="wb")
    assert got is not None
    assert got.id == t2n.id
    _ack(queue, got, "wb")
    mine = queue.lease(worker_id="wa")
    assert mine is not None
    assert mine.id == t1n.id
    _ack(queue, mine, "wa")
    assert a.id != b.id


def test_fair_lease_priority_still_beats_affinity(queue: JobQueue):
    _reg(queue, "wa", "wb")
    queue.set_queue_worker_affinity("t1", "default", True)
    seed = queue.enqueue(tenant_id="t1", payload={"kind": "render"}, priority=0)
    j = queue.lease(tenant_id="t1", worker_id="wa")
    assert j is not None
    _ack(queue, j, "wa")
    queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 2}, priority=0)
    high = queue.enqueue(tenant_id="t1", payload={"kind": "other"}, priority=10)
    first = queue.lease(tenant_id="t1", worker_id="wa")
    assert first is not None
    assert first.id == high.id
    _ack(queue, first, "wa")


def test_affinity_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{server.server_address[1]}")
        set_q = client.set_queue_worker_affinity(
            "http-af", "default", True, hold_seconds=20
        )
        assert "default" in set_q["queues"]
        assert set_q["hold_seconds"] == 20
        got = client.get_queue_worker_affinity("http-af")
        assert got["queues"] == ["default"]
        seed = client.enqueue(
            tenant_id="http-af",
            payload={"kind": "render"},
            prefer_last_worker=True,
        )
        assert seed.prefer_last_worker is True
        client.register_worker("wa")
        client.register_worker("wb")
        j = client.lease(tenant_id="http-af", worker_id="wa")
        assert j is not None and j.lease_token is not None
        client.ack(
            j.id,
            tenant_id="http-af",
            worker_id="wa",
            lease_token=j.lease_token,
        )
        similar = client.enqueue(
            tenant_id="http-af", payload={"kind": "render", "n": 2}
        )
        client.set_job_prefer_last_worker(
            similar.id, tenant_id="http-af", enabled=True
        )
        other = client.enqueue(
            tenant_id="http-af", payload={"kind": "other"}
        )
        picked = client.lease(tenant_id="http-af", worker_id="wb")
        assert picked is not None
        assert picked.id == other.id
    finally:
        server.shutdown()
