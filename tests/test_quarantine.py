"""Quarantine jobs that hop across distinct failing workers."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _nack


def test_quarantine_dead_after_distinct_worker_nacks(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="q-dead",
        payload={"kind": "fail"},
        max_attempts=10,
        max_worker_hops=2,
        quarantine_action="dead",
        retry_backoff_base_seconds=0,
    )
    a = queue.lease(tenant_id="q-dead", worker_id="w1")
    assert a is not None
    _nack(queue, a, "w1", error="boom1")
    mid = queue.get(job.id, tenant_id="q-dead")
    assert mid is not None and mid.status == JobStatus.PENDING
    assert mid.failed_workers == ("w1",)

    b = queue.lease(tenant_id="q-dead", worker_id="w2")
    assert b is not None
    dead = _nack(queue, b, "w2", error="boom2")
    assert dead.status == JobStatus.DEAD
    assert set(dead.failed_workers) == {"w1", "w2"}
    assert queue.lease(tenant_id="q-dead", worker_id="w3") is None


def test_quarantine_release_parks_without_dead_letter(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="q-rel",
        payload={"kind": "fail"},
        max_attempts=10,
        max_worker_hops=2,
        quarantine_action="release",
        retry_backoff_base_seconds=0,
    )
    for wid in ("w1", "w2"):
        leased = queue.lease(tenant_id="q-rel", worker_id=wid)
        assert leased is not None
        out = _nack(queue, leased, wid, error=f"fail-{wid}")
    assert out.status == JobStatus.QUARANTINED
    got = queue.get(job.id, tenant_id="q-rel")
    assert got is not None and got.status == JobStatus.QUARANTINED
    assert queue.lease(tenant_id="q-rel", worker_id="w3") is None
    # Same-tenant healthy work is unaffected.
    ok = queue.enqueue(tenant_id="q-rel", payload={"ok": True})
    leased_ok = queue.lease(tenant_id="q-rel", worker_id="w3")
    assert leased_ok is not None and leased_ok.id == ok.id
    _ack(queue, leased_ok, "w3")


def test_quarantine_same_worker_retry_still_stops_the_hop(queue: JobQueue):
    """A job bouncing on one worker must not jump forever either."""
    job = queue.enqueue(
        tenant_id="q-same",
        payload={"x": 1},
        max_attempts=5,
        max_worker_hops=2,
        quarantine_action="dead",
        retry_backoff_base_seconds=0,
    )
    first = queue.lease(tenant_id="q-same", worker_id="solo")
    assert first is not None
    out = _nack(queue, first, "solo", error="again")
    assert out.status == JobStatus.PENDING
    second = queue.lease(tenant_id="q-same", worker_id="solo")
    assert second is not None
    dead = _nack(queue, second, "solo", error="again")
    assert dead.status == JobStatus.DEAD
    got = queue.get(job.id, tenant_id="q-same")
    assert got is not None and got.status == JobStatus.DEAD
    assert got.failed_workers == ("solo", "solo")


def test_quarantine_tenant_default_and_isolation(queue: JobQueue):
    queue.set_tenant_quarantine("q-def", 2, action="release")
    poison = queue.enqueue(
        tenant_id="q-def",
        payload={"p": 1},
        max_attempts=8,
        retry_backoff_base_seconds=0,
    )
    assert poison.max_worker_hops == 2
    assert poison.quarantine_action == "release"
    other = queue.enqueue(
        tenant_id="q-free",
        payload={"p": 1},
        max_attempts=8,
        retry_backoff_base_seconds=0,
    )
    assert other.max_worker_hops is None
    for wid in ("a", "b"):
        j = queue.lease(tenant_id="q-def", worker_id=wid)
        assert j is not None
        _nack(queue, j, wid, error="x")
    parked = queue.get(poison.id, tenant_id="q-def")
    assert parked is not None and parked.status == JobStatus.QUARANTINED
    # Uncapped tenant can still hop.
    for wid in ("a", "b", "c"):
        j = queue.lease(tenant_id="q-free", worker_id=wid)
        assert j is not None
        _nack(queue, j, wid, error="x")
    still = queue.get(other.id, tenant_id="q-free")
    assert still is not None and still.status == JobStatus.PENDING


def test_quarantine_expired_lease_hop(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="q-exp",
        payload={"x": 1},
        max_attempts=10,
        max_worker_hops=2,
        quarantine_action="dead",
        visibility_timeout_seconds=1,
    )
    first = queue.lease(tenant_id="q-exp", worker_id="dead-w")
    assert first is not None
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE jobs SET lease_expires_at = %s WHERE id = %s",
            (_utcnow() - timedelta(seconds=5), job.id),
        )
        conn.commit()
    # Reclaim by a second worker records the dead worker hop; still pending.
    second = queue.lease(tenant_id="q-exp", worker_id="w2")
    assert second is not None
    assert "dead-w" in second.failed_workers
    with queue._pool.connection() as conn:
        conn.execute(
            "UPDATE jobs SET lease_expires_at = %s WHERE id = %s",
            (_utcnow() - timedelta(seconds=5), job.id),
        )
        conn.commit()
    third = queue.lease(tenant_id="q-exp", worker_id="w3")
    assert third is None
    got = queue.get(job.id, tenant_id="q-exp")
    assert got is not None and got.status == JobStatus.DEAD


def test_quarantine_replay_clears_hops(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="q-rp",
        payload={"x": 1},
        max_attempts=8,
        max_worker_hops=2,
        quarantine_action="release",
        retry_backoff_base_seconds=0,
    )
    for wid in ("w1", "w2"):
        leased = queue.lease(tenant_id="q-rp", worker_id=wid)
        assert leased is not None
        _nack(queue, leased, wid, error="x")
    parked = queue.get(job.id, tenant_id="q-rp")
    assert parked is not None and parked.status == JobStatus.QUARANTINED
    replayed = queue.replay_dead_letter(job.id, tenant_id="q-rp")
    assert replayed.status == JobStatus.PENDING
    assert replayed.failed_workers == ()
    again = queue.lease(tenant_id="q-rp", worker_id="w9")
    assert again is not None


def test_quarantine_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        cfg = client.set_tenant_quarantine("http-q", 2, action="dead")
        assert cfg["max_worker_hops"] == 2
        assert client.get_tenant_quarantine("http-q")["quarantine_action"] == "dead"
        job = queue.enqueue(
            tenant_id="http-q",
            payload={"kind": "fail"},
            max_attempts=8,
            retry_backoff_base_seconds=0,
        )
        assert job.max_worker_hops == 2
        for wid in ("hw1", "hw2"):
            leased = queue.lease(tenant_id="http-q", worker_id=wid)
            assert leased is not None
            _nack(queue, leased, wid, error="x")
        got = queue.get(job.id, tenant_id="http-q")
        assert got is not None and got.status == JobStatus.DEAD
    finally:
        server.shutdown()
