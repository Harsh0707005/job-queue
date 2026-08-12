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


def _hop_quarantine(queue: JobQueue, job, *, tenant_id: str, workers: tuple[str, ...]):
    out = None
    for i, wid in enumerate(workers):
        leased = None
        extras: list = []
        for n in range(8):
            batch = queue.lease_batch(
                tenant_id=tenant_id,
                worker_id=f"{wid}-{n}",
                limit=8,
                visibility_timeout_seconds=30,
            )
            for claimed in batch:
                if claimed.id == job.id:
                    leased = claimed
                else:
                    extras.append(claimed)
            if leased is not None:
                break
        assert leased is not None, (job.id, queue.list_jobs(tenant_id=tenant_id))
        for extra in extras:
            if extra.lease_token is not None:
                queue.release(
                    extra.id,
                    tenant_id=tenant_id,
                    worker_id=extra.leased_by or wid,
                    lease_token=extra.lease_token,
                )
        out = _nack(queue, leased, leased.leased_by or wid, error=f"fail-{wid}")
    return out


def test_quarantine_child_notifies_waiting_spawn_parent(queue: JobQueue):
    """A parked spawn parent must not wait forever when a child is quarantined."""
    for action, child_status in (
        ("dead", JobStatus.DEAD),
        ("release", JobStatus.QUARANTINED),
    ):
        tid = f"q-sp-{action}"
        queue.set_tenant_quarantine(tid, 2, action=action)
        parent = queue.enqueue(
            tenant_id=tid,
            payload={"p": True},
            retry_backoff_base_seconds=0,
        )
        leased = queue.lease(tenant_id=tid, worker_id="p1")
        assert leased is not None
        result = queue.spawn_children(
            parent.id,
            tenant_id=tid,
            worker_id="p1",
            lease_token=leased.lease_token,
            children=[
                {
                    "payload": {"c": 1},
                    "max_attempts": 8,
                    "retry_backoff_base_seconds": 0,
                },
                {
                    "payload": {"c": 2},
                    "max_attempts": 8,
                    "retry_backoff_base_seconds": 0,
                },
            ],
        )
        kids = list(result.children)
        poison, sibling = kids[0], kids[1]
        # Stamp hops on the poison child (spawn inherit is tenant default).
        assert poison.max_worker_hops == 2
        _hop_quarantine(queue, poison, tenant_id=tid, workers=("c1", "c2"))
        poison_after = queue.get(poison.id, tenant_id=tid)
        assert poison_after is not None and poison_after.status == child_status
        parent_after = queue.get(parent.id, tenant_id=tid)
        assert parent_after is not None
        assert parent_after.status == JobStatus.CANCELLED
        assert parent_after.await_children_outcome == "failed"
        assert parent_after.last_error is not None
        assert "child" in parent_after.last_error
        sib = queue.get(sibling.id, tenant_id=tid)
        assert sib is not None and sib.status == JobStatus.CANCELLED


def test_quarantine_dag_parent_cancels_pending_children(queue: JobQueue):
    """DAG children must not stay pending when their parent is quarantined."""
    for action, parent_status in (
        ("dead", JobStatus.DEAD),
        ("release", JobStatus.QUARANTINED),
    ):
        tid = f"q-dag-{action}"
        out = queue.enqueue_dag(
            tenant_id=tid,
            batch_key=f"g-{action}",
            jobs=[
                {
                    "ref": "extract",
                    "payload": {"n": 1},
                    "max_attempts": 8,
                    "retry_backoff_base_seconds": 0,
                    "max_worker_hops": 2,
                    "quarantine_action": action,
                },
                {
                    "ref": "t1",
                    "payload": {"n": 2},
                    "depends_on": ["extract"],
                },
                {
                    "ref": "t2",
                    "payload": {"n": 3},
                    "depends_on": ["extract"],
                },
            ],
        )
        parent = out.by_ref["extract"]
        _hop_quarantine(queue, parent, tenant_id=tid, workers=("d1", "d2"))
        got = queue.get(parent.id, tenant_id=tid)
        assert got is not None and got.status == parent_status
        for ref in ("t1", "t2"):
            child = queue.get(out.by_ref[ref].id, tenant_id=tid)
            assert child is not None
            assert child.status == JobStatus.CANCELLED
        assert queue.lease(tenant_id=tid, worker_id="none") is None


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
