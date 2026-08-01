"""Exponential retry backoff."""

from __future__ import annotations

import time

import pytest

from job_queue import Job, JobQueue, JobStatus, Worker

from tests.helpers_queue import (
    _ack,
    _nack,
)

# ---------------------------------------------------------------------------
# Exponential retry backoff
# ---------------------------------------------------------------------------


def test_nack_applies_exponential_backoff_before_reclaimable(queue: JobQueue):
    from datetime import datetime, timezone

    from job_queue import compute_retry_backoff_seconds

    job = queue.enqueue(
        tenant_id="t1",
        payload={"retry": True},
        max_attempts=5,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=8,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.attempts == 1

    before = datetime.now(timezone.utc)
    nacked = _nack(queue, leased, "w1", error="temp fail", requeue=True)
    assert nacked.status == JobStatus.PENDING
    expected = compute_retry_backoff_seconds(1, base_seconds=1, max_seconds=8)
    assert expected == 1
    assert nacked.available_at >= before
    # Not immediately claimable.
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    time.sleep(1.15)
    again = queue.lease(tenant_id="t1", worker_id="w2")
    assert again is not None
    assert again.id == job.id
    assert again.attempts == 2


def test_backoff_grows_exponentially_and_caps(queue: JobQueue):
    from job_queue import compute_retry_backoff_seconds

    assert compute_retry_backoff_seconds(1, base_seconds=2, max_seconds=10) == 2
    assert compute_retry_backoff_seconds(2, base_seconds=2, max_seconds=10) == 4
    assert compute_retry_backoff_seconds(3, base_seconds=2, max_seconds=10) == 8
    assert (
        compute_retry_backoff_seconds(4, base_seconds=2, max_seconds=10) == 10
    )  # capped
    assert compute_retry_backoff_seconds(5, base_seconds=2, max_seconds=10) == 10

    queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=4,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=2,  # cap at 2s
    )
    # Fail attempt 1 → backoff 1s
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    n1 = _nack(queue, j, "w1", error="e1", requeue=True)
    assert n1.status == JobStatus.PENDING
    d1 = (n1.available_at - n1.updated_at).total_seconds()
    assert 0.9 <= d1 <= 1.5

    time.sleep(1.15)
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    assert j.attempts == 2
    # Fail attempt 2 → backoff min(2, 2)=2s
    n2 = _nack(queue, j, "w1", error="e2", requeue=True)
    d2 = (n2.available_at - n2.updated_at).total_seconds()
    assert 1.9 <= d2 <= 2.5

    time.sleep(2.15)
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    assert j.attempts == 3
    # Fail attempt 3 → still capped at 2s
    n3 = _nack(queue, j, "w1", error="e3", requeue=True)
    d3 = (n3.available_at - n3.updated_at).total_seconds()
    assert 1.9 <= d3 <= 2.5


def test_backoff_then_dead_letter_after_max_attempts(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=2,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=5,
    )
    j1 = queue.lease(tenant_id="t1", worker_id="w1")
    assert j1 is not None
    n1 = _nack(queue, j1, "w1", error="once", requeue=True)
    assert n1.status == JobStatus.PENDING
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    time.sleep(1.15)
    j2 = queue.lease(tenant_id="t1", worker_id="w2")
    assert j2 is not None
    assert j2.attempts == 2
    n2 = _nack(queue, j2, "w2", error="twice", requeue=True)
    assert n2.status == JobStatus.DEAD
    assert queue.lease(tenant_id="t1", worker_id="w3") is None
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 1


def test_fair_lease_skips_jobs_in_backoff(queue: JobQueue):
    """A nacked job in backoff must not be claimed by fair lease either."""
    broken = queue.enqueue(
        tenant_id="broken",
        payload={"b": 1},
        max_attempts=5,
        retry_backoff_base_seconds=2,
        retry_backoff_max_seconds=30,
    )
    healthy = queue.enqueue(tenant_id="healthy", payload={"h": 1})

    j = queue.lease(worker_id="fw")
    # Fair order: never-served tenants; broken enqueued first.
    assert j is not None
    if j.id == broken.id:
        _nack(queue, j, "fw", error="nope", requeue=True)
        # Immediate fair lease should get healthy, not broken-in-backoff.
        j2 = queue.lease(worker_id="fw")
        assert j2 is not None
        assert j2.id == healthy.id
        _ack(queue, j2, "fw")
        assert queue.lease(worker_id="fw") is None  # broken still in backoff
    else:
        # Unlikely but possible if fairness flips; still validate backoff.
        _ack(queue, j, "fw")
        j2 = queue.lease(worker_id="fw")
        assert j2 is not None
        assert j2.id == broken.id
        _nack(queue, j2, "fw", error="nope", requeue=True)
        assert queue.lease(worker_id="fw") is None


def test_worker_nack_uses_backoff(queue: JobQueue):
    enq = queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=3,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=5,
    )
    worker = Worker(queue, tenant_id="t1", worker_id="bw")

    @worker.handler
    def handle(job: Job) -> None:
        raise RuntimeError("still broken")

    with pytest.raises(RuntimeError, match="still broken"):
        worker.run_once()

    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    got = queue.get(enq.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.PENDING
    assert got.available_at > got.updated_at
    assert queue.lease(tenant_id="t1", worker_id="x") is None
    time.sleep(1.15)
    again = queue.lease(tenant_id="t1", worker_id="x")
    assert again is not None
    assert again.id == enq.id
    assert again.attempts == 2
