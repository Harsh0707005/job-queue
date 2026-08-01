"""Basic enqueue / lease / ack."""

from __future__ import annotations

from job_queue import JobQueue, JobStatus

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Basic enqueue / lease / ack
# ---------------------------------------------------------------------------


def test_enqueue_and_lease(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"task": "hello"})
    assert job.status == JobStatus.PENDING
    assert job.attempts == 0
    assert job.lease_token is None

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == job.id
    assert leased.status == JobStatus.LEASED
    assert leased.leased_by == "w1"
    assert leased.lease_token is not None
    assert leased.attempts == 1
    assert leased.lease_expires_at is not None


def test_ack_completes_job(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={"n": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    done = _ack(queue, leased, "w1")
    assert done.status == JobStatus.COMPLETED
    assert done.completed_at is not None
    assert done.leased_by is None
    assert done.lease_token is None

    assert queue.lease(tenant_id="t1", worker_id="w2") is None


def test_empty_queue_returns_none(queue: JobQueue):
    assert queue.lease(tenant_id="t1", worker_id="w1") is None
