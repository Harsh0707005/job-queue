"""Priority, nack, dead-letter, skip-limit removal."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
    _nack,
)

# ---------------------------------------------------------------------------
# Priority, nack, dead-letter, skip-limit removal
# ---------------------------------------------------------------------------

def test_higher_priority_leased_first(queue: JobQueue):
    low = queue.enqueue(tenant_id="t1", payload={"p": "low"}, priority=1)
    high = queue.enqueue(tenant_id="t1", payload={"p": "high"}, priority=10)

    first = queue.lease(tenant_id="t1", worker_id="w1")
    assert first is not None
    assert first.id == high.id

    second = queue.lease(tenant_id="t1", worker_id="w1")
    assert second is not None
    assert second.id == low.id

def test_nack_requeues_job(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=3,
        retry_backoff_base_seconds=0,  # immediate for this unit path
    )
    job = queue.lease(tenant_id="t1", worker_id="w1")
    assert job is not None

    nacked = _nack(queue, job, "w1", error="boom", requeue=True)
    assert nacked.status == JobStatus.PENDING
    assert nacked.last_error == "boom"

    again = queue.lease(tenant_id="t1", worker_id="w2")
    assert again is not None
    assert again.id == job.id
    assert again.attempts == 2

def test_nack_dead_letters_after_max_attempts(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=2,
        retry_backoff_base_seconds=0,
    )

    for i, wid in enumerate(["w1", "w2"], start=1):
        job = queue.lease(tenant_id="t1", worker_id=wid)
        assert job is not None
        assert job.attempts == i
        result = _nack(queue, job, wid, error=f"fail-{i}", requeue=True)
        if i < 2:
            assert result.status == JobStatus.PENDING
        else:
            assert result.status == JobStatus.DEAD

    assert queue.lease(tenant_id="t1", worker_id="w3") is None
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 1

def test_many_dead_lettered_jobs_do_not_block_claimable_work(queue: JobQueue):
    """Previously a skip-limit of 32 could return None while jobs remained.

    Seed many exhausted expired leases, then one healthy pending job. Lease
    must still return the healthy job (bulk dead-letter + no skip cap).
    """
    # 50 jobs that will be dead-lettered after one expired lease attempt.
    for i in range(50):
        queue.enqueue(
            tenant_id="t1",
            payload={"dead": i},
            max_attempts=1,
            visibility_timeout_seconds=1,
            priority=100,  # higher priority — sit at the head
        )

    # Lease each once so attempts=1, then let them expire.
    for i in range(50):
        j = queue.lease(
            tenant_id="t1",
            worker_id=f"temp-{i}",
            visibility_timeout_seconds=1,
        )
        assert j is not None

    # Healthy job with lower priority (would be behind dead ones by priority).
    good = queue.enqueue(
        tenant_id="t1",
        payload={"good": True},
        max_attempts=3,
        visibility_timeout_seconds=30,
        priority=0,
    )

    time.sleep(1.2)

    # First lease pass should dead-letter exhausted orphans and return good.
    claimed = queue.lease(tenant_id="t1", worker_id="rescuer")
    assert claimed is not None
    assert claimed.id == good.id
    assert claimed.payload.get("good") is True

    _ack(queue, claimed, "rescuer")
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 50
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1

