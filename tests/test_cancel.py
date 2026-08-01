"""Cancel pending jobs."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Cancel pending jobs
# ---------------------------------------------------------------------------

def test_cancel_pending_job(queue: JobQueue):
    from job_queue import CancelError  # noqa: F401 — import path check

    job = queue.enqueue(tenant_id="t1", payload={"x": 1})
    cancelled = queue.cancel(job.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED
    assert queue.lease(tenant_id="t1", worker_id="w1") is None
    got = queue.get(job.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.CANCELLED

def test_cancel_leased_job_stops_worker_finish(queue: JobQueue):
    """Cancelling a running job invalidates the fence; old worker cannot ack."""
    from job_queue import CancelError, LeaseError

    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.lease_token is not None

    cancelled = queue.cancel(leased.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED
    assert cancelled.lease_token is None
    assert cancelled.leased_by is None
    assert cancelled.last_error == "cancelled while running"

    # Old worker cannot heartbeat / ack / nack.
    with pytest.raises(LeaseError):
        queue.heartbeat(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    with pytest.raises(LeaseError):
        queue.nack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
            error="nope",
        )

    final = queue.get(leased.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.CANCELLED
    # Must not become completed.
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 0

def test_cancel_completed_job_rejected(queue: JobQueue):
    from job_queue import CancelError

    queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    with pytest.raises(CancelError):
        queue.cancel(leased.id, tenant_id="t1")

def test_cancel_cross_tenant_rejected(queue: JobQueue):
    from job_queue import CancelError

    job = queue.enqueue(tenant_id="alpha", payload={})
    with pytest.raises(CancelError, match="not found"):
        queue.cancel(job.id, tenant_id="beta")
    assert queue.get(job.id, tenant_id="alpha").status == JobStatus.PENDING

def test_cancel_unknown_job(queue: JobQueue):
    from job_queue import CancelError

    with pytest.raises(CancelError, match="not found"):
        queue.cancel(uuid4(), tenant_id="t1")

def test_cancel_frees_dedupe_key(queue: JobQueue):
    first = queue.enqueue(tenant_id="t1", payload={"v": 1}, dedupe_key="k")
    queue.cancel(first.id, tenant_id="t1")
    second = queue.enqueue(tenant_id="t1", payload={"v": 2}, dedupe_key="k")
    assert second.id != first.id
    assert second.payload == {"v": 2}

