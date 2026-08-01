"""Visibility timeout / worker death recovery."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
    _hb,
)

# ---------------------------------------------------------------------------
# Visibility timeout / worker death recovery
# ---------------------------------------------------------------------------

def test_expired_lease_becomes_available_again(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={"task": "fragile"},
        visibility_timeout_seconds=2,
    )

    dead = queue.lease(
        tenant_id="t1",
        worker_id="dead-worker",
        visibility_timeout_seconds=2,
    )
    assert dead is not None
    assert dead.leased_by == "dead-worker"

    assert queue.lease(tenant_id="t1", worker_id="alive") is None

    time.sleep(2.2)

    reclaimed = queue.lease(tenant_id="t1", worker_id="alive")
    assert reclaimed is not None
    assert reclaimed.id == dead.id
    assert reclaimed.leased_by == "alive"
    assert reclaimed.attempts == 2
    assert reclaimed.lease_token != dead.lease_token

    _ack(queue, reclaimed, "alive")
    final = queue.get(reclaimed.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.COMPLETED

def test_heartbeat_prevents_reclaim(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={"task": "long"},
        visibility_timeout_seconds=2,
    )
    job = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=2,
    )
    assert job is not None

    for _ in range(3):
        time.sleep(1.0)
        extended = _hb(queue, job, "w1", extend_by_seconds=2)
        assert extended.lease_expires_at is not None

    assert queue.lease(tenant_id="t1", worker_id="w2") is None
    _ack(queue, job, "w1")

def test_heartbeat_from_wrong_worker_fails(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={})
    job = queue.lease(tenant_id="t1", worker_id="w1")
    assert job is not None
    assert job.lease_token is not None

    with pytest.raises(LeaseError):
        queue.heartbeat(
            job.id,
            tenant_id="t1",
            worker_id="impostor",
            lease_token=job.lease_token,
        )

def test_ack_from_wrong_worker_fails(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={})
    job = queue.lease(tenant_id="t1", worker_id="w1")
    assert job is not None
    assert job.lease_token is not None

    with pytest.raises(LeaseError):
        queue.ack(
            job.id,
            tenant_id="t1",
            worker_id="impostor",
            lease_token=job.lease_token,
        )

def test_expired_lease_rejects_ack(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=1,
    )
    job = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=1,
    )
    assert job is not None
    time.sleep(1.2)

    with pytest.raises(LeaseError):
        _ack(queue, job, "w1")

