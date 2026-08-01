"""Checkpoints (resume after reclaim)."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

# ---------------------------------------------------------------------------
# Checkpoints (resume after reclaim)
# ---------------------------------------------------------------------------

def test_checkpoint_requires_live_lease(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"steps": 3})
    with pytest.raises(LeaseError):
        queue.checkpoint(
            job.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=uuid4(),
            progress={"step": 1},
        )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.lease_token is not None
    saved = queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 2, "cursor": "x"},
    )
    assert saved.checkpoint == {"step": 2, "cursor": "x"}
    assert saved.checkpointed_at is not None

    # Wrong worker rejected.
    with pytest.raises(LeaseError):
        queue.checkpoint(
            leased.id,
            tenant_id="t1",
            worker_id="other",
            lease_token=leased.lease_token,
            progress={"step": 3},
        )
    # Wrong tenant rejected even with valid token.
    with pytest.raises(LeaseError):
        queue.checkpoint(
            leased.id,
            tenant_id="other",
            worker_id="w1",
            lease_token=leased.lease_token,
            progress={"step": 3},
        )

def test_checkpoint_survives_reclaim_and_resume(queue: JobQueue):
    """Simulate worker death: expire lease, reclaim, resume from checkpoint."""
    job = queue.enqueue(
        tenant_id="t1",
        payload={"steps": 5},
        visibility_timeout_seconds=1,
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=1,
    )
    assert leased is not None and leased.lease_token is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 3},
    )

    # Let lease expire without heartbeat; second worker reclaims.
    time.sleep(1.2)
    reclaimed = queue.lease(
        tenant_id="t1",
        worker_id="w2",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None
    assert reclaimed.id == job.id
    assert reclaimed.leased_by == "w2"
    assert reclaimed.checkpoint == {"step": 3}
    assert reclaimed.checkpointed_at is not None

    # Old worker cannot overwrite checkpoint / ack.
    with pytest.raises(LeaseError):
        queue.checkpoint(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
            progress={"step": 99},
        )
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )

    # New worker continues and completes; checkpoint still visible on completed row.
    queue.checkpoint(
        reclaimed.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=reclaimed.lease_token,
        progress={"step": 5},
    )
    done = queue.ack(
        reclaimed.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=reclaimed.lease_token,
        result={"started_step": 3, "resumed": True},
    )
    assert done.status == JobStatus.COMPLETED
    assert done.checkpoint == {"step": 5}
    assert done.result == {"started_step": 3, "resumed": True}

def test_checkpoint_survives_nack_requeue(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"cursor": 42},
    )
    queue.nack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        error="transient",
        requeue=True,
    )
    pending = queue.get(job.id, tenant_id="t1")
    assert pending is not None
    assert pending.status == JobStatus.PENDING
    assert pending.checkpoint == {"cursor": 42}

def test_checkpoint_cleared_on_cancel(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    cancelled = queue.cancel(leased.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED
    assert cancelled.checkpoint is None
    assert cancelled.checkpointed_at is None

def test_lease_context_checkpoint(queue: JobQueue):
    from job_queue.worker import LeaseContext, Worker

    job = queue.enqueue(tenant_id="t1", payload={"steps": 2})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    ctx = LeaseContext(
        job=leased,
        queue=queue,
        worker_id="w1",
        lease_lost=threading.Event(),
    )
    updated = ctx.checkpoint({"step": 1})
    assert updated.checkpoint == {"step": 1}
    assert ctx.job.checkpoint == {"step": 1}

    # Worker handler path: process with resume.
    seen: list[int] = []

    def handler(j, lease: LeaseContext):
        start = 0
        if isinstance(j.checkpoint, dict):
            start = int(j.checkpoint.get("step") or 0)
        seen.append(start)
        for s in range(start, 2):
            lease.checkpoint({"step": s + 1})
        return {"started_step": start, "resumed": start > 0}

    # Current lease already has step 1; finish via direct call.
    r = handler(ctx.job, ctx)
    assert r["started_step"] == 1
    assert r["resumed"] is True
    queue.ack(
        ctx.job.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=ctx.job.lease_token,
        result=r,
    )
    _ = Worker  # imported for symmetry with real usage

