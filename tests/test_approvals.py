"""Approvals (park without lease, tenant-scoped, timeout, resume)."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

# ---------------------------------------------------------------------------
# Approvals (park without lease, tenant-scoped, timeout, resume)
# ---------------------------------------------------------------------------

def test_request_approval_releases_lease_and_slot(queue: JobQueue):
    queue.set_concurrency_limit("t1", 1)
    job = queue.enqueue(tenant_id="t1", payload={"steps": 4})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 2, "total": 4},
    )
    parked = queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        message="need sign-off",
        timeout_seconds=60,
    )
    assert parked.status == JobStatus.AWAITING_APPROVAL
    assert parked.lease_token is None
    assert parked.leased_by is None
    assert parked.checkpoint == {"step": 2, "total": 4}
    assert parked.approval_message == "need sign-off"
    assert parked.approval_deadline_at is not None
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0
    # Slot free for other work.
    other = queue.enqueue(tenant_id="t1", payload={"other": True})
    got = queue.lease(tenant_id="t1", worker_id="w2")
    assert got is not None and got.id == other.id
    # Old token cannot finish parked job.
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )

def test_approve_resumes_from_checkpoint(queue: JobQueue):
    from job_queue import ApprovalError

    job = queue.enqueue(tenant_id="t1", payload={"steps": 5})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 3, "total": 5},
        timeout_seconds=120,
    )
    # Cross-tenant approve fails.
    with pytest.raises(ApprovalError):
        queue.approve(job.id, tenant_id="other")

    approved = queue.approve(job.id, tenant_id="t1")
    assert approved.status == JobStatus.PENDING
    assert approved.approval_outcome == "approved"
    assert approved.checkpoint == {"step": 3, "total": 5}
    # Idempotent re-approve.
    again = queue.approve(job.id, tenant_id="t1")
    assert again.status == JobStatus.PENDING
    assert again.approval_outcome == "approved"

    resumed = queue.lease(tenant_id="t1", worker_id="w2")
    assert resumed is not None and resumed.id == job.id
    assert resumed.checkpoint == {"step": 3, "total": 5}
    assert resumed.approval_outcome == "approved"
    queue.ack(
        resumed.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=resumed.lease_token,
        result={"started_step": 3, "resumed": True},
    )
    done = queue.get(job.id, tenant_id="t1")
    assert done is not None and done.status == JobStatus.COMPLETED

def test_reject_idempotent_and_tenant_scoped(queue: JobQueue):
    from job_queue import ApprovalError

    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        timeout_seconds=30,
    )
    with pytest.raises(ApprovalError):
        queue.reject(job.id, tenant_id="other")
    rejected = queue.reject(job.id, tenant_id="t1", reason="nope")
    assert rejected.status == JobStatus.CANCELLED
    assert rejected.approval_outcome == "rejected"
    assert "nope" in (rejected.last_error or "")
    # Retry reject is safe.
    again = queue.reject(job.id, tenant_id="t1", reason="nope")
    assert again.status == JobStatus.CANCELLED
    assert again.approval_outcome == "rejected"
    # Cannot approve after reject.
    with pytest.raises(ApprovalError):
        queue.approve(job.id, tenant_id="t1")

def test_approval_timeout_auto_rejects(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        timeout_seconds=1,
        progress={"step": 1},
    )
    time.sleep(1.2)
    timed = queue.process_approval_timeouts(tenant_id="t1")
    assert len(timed) == 1
    assert timed[0].id == job.id
    assert timed[0].status == JobStatus.CANCELLED
    assert timed[0].approval_outcome == "timeout"
    # Still parked checkpoint was kept until cancel... timeout cancels but we
    # keep checkpoint for audit (cancel clears on cancel() only).
    assert timed[0].checkpoint == {"step": 1}
    # Second process is empty.
    assert queue.process_approval_timeouts(tenant_id="t1") == []

def test_list_awaiting_approvals(queue: JobQueue):
    a = queue.enqueue(tenant_id="t1", payload={"a": 1})
    b = queue.enqueue(tenant_id="t1", payload={"b": 1})
    for jid, w in ((a.id, "w1"), (b.id, "w2")):
        leased = queue.lease(tenant_id="t1", worker_id=w)
        assert leased is not None
        queue.request_approval(
            leased.id,
            tenant_id="t1",
            worker_id=w,
            lease_token=leased.lease_token,
            timeout_seconds=120,
        )
    waiting = queue.list_awaiting_approvals(tenant_id="t1")
    assert len(waiting) == 2
    assert {j.id for j in waiting} == {a.id, b.id}

def test_request_approval_idempotent_when_already_parked(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    p1 = queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        timeout_seconds=60,
    )
    # Second call without live lease still returns parked row (idempotent).
    p2 = queue.request_approval(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        timeout_seconds=60,
    )
    assert p1.status == p2.status == JobStatus.AWAITING_APPROVAL
    assert p1.id == p2.id == job.id

