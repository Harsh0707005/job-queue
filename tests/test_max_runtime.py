"""Max runtime hard stop (stuck heartbeat cannot hold forever)."""

from __future__ import annotations

import time

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
    _nack,
)

# ---------------------------------------------------------------------------
# Max runtime hard stop (stuck heartbeat cannot hold forever)
# ---------------------------------------------------------------------------


def test_heartbeat_cannot_extend_past_max_runtime(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=30,
        max_runtime_seconds=2,
        max_attempts=3,
    )
    leased = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=30,
    )
    assert leased is not None
    assert leased.lease_started_at is not None
    assert leased.max_runtime_seconds == 2
    # First heartbeat while within window succeeds but caps expiry.
    hb = queue.heartbeat(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        extend_by_seconds=30,
    )
    hard = leased.lease_started_at.timestamp() + 2
    assert hb.lease_expires_at is not None
    assert hb.lease_expires_at.timestamp() <= hard + 0.05

    time.sleep(2.1)
    with pytest.raises(LeaseError, match="max runtime"):
        queue.heartbeat(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )

    got = queue.get(leased.id, tenant_id="t1")
    assert got is not None
    # Force-released for retry (attempts=1 < max 3).
    assert got.status == JobStatus.PENDING
    assert got.last_error == "max runtime exceeded"
    assert got.lease_token is None


def test_max_runtime_dead_letters_after_max_attempts(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=30,
        max_runtime_seconds=1,
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    time.sleep(1.1)
    with pytest.raises(LeaseError, match="max runtime"):
        queue.heartbeat(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    got = queue.get(job.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.DEAD
    assert got.last_error == "max runtime exceeded"


def test_ack_rejected_after_max_runtime(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={},
        max_runtime_seconds=1,
        visibility_timeout_seconds=30,
        max_attempts=5,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    time.sleep(1.1)
    with pytest.raises(LeaseError, match="max runtime"):
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
            result={"late": True},
        )
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 0


def test_worker_stopped_by_max_runtime(queue: JobQueue):
    """Worker heartbeating a long job hits max_runtime and cannot complete."""
    queue.enqueue(
        tenant_id="t1",
        payload={},
        max_runtime_seconds=1,
        visibility_timeout_seconds=30,
        max_attempts=3,
    )

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="stuck",
        heartbeat_interval_seconds=0.2,
        visibility_timeout_seconds=30,
    )
    outcomes: list[str] = []

    @worker.handler
    def handle(job: Job, lease: LeaseContext) -> dict:
        outcomes.append("start")
        deadline = time.time() + 4.0
        while time.time() < deadline:
            if lease.lease_lost.is_set():
                outcomes.append("lost")
                raise LeaseError("stopped")
            try:
                lease.ensure_held()
            except LeaseError:
                outcomes.append("ensure")
                raise
            time.sleep(0.1)
        outcomes.append("done")
        return {"ok": True}

    with pytest.raises(LeaseError):
        worker.run_once()

    assert "done" not in outcomes
    assert "lost" in outcomes or "ensure" in outcomes
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 0
    # Force-released back to pending (or dead if attempts exhausted).
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0
    assert (
        queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
        or queue.count(tenant_id="t1", status=JobStatus.DEAD) == 1
    )


def test_parent_dead_cancels_running_child_and_pending_grandchild(queue: JobQueue):
    """When a mid-chain job dies, leased children are fenced and descendants cancelled."""

    root = queue.enqueue(tenant_id="t1", payload={"n": "root"})
    mid = queue.enqueue(tenant_id="t1", payload={"n": "mid"}, depends_on=root.id)
    leaf = queue.enqueue(tenant_id="t1", payload={"n": "leaf"}, depends_on=mid.id)

    # Complete root so mid can run.
    r = queue.lease(tenant_id="t1", worker_id="w1")
    assert r is not None and r.id == root.id
    _ack(queue, r, "w1")

    # Lease mid (running child of root; parent of leaf).
    m = queue.lease(tenant_id="t1", worker_id="w-mid")
    assert m is not None and m.id == mid.id
    assert m.lease_token is not None

    # Kill mid → cascade must stop mid's fence and cancel pending leaf.
    dead = _nack(queue, m, "w-mid", error="mid boom", requeue=False)
    assert dead.status in (JobStatus.FAILED, JobStatus.DEAD)

    mid_after = queue.get(mid.id, tenant_id="t1")
    # mid itself is failed/dead from nack, not cancelled
    assert mid_after is not None
    assert mid_after.status in (JobStatus.FAILED, JobStatus.DEAD)

    leaf_after = queue.get(leaf.id, tenant_id="t1")
    assert leaf_after is not None
    assert leaf_after.status == JobStatus.CANCELLED


def test_cancel_running_parent_kills_running_dependent_chain(queue: JobQueue):
    """Cancel leased mid-job: its leased child cannot finish (fence cleared)."""
    from job_queue import LeaseError

    # Build chain where we can have two leased jobs that depend transitively.
    # a completed → b leased, c depends on b still pending.
    # Then cancel b while leased → c cancelled.
    a = queue.enqueue(tenant_id="t1", payload={"n": "a"})
    b = queue.enqueue(tenant_id="t1", payload={"n": "b"}, depends_on=a.id)
    c = queue.enqueue(tenant_id="t1", payload={"n": "c"}, depends_on=b.id)

    la = queue.lease(tenant_id="t1", worker_id="wa")
    assert la is not None and la.id == a.id
    _ack(queue, la, "wa")

    lb = queue.lease(tenant_id="t1", worker_id="wb")
    assert lb is not None and lb.id == b.id
    token_b = lb.lease_token
    assert token_b is not None

    cancelled = queue.cancel(b.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED

    # Running holder cannot finish.
    with pytest.raises(LeaseError):
        queue.ack(b.id, tenant_id="t1", worker_id="wb", lease_token=token_b)
    with pytest.raises(LeaseError):
        queue.heartbeat(b.id, tenant_id="t1", worker_id="wb", lease_token=token_b)

    lc = queue.get(c.id, tenant_id="t1")
    assert lc is not None
    assert lc.status == JobStatus.CANCELLED
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1  # only a
