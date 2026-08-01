"""Batch lease."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

# ---------------------------------------------------------------------------
# Batch lease
# ---------------------------------------------------------------------------

def test_lease_batch_tenant_scoped_priority_order(queue: JobQueue):
    low = queue.enqueue(tenant_id="t1", payload={"p": "low"}, priority=1)
    mid = queue.enqueue(tenant_id="t1", payload={"p": "mid"}, priority=5)
    high = queue.enqueue(tenant_id="t1", payload={"p": "high"}, priority=10)

    batch = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=3)
    assert len(batch) == 3
    assert [j.id for j in batch] == [high.id, mid.id, low.id]
    # Distinct fencing tokens.
    assert len({j.lease_token for j in batch}) == 3

def test_lease_batch_respects_limit_and_empty(queue: JobQueue):
    for i in range(5):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    batch = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=2)
    assert len(batch) == 2
    batch2 = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=10)
    assert len(batch2) == 3
    assert queue.lease_batch(tenant_id="t1", worker_id="w1", limit=5) == []

def test_lease_batch_fair_rotates_tenants_in_batch(queue: JobQueue):
    """A fair batch should not hand all slots to one tenant when peers are ready."""
    for t in ("a", "b", "c"):
        for i in range(10):
            queue.enqueue(tenant_id=t, payload={"t": t, "i": i})

    batch = queue.lease_batch(worker_id="fair-batch", limit=6)
    assert len(batch) == 6
    tenants = [j.tenant_id for j in batch]
    assert set(tenants) == {"a", "b", "c"}
    # No tenant should take more than half of a 6-job fair batch.
    for t in ("a", "b", "c"):
        assert tenants.count(t) <= 3

def test_lease_batch_fair_priority_before_low(queue: JobQueue):
    for i in range(10):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=0)
    for i in range(3):
        queue.enqueue(tenant_id="vip", payload={"i": i}, priority=100)

    batch = queue.lease_batch(worker_id="pri-batch", limit=5)
    assert len(batch) == 5
    # First three must be high priority.
    assert all(j.priority == 100 for j in batch[:3])
    assert all(j.tenant_id == "vip" for j in batch[:3])
    assert all(j.priority == 0 for j in batch[3:])

def test_lease_batch_concurrency_cap(queue: JobQueue):
    queue.set_concurrency_limit("capped", 2)
    for i in range(5):
        queue.enqueue(tenant_id="capped", payload={"i": i})

    batch = queue.lease_batch(tenant_id="capped", worker_id="w1", limit=5)
    assert len(batch) == 2
    more = queue.lease_batch(tenant_id="capped", worker_id="w2", limit=5)
    assert more == []

def test_worker_batch_processes_all(queue: JobQueue):
    from job_queue import Worker

    for i in range(7):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    worker = Worker(queue, tenant_id="t1", worker_id="bw", batch_size=3)

    @worker.handler
    def handle(job):
        return {"ok": True}

    n = worker.run(max_jobs=7, stop_when_idle=True)
    assert n == 7
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 7

def test_lease_batch_invalid_limit(queue: JobQueue):
    import pytest

    with pytest.raises(ValueError):
        queue.lease_batch(worker_id="w", limit=0)
    with pytest.raises(ValueError):
        queue.lease_batch(worker_id="w", limit=1000)

def test_cancel_running_worker_aborts_mid_process(queue: JobQueue):
    """Worker holding a lease must not complete after cancel."""
    from job_queue import LeaseError

    queue.enqueue(tenant_id="t1", payload={"slow": True})

    started = threading.Event()
    cancelled_seen = threading.Event()
    outcomes: list[str] = []

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="runner",
        heartbeat_interval_seconds=0.2,
        visibility_timeout_seconds=30,
    )

    @worker.handler
    def handle(job: Job, lease: LeaseContext) -> dict:
        outcomes.append("start")
        started.set()
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if lease.lease_lost.is_set():
                outcomes.append("lease_lost")
                cancelled_seen.set()
                raise LeaseError("aborted after cancel")
            try:
                lease.ensure_held()
            except LeaseError:
                outcomes.append("ensure_failed")
                cancelled_seen.set()
                raise
            time.sleep(0.05)
        outcomes.append("finished_ok")
        return {"ok": True}

    def run_worker() -> None:
        try:
            worker.run_once()
            outcomes.append("run_returned")
        except LeaseError:
            outcomes.append("run_lease_error")

    t = threading.Thread(target=run_worker)
    t.start()
    assert started.wait(timeout=5)

    # Cancel the running job.
    leased = None
    for _ in range(20):
        with queue._pool.connection() as conn:
            row = conn.execute(
                "SELECT id FROM jobs WHERE tenant_id = %s AND status = 'leased' LIMIT 1",
                ("t1",),
            ).fetchone()
        if row:
            leased = row["id"]
            break
        time.sleep(0.05)
    assert leased is not None
    cancelled = queue.cancel(leased, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED

    t.join(timeout=10)
    assert not t.is_alive()
    assert "finished_ok" not in outcomes
    assert "run_returned" not in outcomes
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.CANCELLED) == 1
    assert cancelled_seen.is_set() or "run_lease_error" in outcomes

def test_cancel_running_cascades_to_pending_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    child = queue.enqueue(
        tenant_id="t1",
        payload={"c": 1},
        depends_on=parent.id,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == parent.id

    queue.cancel(parent.id, tenant_id="t1")
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.CANCELLED
    assert queue.get(child.id, tenant_id="t1").status == JobStatus.CANCELLED

