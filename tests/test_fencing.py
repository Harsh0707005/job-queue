"""No double-processing / fencing."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# No double-processing / fencing
# ---------------------------------------------------------------------------


def test_two_workers_cannot_lease_same_job(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={"n": 1})

    first = queue.lease(tenant_id="t1", worker_id="w1")
    second = queue.lease(tenant_id="t1", worker_id="w2")

    assert first is not None
    assert second is None


def test_concurrent_lease_at_most_once(queue: JobQueue):
    n_jobs = 40
    n_workers = 12
    for i in range(n_jobs):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    claimed: list[str] = []
    lock = threading.Lock()

    def worker(wid: str) -> list[str]:
        ids: list[str] = []
        while True:
            job = queue.lease(tenant_id="t1", worker_id=wid)
            if job is None:
                break
            ids.append(str(job.id))
        return ids

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futs = [pool.submit(worker, f"w{i}") for i in range(n_workers)]
        for fut in as_completed(futs):
            with lock:
                claimed.extend(fut.result())

    assert len(claimed) == n_jobs
    assert len(set(claimed)) == n_jobs


def test_stale_worker_cannot_ack_after_reclaim(queue: JobQueue):
    """Zombie worker with an expired/reclaimed lease cannot complete the job.

    This is the fencing-token fix: even if worker A finishes work after its
    lease timed out and worker B has reclaimed, A's ack is rejected and only
    B's token can complete the job (once).
    """
    queue.enqueue(
        tenant_id="t1",
        payload={"task": "long"},
        visibility_timeout_seconds=1,
        max_attempts=5,
    )

    stale = queue.lease(
        tenant_id="t1",
        worker_id="stale",
        visibility_timeout_seconds=1,
    )
    assert stale is not None
    stale_token = stale.lease_token
    assert stale_token is not None

    time.sleep(1.2)

    # New worker reclaims — mints a new fencing token.
    live = queue.lease(
        tenant_id="t1",
        worker_id="live",
        visibility_timeout_seconds=30,
    )
    assert live is not None
    assert live.id == stale.id
    assert live.lease_token != stale_token
    assert live.leased_by == "live"

    # Stale worker tries to finish — must fail (wrong token / not holder).
    with pytest.raises(LeaseError):
        queue.ack(
            stale.id,
            tenant_id="t1",
            worker_id="stale",
            lease_token=stale_token,
        )
    with pytest.raises(LeaseError):
        queue.heartbeat(
            stale.id,
            tenant_id="t1",
            worker_id="stale",
            lease_token=stale_token,
        )

    # Live worker alone can complete; job will not be picked again.
    _ack(queue, live, "live")
    assert queue.lease(tenant_id="t1", worker_id="other") is None
    final = queue.get(live.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.COMPLETED


def test_worker_aborts_when_lease_reclaimed_mid_process(queue: JobQueue):
    """If the lease is lost mid-handler, Worker refuses to ack/nack-as-success.

    Handler checks LeaseContext and stops; only one completion path exists.
    """
    queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=1,
        max_attempts=5,
    )

    started = threading.Event()
    handler_runs = []

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="zombie",
        # Deliberately slow heartbeat so lease can expire under us.
        heartbeat_interval_seconds=10.0,
        visibility_timeout_seconds=1,
    )

    @worker.handler
    def handle(job: Job, lease: LeaseContext) -> None:
        handler_runs.append("start")
        started.set()
        # Simulate slow work while lease expires and is reclaimed.
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if lease.lease_lost.is_set():
                handler_runs.append("cancelled")
                raise LeaseError("handler aborted: lease lost")
            try:
                lease.ensure_held()
            except LeaseError:
                handler_runs.append("ensure_failed")
                raise
            time.sleep(0.1)
        handler_runs.append("finished_without_cancel")

    def run_zombie() -> None:
        with pytest.raises(LeaseError):
            worker.run_once()

    t = threading.Thread(target=run_zombie)
    t.start()
    assert started.wait(timeout=5)

    # Wait for expiry and reclaim as another worker.
    time.sleep(1.2)
    reclaimed = queue.lease(
        tenant_id="t1",
        worker_id="rescuer",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None

    t.join(timeout=10)
    assert not t.is_alive()

    # Zombie must not have completed the job; rescuer still holds it.
    assert "finished_without_cancel" not in handler_runs
    assert reclaimed.leased_by == "rescuer"
    got = queue.get(reclaimed.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.LEASED
    assert got.leased_by == "rescuer"

    _ack(queue, reclaimed, "rescuer")
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
