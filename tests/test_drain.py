"""Graceful worker drain."""

from __future__ import annotations

import threading
import time

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Graceful worker drain
# ---------------------------------------------------------------------------


def test_release_returns_job_pending_immediately(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=60,
        max_attempts=5,
        retry_backoff_base_seconds=30,
    )
    leased = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=60,
    )
    assert leased is not None and leased.lease_token is not None
    released = queue.release(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        reason="drain",
    )
    assert released.status == JobStatus.PENDING
    assert released.leased_by is None
    assert released.lease_token is None
    # Immediate reclaim by another worker (no visibility wait, no backoff).
    j2 = queue.lease(tenant_id="t1", worker_id="w2", visibility_timeout_seconds=30)
    assert j2 is not None and j2.id == job.id
    assert j2.leased_by == "w2"


def test_worker_drain_finishes_in_flight(queue: JobQueue):
    from job_queue import Worker

    started = threading.Event()
    finish = threading.Event()

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-ok",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=30,
        drain_timeout_seconds=10,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        started.set()
        # Wait until test allows finish (or lease lost).
        while not finish.is_set():
            lease.ensure_held()
            if finish.wait(0.05):
                break
        return {"ok": True}

    queue.enqueue(tenant_id="t1", payload={"n": 1})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)

    # Drain while job is running; allow handler to finish.
    def _finish_soon() -> None:
        time.sleep(0.2)
        finish.set()

    threading.Thread(target=_finish_soon, daemon=True).start()
    result = worker.drain(timeout_seconds=5)
    t.join(timeout=5)
    assert result.drained is True
    assert result.timed_out is False
    assert result.released == 0
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0


def test_worker_drain_timeout_releases_for_reclaim(queue: JobQueue):
    from job_queue import Worker

    started = threading.Event()
    # Handler blocks until lease is lost (drain timeout forces release).
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-to",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=120,
        drain_timeout_seconds=1,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        started.set()
        while not lease.lease_lost.is_set():
            time.sleep(0.05)
        # Drain force-release sets lease_lost; exit without re-raising so the
        # worker thread shuts down cleanly.
        return {"released": True}

    job = queue.enqueue(tenant_id="t1", payload={"long": True})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    result = worker.drain(timeout_seconds=0.3)
    t.join(timeout=5)
    assert result.timed_out is True
    assert result.released >= 1
    # Another worker can claim immediately (not wait 120s VT).
    j2 = queue.lease(
        tenant_id="t1",
        worker_id="other",
        visibility_timeout_seconds=30,
    )
    assert j2 is not None and j2.id == job.id
    assert j2.leased_by == "other"
    _ack(queue, j2, "other")


def test_worker_drain_stops_new_leases(queue: JobQueue):
    from job_queue import Worker

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-nolease",
        poll_interval_seconds=0.05,
    )

    @worker.handler
    def process(job: Job) -> None:
        return None

    worker._draining.set()  # type: ignore[attr-defined]
    queue.enqueue(tenant_id="t1", payload={})
    jobs = worker.run_batch_once()
    assert jobs == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_worker_drain_releases_entire_batch_not_only_active(queue: JobQueue):
    """Bug: only the running job was released; rest of batch stayed leased."""
    from job_queue import Worker

    started = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-batch",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=120,
        batch_size=4,
        drain_timeout_seconds=30,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        started.set()
        while not lease.lease_lost.is_set():
            time.sleep(0.05)
        return {"released": True}

    ids = []
    for i in range(4):
        j = queue.enqueue(tenant_id="t1", payload={"i": i})
        ids.append(j.id)

    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    time.sleep(0.3)
    with worker._lease_lock:  # type: ignore[attr-defined]
        held = len(worker._held)  # type: ignore[attr-defined]
        active = len(worker._active)  # type: ignore[attr-defined]
    assert held >= 2, f"expected full batch held, held={held} active={active}"
    assert active == 1

    # Long drain timeout: only *queued* batch jobs should free immediately;
    # active stays running until we force-timeout later.
    drain_done = threading.Event()
    result_box: list = []

    def _do_drain() -> None:
        result_box.append(worker.drain(timeout_seconds=8))
        drain_done.set()

    threading.Thread(target=_do_drain, daemon=True).start()
    deadline = time.time() + 2.0
    pending_n = 0
    while time.time() < deadline:
        pending_n = queue.count(tenant_id="t1", status=JobStatus.PENDING)
        if pending_n >= 3:
            break
        time.sleep(0.05)
    assert pending_n >= 3, (
        f"queued batch jobs not released promptly; pending={pending_n}"
    )
    # Active still leased until timeout / cancel.
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 1

    # Finish active via force path (short remaining wait on drain thread).
    # Trigger timeout path by waiting out the 8s drain, or poke force:
    with worker._lease_lock:  # type: ignore[attr-defined]
        for a in list(worker._active.values()):  # type: ignore[attr-defined]
            a.lease_lost.set()
    assert drain_done.wait(12)
    result = result_box[0]
    assert result.released >= 3
    t.join(timeout=5)
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0

    claimed = 0
    for _ in range(4):
        j = queue.lease(
            tenant_id="t1", worker_id="other", visibility_timeout_seconds=30
        )
        if j is None:
            break
        claimed += 1
        _ack(queue, j, "other")
    assert claimed >= 3


def test_worker_drain_timeout_releases_active_and_batch(queue: JobQueue):
    from job_queue import Worker

    started = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-all",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=120,
        batch_size=3,
        drain_timeout_seconds=0.4,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        started.set()
        while not lease.lease_lost.is_set():
            time.sleep(0.05)
        return {"ok": True}

    for i in range(3):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    time.sleep(0.25)
    result = worker.drain(timeout_seconds=0.3)
    t.join(timeout=5)
    assert result.timed_out is True
    # All batch members released (1 active + 2 queued at drain start).
    assert result.released >= 3
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 3


def test_worker_drain_idle_is_noop(queue: JobQueue):
    """Drain with nothing leased completes immediately."""
    from job_queue import Worker

    worker = Worker(queue, tenant_id="t1", worker_id="drain-idle")

    @worker.handler
    def process(job: Job) -> None:
        return None

    result = worker.drain(timeout_seconds=2)
    assert result.drained is True
    assert result.timed_out is False
    assert result.in_flight_at_start == 0
    assert result.released == 0
    assert result.wait_seconds < 1.0


def test_worker_drain_before_any_job_starts_releases_full_batch(queue: JobQueue):
    """If drain wins before the first handler starts, free the entire batch."""
    from job_queue import Worker

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-prestart",
        batch_size=3,
        visibility_timeout_seconds=120,
        drain_timeout_seconds=5,
    )
    # Block the handler from ever starting meaningful work.
    gate = threading.Event()
    entered = threading.Event()

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        entered.set()
        gate.wait(timeout=10)
        return {"ok": True}

    for i in range(3):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    # Manually lease a batch and register held (simulates post-lease pre-loop).
    jobs = queue.lease_batch(
        tenant_id="t1",
        worker_id="drain-prestart",
        visibility_timeout_seconds=120,
        limit=3,
    )
    assert len(jobs) == 3
    with worker._lease_lock:  # type: ignore[attr-defined]
        for j in jobs:
            worker._held[j.id] = j  # type: ignore[attr-defined]
        # No active yet.
        assert len(worker._active) == 0  # type: ignore[attr-defined]

    result = worker.drain(timeout_seconds=2)
    assert result.drained is True
    assert result.released == 3
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 3
    # Reclaim all three without waiting visibility.
    for _ in range(3):
        j = queue.lease(
            tenant_id="t1", worker_id="other", visibility_timeout_seconds=30
        )
        assert j is not None
        _ack(queue, j, "other")


def test_worker_stop_releases_queued_batch_not_active(queue: JobQueue):
    """stop() also frees leased-but-not-running batch members immediately."""
    from job_queue import Worker

    started = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="stop-batch",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=120,
        batch_size=3,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        started.set()
        while not lease.lease_lost.is_set():
            time.sleep(0.05)
        return {"ok": True}

    for i in range(3):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    time.sleep(0.25)
    with worker._lease_lock:  # type: ignore[attr-defined]
        assert len(worker._held) >= 2  # type: ignore[attr-defined]
        assert len(worker._active) == 1  # type: ignore[attr-defined]

    worker.stop()
    # Queued batch members freed; active still leased until force path / timeout.
    deadline = time.time() + 2.0
    pending_n = 0
    while time.time() < deadline:
        pending_n = queue.count(tenant_id="t1", status=JobStatus.PENDING)
        if pending_n >= 2:
            break
        time.sleep(0.05)
    assert pending_n >= 2, f"stop did not free batch leftovers; pending={pending_n}"
    # Force free active so the worker thread can exit cleanly.
    with worker._lease_lock:  # type: ignore[attr-defined]
        for a in list(worker._active.values()):  # type: ignore[attr-defined]
            a.lease_lost.set()
            a.stop_hb.set()
        for j in list(worker._held.values()):  # type: ignore[attr-defined]
            try:
                queue.release(
                    j.id,
                    tenant_id=j.tenant_id,
                    worker_id="stop-batch",
                    lease_token=j.lease_token,  # type: ignore[arg-type]
                    reason="test_cleanup",
                )
            except LeaseError:
                pass
        worker._held.clear()  # type: ignore[attr-defined]
        worker._active.clear()  # type: ignore[attr-defined]
    t.join(timeout=5)


def test_worker_drain_mid_batch_does_not_start_next_job(queue: JobQueue):
    """After releasing leftovers, the run loop must not start another batch job."""
    from job_queue import Worker

    started_ids: list = []
    lock = threading.Lock()
    started = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drain-mid",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=120,
        batch_size=4,
        drain_timeout_seconds=10,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        with lock:
            started_ids.append(job.id)
        started.set()
        while not lease.lease_lost.is_set():
            time.sleep(0.05)
        return {"ok": True}

    for i in range(4):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    time.sleep(0.3)
    # Drain with generous timeout so active can finish if we signal it.
    drain_t = threading.Thread(
        target=lambda: worker.drain(timeout_seconds=5), daemon=True
    )
    drain_t.start()
    time.sleep(0.5)
    with lock:
        n_started = len(started_ids)
    # Only the first job of the batch should have started.
    assert n_started == 1, f"started {n_started} jobs; expected only active"
    with worker._lease_lock:  # type: ignore[attr-defined]
        for a in list(worker._active.values()):  # type: ignore[attr-defined]
            a.lease_lost.set()
    drain_t.join(timeout=8)
    t.join(timeout=5)
    with lock:
        assert len(started_ids) == 1
