"""Functional tests for the multi-tenant job queue.

Uses a disposable Docker Postgres (see conftest.py). Containers are deleted
after the test session finishes.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker


def _ack(queue: JobQueue, job: Job, worker_id: str, result=None) -> Job:
    assert job.lease_token is not None
    return queue.ack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        result=result,
    )


def _hb(queue: JobQueue, job: Job, worker_id: str, **kw) -> Job:
    assert job.lease_token is not None
    return queue.heartbeat(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        **kw,
    )


def _nack(queue: JobQueue, job: Job, worker_id: str, **kw) -> Job:
    assert job.lease_token is not None
    return queue.nack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=worker_id,
        lease_token=job.lease_token,
        **kw,
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


# ---------------------------------------------------------------------------
# Multi-tenant isolation
# ---------------------------------------------------------------------------


def test_tenants_are_isolated(queue: JobQueue):
    a = queue.enqueue(tenant_id="alpha", payload={"who": "a"})
    b = queue.enqueue(tenant_id="beta", payload={"who": "b"})

    leased_a = queue.lease(tenant_id="alpha", worker_id="w1")
    leased_b = queue.lease(tenant_id="beta", worker_id="w1")

    assert leased_a is not None and leased_a.id == a.id
    assert leased_b is not None and leased_b.id == b.id
    assert queue.lease(tenant_id="alpha", worker_id="w2") is None


def test_cross_tenant_get_returns_none(queue: JobQueue):
    job = queue.enqueue(tenant_id="alpha", payload={"secret": True})
    assert queue.get(job.id, tenant_id="alpha") is not None
    assert queue.get(job.id, tenant_id="beta") is None


def test_cross_tenant_ack_heartbeat_nack_rejected(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={})
    job = queue.lease(tenant_id="alpha", worker_id="w1")
    assert job is not None
    assert job.lease_token is not None

    with pytest.raises(LeaseError):
        queue.ack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError):
        queue.heartbeat(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError):
        queue.nack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
            error="nope",
        )

    # Original tenant can still complete.
    _ack(queue, job, "w1")
    assert queue.get(job.id, tenant_id="alpha") is not None
    final = queue.get(job.id, tenant_id="alpha")
    assert final is not None
    assert final.status == JobStatus.COMPLETED


def test_queue_name_isolation(queue: JobQueue):
    queue.enqueue(tenant_id="t1", queue_name="email", payload={"t": "e"})
    queue.enqueue(tenant_id="t1", queue_name="sms", payload={"t": "s"})

    email = queue.lease(tenant_id="t1", worker_id="w1", queue_name="email")
    assert email is not None
    assert email.payload["t"] == "e"

    sms = queue.lease(tenant_id="t1", worker_id="w1", queue_name="sms")
    assert sms is not None
    assert sms.payload["t"] == "s"


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


# ---------------------------------------------------------------------------
# Worker helper
# ---------------------------------------------------------------------------


def test_worker_processes_and_acks(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={"x": 1})
    queue.enqueue(tenant_id="t1", payload={"x": 2})

    seen: list[int] = []
    worker = Worker(queue, tenant_id="t1", worker_id="worker-a")

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.payload["x"])

    n = worker.run(max_jobs=2, stop_when_idle=True)
    assert n == 2
    assert sorted(seen) == [1, 2]
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 2


def test_worker_nack_on_handler_error(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={}, max_attempts=3)
    worker = Worker(queue, tenant_id="t1", worker_id="worker-b")

    @worker.handler
    def handle(job: Job) -> None:
        raise RuntimeError("handler blew up")

    with pytest.raises(RuntimeError, match="handler blew up"):
        worker.run_once()

    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_worker_heartbeat_keeps_lease(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={"sleep": 3},
        visibility_timeout_seconds=2,
    )

    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="hb-worker",
        heartbeat_interval_seconds=0.5,
        visibility_timeout_seconds=2,
    )

    @worker.handler
    def handle(job: Job) -> None:
        time.sleep(3.0)

    worker.run_once()
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1


def test_get_unknown_job(queue: JobQueue):
    assert queue.get(uuid4(), tenant_id="t1") is None


# ---------------------------------------------------------------------------
# Delayed jobs
# ---------------------------------------------------------------------------


def test_delayed_job_not_claimable_until_available_at(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    delayed = queue.enqueue(
        tenant_id="t1",
        payload={"when": "later"},
        available_at=now + timedelta(seconds=2),
    )
    assert delayed.available_at > now

    # Immediate job remains claimable while the delayed one waits.
    immediate = queue.enqueue(tenant_id="t1", payload={"when": "now"})

    first = queue.lease(tenant_id="t1", worker_id="w1")
    assert first is not None
    assert first.id == immediate.id
    _ack(queue, first, "w1")

    # Delayed job still not due.
    assert queue.lease(tenant_id="t1", worker_id="w1") is None

    time.sleep(2.2)

    later = queue.lease(tenant_id="t1", worker_id="w1")
    assert later is not None
    assert later.id == delayed.id
    _ack(queue, later, "w1")


def test_delay_seconds_helper(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"d": 1},
        delay_seconds=2,
    )
    assert queue.lease(tenant_id="t1", worker_id="w1") is None
    time.sleep(2.2)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == job.id


def test_available_at_and_delay_seconds_mutually_exclusive(queue: JobQueue):
    from datetime import datetime, timezone

    with pytest.raises(ValueError, match="only one of available_at or delay_seconds"):
        queue.enqueue(
            tenant_id="t1",
            payload={},
            available_at=datetime.now(timezone.utc),
            delay_seconds=5,
        )


def test_delayed_job_does_not_block_ready_jobs(queue: JobQueue):
    """A future high-priority delayed job must not starve ready work."""
    from datetime import datetime, timedelta, timezone

    queue.enqueue(
        tenant_id="t1",
        payload={"delayed": True},
        priority=100,
        available_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    ready = queue.enqueue(
        tenant_id="t1",
        payload={"ready": True},
        priority=1,
    )
    claimed = queue.lease(tenant_id="t1", worker_id="w1")
    assert claimed is not None
    assert claimed.id == ready.id


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def test_dedupe_key_returns_existing_job(queue: JobQueue):
    first = queue.enqueue(
        tenant_id="t1",
        payload={"n": 1},
        dedupe_key="invoice-42",
    )
    second = queue.enqueue(
        tenant_id="t1",
        payload={"n": 2},  # different payload — still deduped
        dedupe_key="invoice-42",
    )
    assert second.id == first.id
    assert second.payload == first.payload  # original preserved
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_dedupe_key_scoped_by_tenant_and_queue(queue: JobQueue):
    a = queue.enqueue(tenant_id="alpha", payload={}, dedupe_key="same")
    b = queue.enqueue(tenant_id="beta", payload={}, dedupe_key="same")
    c = queue.enqueue(
        tenant_id="alpha",
        payload={},
        dedupe_key="same",
        queue_name="other",
    )
    assert a.id != b.id
    assert a.id != c.id
    assert b.id != c.id


def test_dedupe_allows_reenqueue_after_completion(queue: JobQueue):
    first = queue.enqueue(tenant_id="t1", payload={"v": 1}, dedupe_key="k1")
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == first.id
    _ack(queue, leased, "w1")

    second = queue.enqueue(tenant_id="t1", payload={"v": 2}, dedupe_key="k1")
    assert second.id != first.id
    assert second.payload == {"v": 2}
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_dedupe_applies_while_job_is_leased(queue: JobQueue):
    first = queue.enqueue(tenant_id="t1", payload={}, dedupe_key="live")
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    again = queue.enqueue(tenant_id="t1", payload={"x": 1}, dedupe_key="live")
    assert again.id == first.id
    assert again.status == JobStatus.LEASED


def test_concurrent_dedupe_enqueue_single_row(queue: JobQueue):
    """Many threads enqueue the same key; only one active job exists."""
    results: list[str] = []
    lock = threading.Lock()

    def go() -> None:
        job = queue.enqueue(
            tenant_id="t1",
            payload={"t": threading.current_thread().name},
            dedupe_key="hot-key",
        )
        with lock:
            results.append(str(job.id))

    threads = [threading.Thread(target=go) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 20
    assert len(set(results)) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_empty_dedupe_key_rejected(queue: JobQueue):
    with pytest.raises(ValueError, match="dedupe_key"):
        queue.enqueue(tenant_id="t1", payload={}, dedupe_key="")


# ---------------------------------------------------------------------------
# Per-tenant concurrency limits
# ---------------------------------------------------------------------------


def test_concurrency_limit_caps_leased_jobs(queue: JobQueue):
    queue.set_concurrency_limit("t1", 2)
    assert queue.get_concurrency_limit("t1") == 2

    for i in range(5):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    j1 = queue.lease(tenant_id="t1", worker_id="w1")
    j2 = queue.lease(tenant_id="t1", worker_id="w2")
    j3 = queue.lease(tenant_id="t1", worker_id="w3")

    assert j1 is not None and j2 is not None
    assert j3 is None  # at cap
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 2

    _ack(queue, j1, "w1")
    j4 = queue.lease(tenant_id="t1", worker_id="w4")
    assert j4 is not None
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 2


def test_concurrency_limit_is_per_tenant(queue: JobQueue):
    queue.set_concurrency_limit("alpha", 1)
    queue.set_concurrency_limit("beta", 1)

    queue.enqueue(tenant_id="alpha", payload={"t": "a1"})
    queue.enqueue(tenant_id="alpha", payload={"t": "a2"})
    queue.enqueue(tenant_id="beta", payload={"t": "b1"})

    a1 = queue.lease(tenant_id="alpha", worker_id="w1")
    a2 = queue.lease(tenant_id="alpha", worker_id="w2")
    b1 = queue.lease(tenant_id="beta", worker_id="w1")

    assert a1 is not None
    assert a2 is None  # alpha at cap
    assert b1 is not None  # beta independent


def test_concurrency_limit_tenant_wide_across_queues(queue: JobQueue):
    queue.set_concurrency_limit("t1", 1)
    queue.enqueue(tenant_id="t1", queue_name="email", payload={"q": "e"})
    queue.enqueue(tenant_id="t1", queue_name="sms", payload={"q": "s"})

    e = queue.lease(tenant_id="t1", worker_id="w1", queue_name="email")
    s = queue.lease(tenant_id="t1", worker_id="w2", queue_name="sms")
    assert e is not None
    assert s is None  # shared tenant cap


def test_clear_concurrency_limit(queue: JobQueue):
    queue.set_concurrency_limit("t1", 1)
    for _ in range(3):
        queue.enqueue(tenant_id="t1", payload={})
    assert queue.lease(tenant_id="t1", worker_id="w1") is not None
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    queue.set_concurrency_limit("t1", None)
    assert queue.get_concurrency_limit("t1") is None
    assert queue.lease(tenant_id="t1", worker_id="w2") is not None


def test_concurrent_lease_respects_tenant_cap(queue: JobQueue):
    """Many workers race; leased count never exceeds the tenant cap."""
    queue.set_concurrency_limit("t1", 3)
    for i in range(30):
        queue.enqueue(tenant_id="t1", payload={"i": i})

    barrier = threading.Barrier(12)
    leased_ids: list[str] = []
    lock = threading.Lock()

    def worker(wid: str) -> None:
        barrier.wait()
        job = queue.lease(tenant_id="t1", worker_id=wid)
        if job is not None:
            with lock:
                leased_ids.append(str(job.id))

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(leased_ids) == 3
    assert len(set(leased_ids)) == 3
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 3


def test_at_limit_can_still_reclaim_expired_lease(queue: JobQueue):
    queue.set_concurrency_limit("t1", 1)
    queue.enqueue(
        tenant_id="t1",
        payload={"first": True},
        visibility_timeout_seconds=1,
    )
    queue.enqueue(tenant_id="t1", payload={"second": True})

    first = queue.lease(
        tenant_id="t1",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert first is not None
    assert queue.lease(tenant_id="t1", worker_id="other") is None

    time.sleep(1.2)

    # Reclaim frees the slot to the new worker (still one leased job).
    reclaimed = queue.lease(tenant_id="t1", worker_id="rescuer")
    assert reclaimed is not None
    assert reclaimed.id == first.id
    assert reclaimed.leased_by == "rescuer"
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 1


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


def test_cancel_leased_job_rejected(queue: JobQueue):
    from job_queue import CancelError

    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    with pytest.raises(CancelError, match="only pending") as exc_info:
        queue.cancel(leased.id, tenant_id="t1")
    assert exc_info.value.job is not None
    assert exc_info.value.job.status == JobStatus.LEASED

    # Still leased and completable.
    _ack(queue, leased, "w1")
    assert queue.get(leased.id, tenant_id="t1").status == JobStatus.COMPLETED


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


# ---------------------------------------------------------------------------
# Dead letter inspect + replay
# ---------------------------------------------------------------------------


def _drive_to_dead(queue: JobQueue, *, tenant_id: str, payload: dict, max_attempts: int = 1, **enq_kw) -> Job:
    """Enqueue a job and force it into dead via repeated lease+nack."""
    enq_kw.setdefault("retry_backoff_base_seconds", 0)
    job = queue.enqueue(
        tenant_id=tenant_id,
        payload=payload,
        max_attempts=max_attempts,
        **enq_kw,
    )
    for i in range(max_attempts):
        leased = queue.lease(tenant_id=tenant_id, worker_id=f"w-dead-{i}", queue_name=job.queue_name)
        assert leased is not None
        assert leased.id == job.id
        result = _nack(queue, leased, f"w-dead-{i}", error=f"fail-{i}", requeue=True)
        if i + 1 < max_attempts:
            assert result.status == JobStatus.PENDING
        else:
            assert result.status == JobStatus.DEAD
            return result
    raise AssertionError("job never reached dead")


def test_list_dead_letters_empty(queue: JobQueue):
    assert queue.list_dead_letters(tenant_id="t1") == []


def test_list_dead_letters_returns_dead_jobs(queue: JobQueue):
    d1 = _drive_to_dead(queue, tenant_id="t1", payload={"n": 1})
    d2 = _drive_to_dead(queue, tenant_id="t1", payload={"n": 2})

    # Non-dead noise should not appear.
    queue.enqueue(tenant_id="t1", payload={"live": True})

    dead = queue.list_dead_letters(tenant_id="t1")
    ids = {j.id for j in dead}
    assert ids == {d1.id, d2.id}
    assert all(j.status == JobStatus.DEAD for j in dead)
    assert all(j.last_error is not None for j in dead)


def test_list_dead_letters_tenant_isolation(queue: JobQueue):
    a = _drive_to_dead(queue, tenant_id="alpha", payload={"t": "a"})
    b = _drive_to_dead(queue, tenant_id="beta", payload={"t": "b"})

    only_a = queue.list_dead_letters(tenant_id="alpha")
    only_b = queue.list_dead_letters(tenant_id="beta")
    assert [j.id for j in only_a] == [a.id]
    assert [j.id for j in only_b] == [b.id]


def test_list_dead_letters_filter_queue_name_and_paging(queue: JobQueue):
    email = _drive_to_dead(
        queue, tenant_id="t1", payload={"q": "e"}, queue_name="email"
    )
    _drive_to_dead(queue, tenant_id="t1", payload={"q": "s"}, queue_name="sms")

    only_email = queue.list_dead_letters(tenant_id="t1", queue_name="email")
    assert len(only_email) == 1
    assert only_email[0].id == email.id

    # Create several dead jobs then page.
    for i in range(5):
        _drive_to_dead(queue, tenant_id="t2", payload={"i": i})
    page1 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=0)
    page2 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=2)
    page3 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=4)
    assert len(page1) == 2
    assert len(page2) == 2
    assert len(page3) == 1
    all_ids = [j.id for j in page1 + page2 + page3]
    assert len(all_ids) == len(set(all_ids)) == 5


def test_replay_dead_letter_makes_job_claimable(queue: JobQueue):
    from job_queue import ReplayError  # noqa: F401

    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"task": "retry-me"}, max_attempts=2
    )
    assert dead.attempts == 2
    assert dead.last_error is not None

    replayed = queue.replay_dead_letter(dead.id, tenant_id="t1")
    assert replayed.status == JobStatus.PENDING
    assert replayed.attempts == 0
    assert replayed.leased_by is None
    assert replayed.lease_token is None
    # Forensic error kept by default.
    assert replayed.last_error == dead.last_error

    leased = queue.lease(tenant_id="t1", worker_id="rescuer")
    assert leased is not None
    assert leased.id == dead.id
    assert leased.attempts == 1
    _ack(queue, leased, "rescuer")
    assert queue.get(dead.id, tenant_id="t1").status == JobStatus.COMPLETED


def test_replay_dead_letter_clear_error_and_custom_max_attempts(queue: JobQueue):
    dead = _drive_to_dead(queue, tenant_id="t1", payload={}, max_attempts=1)
    replayed = queue.replay_dead_letter(
        dead.id,
        tenant_id="t1",
        clear_error=True,
        max_attempts=5,
    )
    assert replayed.last_error is None
    assert replayed.max_attempts == 5
    assert replayed.attempts == 0


def test_replay_dead_letter_with_delay(queue: JobQueue):
    dead = _drive_to_dead(queue, tenant_id="t1", payload={"later": True})
    replayed = queue.replay_dead_letter(
        dead.id, tenant_id="t1", delay_seconds=2
    )
    assert replayed.status == JobStatus.PENDING
    assert queue.lease(tenant_id="t1", worker_id="w1") is None

    time.sleep(2.2)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == dead.id


def test_replay_rejects_non_dead_and_cross_tenant(queue: JobQueue):
    from job_queue import ReplayError

    pending = queue.enqueue(tenant_id="t1", payload={})
    with pytest.raises(ReplayError, match="not dead-lettered"):
        queue.replay_dead_letter(pending.id, tenant_id="t1")

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    with pytest.raises(ReplayError, match="not dead-lettered"):
        queue.replay_dead_letter(leased.id, tenant_id="t1")

    dead = _drive_to_dead(queue, tenant_id="alpha", payload={})
    with pytest.raises(ReplayError, match="not found"):
        queue.replay_dead_letter(dead.id, tenant_id="beta")

    with pytest.raises(ReplayError, match="not found"):
        queue.replay_dead_letter(uuid4(), tenant_id="t1")


def test_replay_dedupe_conflict(queue: JobQueue):
    from job_queue import ReplayError

    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"v": 1}, dedupe_key="shared"
    )
    # Active job holds the same dedupe key.
    live = queue.enqueue(
        tenant_id="t1", payload={"v": 2}, dedupe_key="shared"
    )
    assert live.status == JobStatus.PENDING

    with pytest.raises(ReplayError, match="dedupe_key"):
        queue.replay_dead_letter(dead.id, tenant_id="t1")

    # After live completes, replay succeeds.
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    replayed = queue.replay_dead_letter(dead.id, tenant_id="t1")
    assert replayed.status == JobStatus.PENDING


def test_replay_dead_letters_bulk_by_ids_and_filter(queue: JobQueue):
    d1 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 1}, queue_name="a")
    d2 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 2}, queue_name="a")
    d3 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 3}, queue_name="b")

    # Bulk by explicit ids (include a non-dead id — skipped).
    pending = queue.enqueue(tenant_id="t1", payload={"skip": True})
    replayed = queue.replay_dead_letters(
        tenant_id="t1",
        job_ids=[d1.id, d2.id, pending.id],
    )
    assert {j.id for j in replayed} == {d1.id, d2.id}
    assert all(j.status == JobStatus.PENDING for j in replayed)

    # d3 still dead; replay whole queue "b"
    only_b = queue.replay_dead_letters(tenant_id="t1", queue_name="b")
    assert len(only_b) == 1
    assert only_b[0].id == d3.id

    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) >= 3


def test_replay_dead_letters_limit(queue: JobQueue):
    for i in range(4):
        _drive_to_dead(queue, tenant_id="t1", payload={"i": i})

    first_batch = queue.replay_dead_letters(tenant_id="t1", limit=2)
    assert len(first_batch) == 2
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 2

    second_batch = queue.replay_dead_letters(tenant_id="t1", limit=10)
    assert len(second_batch) == 2
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 0


def test_worker_processes_replayed_dead_letter(queue: JobQueue):
    """End-to-end: dead → list → replay → worker completes."""
    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"x": 99}, max_attempts=1
    )
    listed = queue.list_dead_letters(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].id == dead.id

    queue.replay_dead_letter(dead.id, tenant_id="t1", clear_error=True)

    seen: list[int] = []
    worker = Worker(queue, tenant_id="t1", worker_id="dlq-worker")

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.payload["x"])

    assert worker.run_once() is not None
    assert seen == [99]
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
    assert queue.list_dead_letters(tenant_id="t1") == []


def test_replay_dedupe_race_with_concurrent_enqueue(queue: JobQueue):
    """Replay vs enqueue on the same dedupe key must not leak unique_violation.

    Under load, either replay wins (dead → pending) or enqueue wins (new/existing
    active row) and losers get clean outcomes (ReplayError / existing job).
    Never more than one active job for the key, and never a raw DB error.
    """
    from job_queue import ReplayError
    from psycopg.errors import UniqueViolation

    dead = _drive_to_dead(
        queue,
        tenant_id="t1",
        payload={"origin": "dead"},
        dedupe_key="race-key",
        max_attempts=1,
    )
    assert dead.status == JobStatus.DEAD

    n_replay = 12
    n_enqueue = 12
    barrier = threading.Barrier(n_replay + n_enqueue)
    outcomes: list[tuple[str, object]] = []
    raw_errors: list[BaseException] = []
    lock = threading.Lock()

    def record(kind: str, value: object) -> None:
        with lock:
            outcomes.append((kind, value))

    def record_error(exc: BaseException) -> None:
        with lock:
            raw_errors.append(exc)

    def replayer() -> None:
        barrier.wait()
        try:
            job = queue.replay_dead_letter(dead.id, tenant_id="t1")
            record("replay_ok", job)
        except ReplayError as exc:
            record("replay_err", str(exc))
        except Exception as exc:  # noqa: BLE001 — we assert none of these
            record_error(exc)

    def enqueuer(i: int) -> None:
        barrier.wait()
        try:
            job = queue.enqueue(
                tenant_id="t1",
                payload={"origin": f"enq-{i}"},
                dedupe_key="race-key",
            )
            record("enq_ok", job)
        except Exception as exc:  # noqa: BLE001
            record_error(exc)

    threads = [threading.Thread(target=replayer) for _ in range(n_replay)]
    threads += [
        threading.Thread(target=enqueuer, args=(i,)) for i in range(n_enqueue)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    # No raw unique violations or other unexpected DB errors.
    assert raw_errors == [], f"unexpected errors: {raw_errors!r}"
    assert not any(isinstance(e, UniqueViolation) for e in raw_errors)

    assert len(outcomes) == n_replay + n_enqueue

    # At most one successful replay (job can only leave dead once).
    replay_oks = [j for k, j in outcomes if k == "replay_ok"]
    assert len(replay_oks) <= 1

    # Exactly one active (pending/leased) job for the dedupe key.
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n, array_agg(id) AS ids
            FROM jobs
            WHERE tenant_id = %s
              AND dedupe_key = %s
              AND status IN ('pending', 'leased')
            """,
            ("t1", "race-key"),
        ).fetchone()
    assert row is not None
    assert int(row["n"]) == 1, f"expected 1 active job, got {row}"

    # Enqueues always return some job (either newly inserted or the active one).
    enq_oks = [j for k, j in outcomes if k == "enq_ok"]
    assert len(enq_oks) == n_enqueue
    active_id = enq_oks[0].id
    assert all(j.id == active_id for j in enq_oks)

    # If replay succeeded, it must be that same active job.
    if replay_oks:
        assert replay_oks[0].id == active_id
        assert replay_oks[0].id == dead.id
    else:
        # Enqueue won the race: dead job stays dead (or was never activated).
        still = queue.get(dead.id, tenant_id="t1")
        assert still is not None
        # Either still dead, or if somehow replayed only one active exists above.
        assert still.status in (JobStatus.DEAD, JobStatus.PENDING, JobStatus.LEASED)


# ---------------------------------------------------------------------------
# Recurring schedules
# ---------------------------------------------------------------------------


def test_create_list_get_schedule(queue: JobQueue):
    from job_queue import Schedule

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=30,
        payload={"task": "heartbeat-check"},
        queue_name="ops",
        name="ops-heartbeat",
        priority=5,
        max_attempts=2,
        visibility_timeout_seconds=45,
    )
    assert isinstance(sched, Schedule)
    assert sched.enabled is True
    assert sched.interval_seconds == 30
    assert sched.queue_name == "ops"
    assert sched.payload == {"task": "heartbeat-check"}
    assert sched.name == "ops-heartbeat"

    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.id == sched.id
    assert queue.get_schedule(sched.id, tenant_id="other") is None

    listed = queue.list_schedules(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].id == sched.id


def test_process_due_schedules_enqueues_job_with_metadata(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 7},
        queue_name="metrics",
        priority=9,
        max_attempts=4,
        visibility_timeout_seconds=20,
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    fire = fires[0]
    assert fire.schedule.id == sched.id
    assert fire.schedule.last_enqueued_at is not None
    assert fire.schedule.next_run_at > fire.schedule.last_enqueued_at
    assert fire.job.tenant_id == "t1"
    assert fire.job.queue_name == "metrics"
    assert fire.job.payload == {"n": 7}
    assert fire.job.priority == 9
    assert fire.job.max_attempts == 4
    assert fire.job.visibility_timeout_seconds == 20
    assert fire.job.status == JobStatus.PENDING

    leased = queue.lease(tenant_id="t1", worker_id="w1", queue_name="metrics")
    assert leased is not None
    assert leased.id == fire.job.id
    _ack(queue, leased, "w1")


def test_schedule_does_not_double_fire_until_interval_elapses(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=2,
        payload={"tick": True},
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    # Complete so a later tick can enqueue (no-overlap policy).
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    second = queue.process_due_schedules(tenant_id="t1")
    assert second == []  # interval not elapsed

    time.sleep(2.2)
    third = queue.process_due_schedules(tenant_id="t1")
    assert len(third) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_pause_and_resume_schedule(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"x": 1},
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    paused = queue.pause_schedule(sched.id, tenant_id="t1")
    assert paused.enabled is False
    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []

    resumed = queue.resume_schedule(sched.id, tenant_id="t1")
    assert resumed.enabled is True
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1


def test_delete_schedule_stops_future_fires(queue: JobQueue):
    from job_queue import ScheduleError

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={},
    )
    assert queue.delete_schedule(sched.id, tenant_id="t1") is True
    assert queue.get_schedule(sched.id, tenant_id="t1") is None
    assert queue.delete_schedule(sched.id, tenant_id="t1") is False

    time.sleep(1.1)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0

    with pytest.raises(ScheduleError):
        queue.pause_schedule(sched.id, tenant_id="t1")


def test_schedule_name_unique_per_tenant(queue: JobQueue):
    from job_queue import ScheduleError

    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=10,
        payload={},
        name="daily",
    )
    with pytest.raises(ScheduleError, match="already exists"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=10,
            payload={},
            name="daily",
        )
    # Same name other tenant is fine.
    other = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=10,
        payload={},
        name="daily",
    )
    assert other.tenant_id == "t2"


def test_schedule_tenant_isolation_on_process(queue: JobQueue):
    queue.create_schedule(tenant_id="alpha", interval_seconds=60, payload={"t": "a"})
    queue.create_schedule(tenant_id="beta", interval_seconds=60, payload={"t": "b"})

    alpha_fires = queue.process_due_schedules(tenant_id="alpha")
    assert len(alpha_fires) == 1
    assert alpha_fires[0].job.payload == {"t": "a"}
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 0

    beta_fires = queue.process_due_schedules(tenant_id="beta")
    assert len(beta_fires) == 1


def test_concurrent_process_due_schedules_single_fire(queue: JobQueue):
    """Many tickers race; each due schedule fires exactly once per window."""
    n_schedules = 5
    for i in range(n_schedules):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=30,
            payload={"i": i},
            name=f"s-{i}",
        )

    barrier = threading.Barrier(8)
    all_fires: list = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def ticker() -> None:
        barrier.wait()
        try:
            fires = queue.process_due_schedules(tenant_id="t1", limit=100)
            with lock:
                all_fires.extend(fires)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=ticker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    assert errors == []
    assert len(all_fires) == n_schedules
    job_ids = [f.job.id for f in all_fires]
    assert len(set(job_ids)) == n_schedules
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == n_schedules

    # Not due again immediately.
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_schedule_runner_fires_and_stop(queue: JobQueue):
    from job_queue import ScheduleRunner

    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"runner": True},
    )
    seen: list = []
    runner = ScheduleRunner(
        queue,
        tenant_id="t1",
        poll_interval_seconds=0.2,
        on_fire=lambda f: seen.append(f.job.id),
    )

    # Background run briefly.
    t = threading.Thread(target=runner.run, daemon=True)
    t.start()
    time.sleep(1.5)
    runner.stop()
    t.join(timeout=5)
    assert not t.is_alive()
    assert len(seen) >= 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) >= 1


def test_schedule_start_at_future_not_due_yet(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    future = datetime.now(timezone.utc) + timedelta(seconds=3)
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"later": True},
        start_at=future,
    )
    assert queue.process_due_schedules(tenant_id="t1") == []
    time.sleep(3.2)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1


def test_schedule_skips_tick_while_previous_job_pending(queue: JobQueue):
    """Slow work: interval elapses but prior job still pending → no overlap."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"work": "slow"},
        name="slow-job",
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    first_job = first[0].job
    assert first_job.schedule_id == sched.id
    assert first_job.status == JobStatus.PENDING

    time.sleep(1.2)
    # Due again, but previous still pending.
    skipped = queue.process_due_schedules(tenant_id="t1")
    assert skipped == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0

    # Still only the original job.
    pending = queue.lease(tenant_id="t1", worker_id="w1")
    assert pending is not None
    assert pending.id == first_job.id


def test_schedule_skips_tick_while_previous_job_leased(queue: JobQueue):
    """Worker holds lease longer than interval → no second enqueue."""
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"work": "held"},
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1

    leased = queue.lease(tenant_id="t1", worker_id="slow-worker")
    assert leased is not None
    assert leased.status == JobStatus.LEASED

    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 1

    _ack(queue, leased, "slow-worker")
    # Schedule remains due from skipped ticks → fires once free.
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].job.id != leased.id
    assert fires[0].job.schedule_id == first[0].schedule.id


def test_schedule_fires_again_only_after_prior_completes(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
    )
    f1 = queue.process_due_schedules(tenant_id="t1")
    assert len(f1) == 1

    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []

    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    _ack(queue, j, "w1")

    f2 = queue.process_due_schedules(tenant_id="t1")
    assert len(f2) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_concurrent_schedule_ticks_no_overlap_under_load(queue: JobQueue):
    """Many tickers + uncompleted first job: never more than one active job."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"k": "v"},
        name="solo",
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1

    time.sleep(1.2)
    barrier = threading.Barrier(10)
    fire_counts: list[int] = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def ticker() -> None:
        barrier.wait()
        try:
            fires = queue.process_due_schedules(tenant_id="t1")
            with lock:
                fire_counts.append(len(fires))
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=ticker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    assert errors == []
    assert sum(fire_counts) == 0  # all skipped — prior still pending
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM jobs
            WHERE schedule_id = %s AND status IN ('pending', 'leased')
            """,
            (sched.id,),
        ).fetchone()
    assert row is not None
    assert int(row["n"]) == 1


# ---------------------------------------------------------------------------
# Job dependencies
# ---------------------------------------------------------------------------


def test_dependent_job_not_claimable_until_parent_completes(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"step": 1})
    child = queue.enqueue(
        tenant_id="t1",
        payload={"step": 2},
        depends_on=parent.id,
    )
    assert child.depends_on == (parent.id,)

    # Only the parent is claimable.
    first = queue.lease(tenant_id="t1", worker_id="w1")
    assert first is not None
    assert first.id == parent.id
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    _ack(queue, first, "w1")

    second = queue.lease(tenant_id="t1", worker_id="w2")
    assert second is not None
    assert second.id == child.id
    _ack(queue, second, "w2")


def test_dependent_does_not_block_unrelated_jobs(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1}, priority=1)
    queue.enqueue(
        tenant_id="t1",
        payload={"child": True},
        depends_on=parent.id,
        priority=100,  # higher priority but blocked by parent
    )
    ready = queue.enqueue(tenant_id="t1", payload={"ready": True}, priority=0)

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    # Parent (pri 1) beats ready (pri 0); child is blocked despite pri 100.
    assert leased.id == parent.id
    _ack(queue, leased, "w1")

    leased2 = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased2 is not None
    assert leased2.payload.get("child") is True or leased2.id == ready.id
    # After parent done, child (100) should beat ready (0)
    assert leased2.payload.get("child") is True


def test_multi_parent_requires_all_completed(queue: JobQueue):
    a = queue.enqueue(tenant_id="t1", payload={"a": 1})
    b = queue.enqueue(tenant_id="t1", payload={"b": 1})
    child = queue.enqueue(
        tenant_id="t1",
        payload={"join": True},
        depends_on=[a.id, b.id],
    )
    assert set(child.depends_on) == {a.id, b.id}

    la = queue.lease(tenant_id="t1", worker_id="w1")
    assert la is not None
    _ack(queue, la, "w1")
    # One parent done — child still blocked; other parent claimable.
    lb = queue.lease(tenant_id="t1", worker_id="w1")
    assert lb is not None
    assert lb.id in {a.id, b.id}
    assert lb.id != child.id
    _ack(queue, lb, "w1")

    lc = queue.lease(tenant_id="t1", worker_id="w1")
    assert lc is not None
    assert lc.id == child.id


def test_parent_failure_cancels_pending_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={}, max_attempts=1)
    child = queue.enqueue(tenant_id="t1", payload={"c": 1}, depends_on=parent.id)
    grandchild = queue.enqueue(
        tenant_id="t1",
        payload={"g": 1},
        depends_on=child.id,
    )

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == parent.id
    dead = _nack(queue, leased, "w1", error="boom", requeue=True)
    assert dead.status == JobStatus.DEAD

    child_after = queue.get(child.id, tenant_id="t1")
    grand_after = queue.get(grandchild.id, tenant_id="t1")
    assert child_after is not None and child_after.status == JobStatus.CANCELLED
    assert grand_after is not None and grand_after.status == JobStatus.CANCELLED
    assert child_after.last_error is not None
    assert "parent" in child_after.last_error

    # Nothing left to claim.
    assert queue.lease(tenant_id="t1", worker_id="w2") is None


def test_parent_cancel_cascades_to_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={})
    child = queue.enqueue(tenant_id="t1", payload={}, depends_on=parent.id)

    cancelled = queue.cancel(parent.id, tenant_id="t1")
    assert cancelled.status == JobStatus.CANCELLED
    child_after = queue.get(child.id, tenant_id="t1")
    assert child_after is not None
    assert child_after.status == JobStatus.CANCELLED


def test_enqueue_rejects_missing_or_failed_dependency(queue: JobQueue):
    from job_queue import DependencyError
    from uuid import uuid4

    with pytest.raises(DependencyError, match="not found"):
        queue.enqueue(tenant_id="t1", payload={}, depends_on=uuid4())

    parent = queue.enqueue(tenant_id="t1", payload={}, max_attempts=1)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _nack(queue, leased, "w1", error="x", requeue=True)
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.DEAD

    with pytest.raises(DependencyError, match="already dead"):
        queue.enqueue(tenant_id="t1", payload={}, depends_on=parent.id)


def test_dependent_after_parent_success_e2e_worker(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"n": 1})
    queue.enqueue(tenant_id="t1", payload={"n": 2}, depends_on=parent.id)

    seen: list[int] = []
    worker = Worker(queue, tenant_id="t1", worker_id="dep-w")

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.payload["n"])

    assert worker.run(max_jobs=2, stop_when_idle=True) == 2
    assert seen == [1, 2]


# ---------------------------------------------------------------------------
# Job results on ack
# ---------------------------------------------------------------------------


def test_ack_stores_result_and_get_result(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"in": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    done = _ack(queue, leased, "w1", result={"out": 42, "ok": True})
    assert done.status == JobStatus.COMPLETED
    assert done.result == {"out": 42, "ok": True}

    got = queue.get(job.id, tenant_id="t1")
    assert got is not None
    assert got.result == {"out": 42, "ok": True}
    assert queue.get_result(job.id, tenant_id="t1") == {"out": 42, "ok": True}


def test_ack_without_result_leaves_null(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    done = _ack(queue, leased, "w1")
    assert done.result is None
    assert queue.get_result(done.id, tenant_id="t1") is None


def test_get_result_cross_tenant_and_unknown(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1", result={"secret": True})

    assert queue.get_result(leased.id, tenant_id="beta") is None
    assert queue.get_result(uuid4(), tenant_id="alpha") is None


def test_worker_handler_return_value_becomes_result(queue: JobQueue):
    enq = queue.enqueue(tenant_id="t1", payload={"n": 3})
    worker = Worker(queue, tenant_id="t1", worker_id="result-w")

    @worker.handler
    def handle(job: Job) -> dict:
        return {"doubled": job.payload["n"] * 2}

    worker.run_once()
    assert queue.get_result(enq.id, tenant_id="t1") == {"doubled": 6}
    final = queue.get(enq.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.COMPLETED


# ---------------------------------------------------------------------------
# Fair multi-tenant leasing
# ---------------------------------------------------------------------------


def test_fair_lease_without_tenant_id(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={"t": "a"})
    queue.enqueue(tenant_id="beta", payload={"t": "b"})

    j1 = queue.lease(worker_id="shared-1")
    j2 = queue.lease(worker_id="shared-1")
    assert j1 is not None and j2 is not None
    tenants = {j1.tenant_id, j2.tenant_id}
    assert tenants == {"alpha", "beta"}
    assert queue.lease(worker_id="shared-1") is None


def test_fair_lease_prefers_under_served_tenants(queue: JobQueue):
    """Noisy tenant with many jobs must not monopolize early leases."""
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i})
    queue.enqueue(tenant_id="quiet", payload={"only": True})

    claimed_tenants: list[str] = []
    for _ in range(5):
        job = queue.lease(worker_id="fair-w")
        assert job is not None
        claimed_tenants.append(job.tenant_id)

    # Quiet tenant should appear within the first few leases (fairness).
    assert "quiet" in claimed_tenants
    # Not all five from noisy before quiet got a turn.
    assert claimed_tenants.count("noisy") < 5


def test_fair_lease_ack_drain_does_not_starve_late_tenant(queue: JobQueue):
    """Real starvation case: noisy backlog enqueued first, workers lease+ack.

    With rank-by-created_at (or lease-count fairness that collapses when jobs
    are acked immediately), the entire noisy backlog would drain before quiet
    ever runs. Round-robin per tenant must serve quiet early.
    """
    noisy_n = 50
    for i in range(noisy_n):
        queue.enqueue(tenant_id="noisy", payload={"i": i, "t": "noisy"})
    quiet = queue.enqueue(tenant_id="quiet", payload={"t": "quiet"})

    order: list[str] = []
    quiet_position: int | None = None
    for step in range(noisy_n + 1):
        job = queue.lease(worker_id="drain-w")
        assert job is not None, f"expected a job at step {step}"
        order.append(job.tenant_id)
        if job.id == quiet.id:
            quiet_position = step
        _ack(queue, job, "drain-w")

    assert quiet_position is not None, "quiet job never leased"
    # Must not wait until after the full noisy backlog (position 50).
    # Round-robin: at worst quiet is 2nd (after noisy's first rn=1 job).
    assert quiet_position <= 1, (
        f"quiet starved until position {quiet_position}; order={order[:10]}..."
    )
    assert order.count("quiet") == 1
    assert order.count("noisy") == noisy_n
    assert queue.lease(worker_id="drain-w") is None


def test_fair_lease_round_robin_three_tenants_with_backlog(queue: JobQueue):
    """Each tenant with a backlog gets a turn before any gets a second job."""
    for t in ("a", "b", "c"):
        for i in range(10):
            queue.enqueue(tenant_id=t, payload={"t": t, "i": i})

    first_round: list[str] = []
    for _ in range(3):
        job = queue.lease(worker_id="rr-w")
        assert job is not None
        first_round.append(job.tenant_id)
        _ack(queue, job, "rr-w")

    assert set(first_round) == {"a", "b", "c"}

    second_round: list[str] = []
    for _ in range(3):
        job = queue.lease(worker_id="rr-w")
        assert job is not None
        second_round.append(job.tenant_id)
        _ack(queue, job, "rr-w")

    assert set(second_round) == {"a", "b", "c"}


def test_fair_lease_ack_drain_multi_worker_no_starvation(queue: JobQueue):
    """Several workers race lease+ack against a noisy backlog + quiet jobs."""
    for i in range(40):
        queue.enqueue(tenant_id="noisy", payload={"i": i})
    quiet_ids = set()
    for i in range(5):
        q = queue.enqueue(tenant_id="quiet", payload={"q": i})
        quiet_ids.add(q.id)

    completed_quiet: set = set()
    lock = threading.Lock()
    errors: list[BaseException] = []

    def worker_loop(wid: str) -> None:
        try:
            while True:
                job = queue.lease(worker_id=wid)
                if job is None:
                    # Might be racing; brief retry then exit if still empty.
                    time.sleep(0.05)
                    job = queue.lease(worker_id=wid)
                    if job is None:
                        return
                if job.id in quiet_ids:
                    with lock:
                        completed_quiet.add(job.id)
                _ack(queue, job, wid)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=worker_loop, args=(f"mw-{i}",)) for i in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    assert errors == [], f"worker errors: {errors!r}"
    assert completed_quiet == quiet_ids
    assert queue.count(tenant_id="noisy", status=JobStatus.COMPLETED) == 40
    assert queue.count(tenant_id="quiet", status=JobStatus.COMPLETED) == 5


def test_fair_lease_starvation_with_priority_and_backlog(queue: JobQueue):
    """High-priority noisy backlog still cannot block quiet default jobs forever."""
    for i in range(30):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=10)
    quiet = queue.enqueue(tenant_id="quiet", payload={"t": "q"}, priority=0)

    seen_quiet = False
    for step in range(31):
        job = queue.lease(worker_id="pri-w")
        assert job is not None
        if job.id == quiet.id:
            seen_quiet = True
            _ack(queue, job, "pri-w")
            break
        _ack(queue, job, "pri-w")
        assert step < 5, f"quiet still starved after {step + 1} leases"

    assert seen_quiet


def test_fair_lease_respects_tenant_concurrency_limit(queue: JobQueue):
    queue.set_concurrency_limit("capped", 1)
    queue.enqueue(tenant_id="capped", payload={"n": 1})
    queue.enqueue(tenant_id="capped", payload={"n": 2})
    queue.enqueue(tenant_id="other", payload={"n": 3})

    first = queue.lease(worker_id="w1")
    assert first is not None
    # Hold first without acking.
    second = queue.lease(worker_id="w2")
    assert second is not None

    if first.tenant_id == "capped":
        # Second must not be another capped pending job while first held.
        assert second.tenant_id == "other"
        third = queue.lease(worker_id="w3")
        assert third is None
    else:
        # First was other; second can be capped.
        assert first.tenant_id == "other"
        assert second.tenant_id == "capped"
        third = queue.lease(worker_id="w3")
        assert third is None  # capped at limit, other done


def test_fair_lease_respects_delay_and_dependencies(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    queue.enqueue(
        tenant_id="t1",
        payload={"child": True},
        depends_on=parent.id,
    )
    queue.enqueue(
        tenant_id="t2",
        payload={"later": True},
        available_at=datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    ready = queue.enqueue(tenant_id="t3", payload={"ready": True})

    first = queue.lease(worker_id="w1")
    assert first is not None
    # Child blocked; delayed not due — only parent or ready.
    assert first.id in {parent.id, ready.id}
    if first.id == parent.id:
        _ack(queue, first, "w1")
        second = queue.lease(worker_id="w1")
        assert second is not None
        assert second.id in {ready.id, parent.id} or second.payload.get("child")
    else:
        assert first.id == ready.id


def test_fair_lease_queue_name_isolation(queue: JobQueue):
    queue.enqueue(tenant_id="a", queue_name="email", payload={"q": "e"})
    queue.enqueue(tenant_id="b", queue_name="sms", payload={"q": "s"})

    job = queue.lease(worker_id="w1", queue_name="email")
    assert job is not None
    assert job.queue_name == "email"
    assert job.tenant_id == "a"
    assert queue.lease(worker_id="w1", queue_name="email") is None


def test_tenant_scoped_lease_still_works(queue: JobQueue):
    queue.enqueue(tenant_id="only-me", payload={"x": 1})
    queue.enqueue(tenant_id="other", payload={"x": 2})

    job = queue.lease(tenant_id="only-me", worker_id="w1")
    assert job is not None
    assert job.tenant_id == "only-me"
    assert queue.lease(tenant_id="only-me", worker_id="w1") is None


def test_fair_worker_processes_across_tenants(queue: JobQueue):
    queue.enqueue(tenant_id="a", payload={"n": 1})
    queue.enqueue(tenant_id="b", payload={"n": 2})

    seen: list[str] = []
    worker = Worker(queue, worker_id="fair-worker")  # no tenant_id

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.tenant_id)

    n = worker.run(max_jobs=2, stop_when_idle=True)
    assert n == 2
    assert set(seen) == {"a", "b"}


# ---------------------------------------------------------------------------
# Exponential retry backoff
# ---------------------------------------------------------------------------


def test_nack_applies_exponential_backoff_before_reclaimable(queue: JobQueue):
    from datetime import datetime, timezone

    from job_queue import compute_retry_backoff_seconds

    job = queue.enqueue(
        tenant_id="t1",
        payload={"retry": True},
        max_attempts=5,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=8,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.attempts == 1

    before = datetime.now(timezone.utc)
    nacked = _nack(queue, leased, "w1", error="temp fail", requeue=True)
    assert nacked.status == JobStatus.PENDING
    expected = compute_retry_backoff_seconds(1, base_seconds=1, max_seconds=8)
    assert expected == 1
    assert nacked.available_at >= before
    # Not immediately claimable.
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    time.sleep(1.15)
    again = queue.lease(tenant_id="t1", worker_id="w2")
    assert again is not None
    assert again.id == job.id
    assert again.attempts == 2


def test_backoff_grows_exponentially_and_caps(queue: JobQueue):
    from job_queue import compute_retry_backoff_seconds

    assert compute_retry_backoff_seconds(1, base_seconds=2, max_seconds=10) == 2
    assert compute_retry_backoff_seconds(2, base_seconds=2, max_seconds=10) == 4
    assert compute_retry_backoff_seconds(3, base_seconds=2, max_seconds=10) == 8
    assert compute_retry_backoff_seconds(4, base_seconds=2, max_seconds=10) == 10  # capped
    assert compute_retry_backoff_seconds(5, base_seconds=2, max_seconds=10) == 10

    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=4,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=2,  # cap at 2s
    )
    # Fail attempt 1 → backoff 1s
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    n1 = _nack(queue, j, "w1", error="e1", requeue=True)
    assert n1.status == JobStatus.PENDING
    d1 = (n1.available_at - n1.updated_at).total_seconds()
    assert 0.9 <= d1 <= 1.5

    time.sleep(1.15)
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    assert j.attempts == 2
    # Fail attempt 2 → backoff min(2, 2)=2s
    n2 = _nack(queue, j, "w1", error="e2", requeue=True)
    d2 = (n2.available_at - n2.updated_at).total_seconds()
    assert 1.9 <= d2 <= 2.5

    time.sleep(2.15)
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    assert j.attempts == 3
    # Fail attempt 3 → still capped at 2s
    n3 = _nack(queue, j, "w1", error="e3", requeue=True)
    d3 = (n3.available_at - n3.updated_at).total_seconds()
    assert 1.9 <= d3 <= 2.5


def test_backoff_then_dead_letter_after_max_attempts(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=2,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=5,
    )
    j1 = queue.lease(tenant_id="t1", worker_id="w1")
    assert j1 is not None
    n1 = _nack(queue, j1, "w1", error="once", requeue=True)
    assert n1.status == JobStatus.PENDING
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    time.sleep(1.15)
    j2 = queue.lease(tenant_id="t1", worker_id="w2")
    assert j2 is not None
    assert j2.attempts == 2
    n2 = _nack(queue, j2, "w2", error="twice", requeue=True)
    assert n2.status == JobStatus.DEAD
    assert queue.lease(tenant_id="t1", worker_id="w3") is None
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 1


def test_fair_lease_skips_jobs_in_backoff(queue: JobQueue):
    """A nacked job in backoff must not be claimed by fair lease either."""
    broken = queue.enqueue(
        tenant_id="broken",
        payload={"b": 1},
        max_attempts=5,
        retry_backoff_base_seconds=2,
        retry_backoff_max_seconds=30,
    )
    healthy = queue.enqueue(tenant_id="healthy", payload={"h": 1})

    j = queue.lease(worker_id="fw")
    # Fair order: never-served tenants; broken enqueued first.
    assert j is not None
    if j.id == broken.id:
        _nack(queue, j, "fw", error="nope", requeue=True)
        # Immediate fair lease should get healthy, not broken-in-backoff.
        j2 = queue.lease(worker_id="fw")
        assert j2 is not None
        assert j2.id == healthy.id
        _ack(queue, j2, "fw")
        assert queue.lease(worker_id="fw") is None  # broken still in backoff
    else:
        # Unlikely but possible if fairness flips; still validate backoff.
        _ack(queue, j, "fw")
        j2 = queue.lease(worker_id="fw")
        assert j2 is not None
        assert j2.id == broken.id
        _nack(queue, j2, "fw", error="nope", requeue=True)
        assert queue.lease(worker_id="fw") is None


def test_worker_nack_uses_backoff(queue: JobQueue):
    enq = queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=3,
        retry_backoff_base_seconds=1,
        retry_backoff_max_seconds=5,
    )
    worker = Worker(queue, tenant_id="t1", worker_id="bw")

    @worker.handler
    def handle(job: Job) -> None:
        raise RuntimeError("still broken")

    with pytest.raises(RuntimeError, match="still broken"):
        worker.run_once()

    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    got = queue.get(enq.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.PENDING
    assert got.available_at > got.updated_at
    assert queue.lease(tenant_id="t1", worker_id="x") is None
    time.sleep(1.15)
    again = queue.lease(tenant_id="t1", worker_id="x")
    assert again is not None
    assert again.id == enq.id
    assert again.attempts == 2
