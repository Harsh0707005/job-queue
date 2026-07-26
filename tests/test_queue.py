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
    """Unrelated tenant cannot heartbeat/ack/nack even with a valid lease token."""
    from uuid import uuid4

    queue.enqueue(tenant_id="alpha", payload={})
    job = queue.lease(tenant_id="alpha", worker_id="w1")
    assert job is not None
    assert job.lease_token is not None
    assert job.lease_tenant_id == "alpha"

    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.ack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.heartbeat(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.nack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
            error="nope",
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.ensure_lease(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )

    # Wrong token still rejected for the owning tenant.
    with pytest.raises(LeaseError):
        queue.heartbeat(
            job.id,
            tenant_id="alpha",
            worker_id="w1",
            lease_token=uuid4(),
        )

    # Owning tenant can still complete.
    _ack(queue, job, "w1")
    assert queue.get(job.id, tenant_id="alpha") is not None
    assert queue.get(job.id, tenant_id="beta") is None
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


def test_pending_limit_rejects_enqueue_at_cap(queue: JobQueue):
    queue.set_pending_limit("t1", 2)
    assert queue.get_pending_limit("t1") == 2

    queue.enqueue(tenant_id="t1", payload={"i": 1})
    queue.enqueue(tenant_id="t1", payload={"i": 2})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="t1", payload={"i": 3})

    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 2

    # Lease one → pending drops; enqueue allowed again.
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    queue.enqueue(tenant_id="t1", payload={"i": 4})
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 2


def test_pending_limit_is_per_tenant(queue: JobQueue):
    queue.set_pending_limit("alpha", 1)
    queue.set_pending_limit("beta", 1)
    queue.enqueue(tenant_id="alpha", payload={"a": 1})
    queue.enqueue(tenant_id="beta", payload={"b": 1})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="alpha", payload={"a": 2})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="beta", payload={"b": 2})


def test_pending_limit_clear(queue: JobQueue):
    queue.set_pending_limit("t1", 1)
    queue.enqueue(tenant_id="t1", payload={"i": 1})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="t1", payload={"i": 2})
    queue.set_pending_limit("t1", None)
    assert queue.get_pending_limit("t1") is None
    queue.enqueue(tenant_id="t1", payload={"i": 2})
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 2


def test_pending_limit_dedupe_existing_does_not_fail(queue: JobQueue):
    queue.set_pending_limit("t1", 1)
    j1 = queue.enqueue(tenant_id="t1", payload={"i": 1}, dedupe_key="k1")
    # Same dedupe returns existing row without counting as a new pending.
    j2 = queue.enqueue(tenant_id="t1", payload={"i": 1}, dedupe_key="k1")
    assert j1.id == j2.id
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="t1", payload={"i": 2}, dedupe_key="k2")


def test_pending_limit_tenant_stats(queue: JobQueue):
    queue.set_pending_limit("t1", 5)
    queue.set_concurrency_limit("t1", 2)
    stats = queue.get_tenant_stats("t1")
    assert stats.max_pending_jobs == 5
    assert stats.max_concurrent_leases == 2


def test_concurrent_enqueue_respects_pending_cap(queue: JobQueue):
    queue.set_pending_limit("t1", 10)
    barrier = threading.Barrier(20)
    ok = 0
    rejected = 0
    lock = threading.Lock()

    def worker(i: int) -> None:
        nonlocal ok, rejected
        barrier.wait()
        try:
            queue.enqueue(tenant_id="t1", payload={"i": i})
            with lock:
                ok += 1
        except ValueError:
            with lock:
                rejected += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert ok == 10
    assert rejected == 10
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 10



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


def test_fair_lease_priority_lane_before_low(queue: JobQueue):
    """High-priority work of any tenant runs before low-priority noise."""
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=0)
    high = queue.enqueue(tenant_id="quiet", payload={"t": "q"}, priority=10)

    first = queue.lease(worker_id="pri-w")
    assert first is not None
    assert first.id == high.id
    _ack(queue, first, "pri-w")


def test_fair_lease_high_priority_lane_fair_across_tenants(queue: JobQueue):
    """Within the high lane, tenants rotate — one tenant cannot monopolize."""
    for i in range(15):
        queue.enqueue(tenant_id="a", payload={"i": i}, priority=10)
    for i in range(15):
        queue.enqueue(tenant_id="b", payload={"i": i}, priority=10)
    # Low noise that must wait until the high lane drains.
    for i in range(10):
        queue.enqueue(tenant_id="c", payload={"i": i}, priority=0)

    first_30: list[str] = []
    for _ in range(30):
        job = queue.lease(worker_id="lane-w")
        assert job is not None
        first_30.append(job.tenant_id)
        assert job.priority == 10, "low lane must not interleave before high drains"
        _ack(queue, job, "lane-w")

    assert first_30.count("a") == 15
    assert first_30.count("b") == 15
    # Round-robin within lane: no long monopoly streak of a single tenant.
    max_streak = 1
    streak = 1
    for i in range(1, len(first_30)):
        if first_30[i] == first_30[i - 1]:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 1
    assert max_streak <= 2, f"tenant monopolized high lane: streak={max_streak}"

    low = queue.lease(worker_id="lane-w")
    assert low is not None
    assert low.priority == 0
    assert low.tenant_id == "c"


def test_fair_lease_high_of_quiet_beats_low_of_noisy(queue: JobQueue):
    """Noisy tenant flooding low priority cannot block quiet high priority."""
    for i in range(40):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=1)
    quiet_high = queue.enqueue(tenant_id="quiet", payload={"t": "q"}, priority=100)

    order: list[tuple[str, int]] = []
    for _ in range(5):
        job = queue.lease(worker_id="pri-w")
        assert job is not None
        order.append((job.tenant_id, job.priority))
        _ack(queue, job, "pri-w")

    assert order[0] == ("quiet", 100)
    # Remaining early leases are noisy low only after high is done.
    assert all(t == "noisy" and p == 1 for t, p in order[1:])


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


# ---------------------------------------------------------------------------
# Tenant pause / resume / metrics
# ---------------------------------------------------------------------------


def test_pause_tenant_blocks_new_leases_scoped_and_fair(queue: JobQueue):
    queue.enqueue(tenant_id="bad", payload={"n": 1})
    queue.enqueue(tenant_id="good", payload={"n": 2})

    queue.pause_tenant("bad")
    assert queue.is_tenant_paused("bad") is True
    assert queue.is_tenant_paused("good") is False

    # Scoped lease for paused tenant yields nothing.
    assert queue.lease(tenant_id="bad", worker_id="w1") is None

    # Fair lease skips paused tenant.
    job = queue.lease(worker_id="fair")
    assert job is not None
    assert job.tenant_id == "good"
    _ack(queue, job, "fair")

    assert queue.lease(worker_id="fair") is None
    # Pending work for bad still exists.
    assert queue.count(tenant_id="bad", status=JobStatus.PENDING) == 1


def test_pause_allows_in_flight_heartbeat_ack_nack(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={"x": 1}, max_attempts=3)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    queue.pause_tenant("t1")
    # No new leases while paused.
    queue.enqueue(tenant_id="t1", payload={"x": 2})
    assert queue.lease(tenant_id="t1", worker_id="w2") is None

    # In-flight still works.
    extended = _hb(queue, leased, "w1", extend_by_seconds=30)
    assert extended.lease_expires_at is not None
    done = _ack(queue, leased, "w1", result={"ok": True})
    assert done.status == JobStatus.COMPLETED
    assert queue.get_result(done.id, tenant_id="t1") == {"ok": True}

    # Nack path on another in-flight job.
    j2 = queue.enqueue(
        tenant_id="t1",
        payload={"x": 3},
        max_attempts=2,
        retry_backoff_base_seconds=0,
    )
    # Still paused — cannot lease the new job.
    assert queue.lease(tenant_id="t1", worker_id="w1") is None

    queue.resume_tenant("t1")
    assert queue.is_tenant_paused("t1") is False
    leased2 = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased2 is not None
    assert leased2.payload["x"] in (2, 3)
    queue.pause_tenant("t1")
    nacked = _nack(queue, leased2, "w1", error="boom", requeue=True)
    assert nacked.status == JobStatus.PENDING


def test_resume_tenant_restores_leasing(queue: JobQueue):
    queue.enqueue(tenant_id="t1", payload={"a": 1})
    queue.pause_tenant("t1")
    assert queue.lease(tenant_id="t1", worker_id="w1") is None

    queue.resume_tenant("t1")
    job = queue.lease(tenant_id="t1", worker_id="w1")
    assert job is not None
    assert job.payload == {"a": 1}
    _ack(queue, job, "w1")


def test_get_tenant_stats_counts_and_flags(queue: JobQueue):
    from job_queue import TenantStats

    queue.set_concurrency_limit("acme", 2)
    # Completed
    queue.enqueue(tenant_id="acme", payload={"p": 1})
    leased = queue.lease(tenant_id="acme", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    # Pending (left unclaimed)
    queue.enqueue(tenant_id="acme", payload={"p": 2})

    # Dead (only job on queue when leased so we hit max_attempts=1)
    queue.enqueue(
        tenant_id="acme",
        payload={"d": 1},
        max_attempts=1,
        retry_backoff_base_seconds=0,
        priority=100,
    )
    lj = queue.lease(tenant_id="acme", worker_id="w1")
    assert lj is not None
    assert lj.payload.get("d") == 1
    nacked = _nack(queue, lj, "w1", error="fail", requeue=True)
    assert nacked.status == JobStatus.DEAD

    # Cancelled
    c = queue.enqueue(tenant_id="acme", payload={"c": 1})
    queue.cancel(c.id, tenant_id="acme")

    # Leased in-flight
    queue.enqueue(tenant_id="acme", payload={"live": True}, priority=50)
    in_flight = queue.lease(tenant_id="acme", worker_id="w1")
    assert in_flight is not None

    queue.pause_tenant("acme")
    stats = queue.get_tenant_stats("acme")
    assert isinstance(stats, TenantStats)
    assert stats.tenant_id == "acme"
    assert stats.paused is True
    assert stats.max_concurrent_leases == 2
    assert stats.completed == 1
    assert stats.dead == 1
    assert stats.cancelled == 1
    assert stats.pending >= 1
    assert stats.leased == 1
    assert stats.total == (
        stats.pending
        + stats.leased
        + stats.completed
        + stats.failed
        + stats.dead
        + stats.cancelled
    )
    assert stats.claimable == 0  # paused

    _ack(queue, in_flight, "w1")
    queue.resume_tenant("acme")
    stats2 = queue.get_tenant_stats("acme")
    assert stats2.paused is False
    assert stats2.claimable >= 1


def test_list_tenant_stats_multiple_tenants(queue: JobQueue):
    queue.enqueue(tenant_id="a", payload={})
    queue.enqueue(tenant_id="b", payload={})
    queue.pause_tenant("b")

    all_stats = queue.list_tenant_stats()
    by_id = {s.tenant_id: s for s in all_stats}
    assert "a" in by_id and "b" in by_id
    assert by_id["a"].paused is False
    assert by_id["b"].paused is True
    assert by_id["a"].pending == 1
    assert by_id["b"].pending == 1


def test_pause_preserves_concurrency_limit(queue: JobQueue):
    queue.set_concurrency_limit("t1", 3)
    queue.pause_tenant("t1")
    assert queue.get_concurrency_limit("t1") == 3
    assert queue.is_tenant_paused("t1") is True
    queue.resume_tenant("t1")
    assert queue.get_concurrency_limit("t1") == 3
    assert queue.is_tenant_paused("t1") is False


def test_fair_lease_skips_paused_prefers_others(queue: JobQueue):
    for i in range(5):
        queue.enqueue(tenant_id="paused-noisy", payload={"i": i})
    queue.enqueue(tenant_id="ok", payload={"only": True})
    queue.pause_tenant("paused-noisy")

    order: list[str] = []
    for _ in range(6):
        job = queue.lease(worker_id="w")
        if job is None:
            break
        order.append(job.tenant_id)
        _ack(queue, job, "w")

    assert order == ["ok"]
    assert queue.count(tenant_id="paused-noisy", status=JobStatus.PENDING) == 5

    queue.resume_tenant("paused-noisy")
    # After resume, drain paused tenant's backlog.
    claimed = 0
    while True:
        job = queue.lease(worker_id="w")
        if job is None:
            break
        assert job.tenant_id == "paused-noisy"
        _ack(queue, job, "w")
        claimed += 1
    assert claimed == 5


def test_paused_tenant_schedules_do_not_enqueue(queue: JobQueue):
    """While paused, process_due must not pile schedule jobs."""
    sched = queue.create_schedule(
        tenant_id="paused-sched",
        interval_seconds=1,
        payload={"tick": True},
        name="t1",
    )
    queue.pause_tenant("paused-sched")

    # Many due ticks simulated by repeated process_due while paused.
    for _ in range(5):
        fires = queue.process_due_schedules(tenant_id="paused-sched")
        assert fires == []
        time.sleep(0.25)

    assert queue.count(tenant_id="paused-sched") == 0
    # Other tenants' schedules still fire.
    queue.create_schedule(
        tenant_id="ok-sched",
        interval_seconds=60,
        payload={"ok": True},
        name="ok",
    )
    ok_fires = queue.process_due_schedules(tenant_id="ok-sched")
    assert len(ok_fires) == 1


def test_resume_tenant_schedule_fires_once_not_tick_backlog(queue: JobQueue):
    """After a long pause, resume + process_due enqueues one job per schedule."""
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
        name="s1",
    )
    # Fire once, complete, then pause and wait through several intervals.
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    _ack(queue, j, "w1")

    queue.pause_tenant("t1")
    time.sleep(2.5)  # would have been multiple ticks if not paused
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1

    queue.resume_tenant("t1")
    fires2 = queue.process_due_schedules(tenant_id="t1")
    assert len(fires2) == 1
    # Second process immediately should not fire again (interval advanced).
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_paused_tenant_expired_lease_not_reclaimed(queue: JobQueue):
    """Expired leases for a paused tenant must not be reclaimed until resume."""
    queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=5,
        visibility_timeout_seconds=1,
    )
    leased = queue.lease(
        tenant_id="t1",
        worker_id="holder",
        visibility_timeout_seconds=1,
    )
    assert leased is not None
    queue.pause_tenant("t1")

    time.sleep(1.2)

    # Scoped and fair must not reclaim while paused.
    assert queue.lease(tenant_id="t1", worker_id="other") is None
    assert queue.lease(worker_id="fair-other") is None

    # Original job still leased (expired) or not taken by others.
    got = queue.get(leased.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.LEASED
    assert got.leased_by == "holder"

    # After resume, reclaim is allowed.
    queue.resume_tenant("t1")
    reclaimed = queue.lease(
        tenant_id="t1",
        worker_id="rescuer",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None
    assert reclaimed.id == leased.id
    assert reclaimed.leased_by == "rescuer"
    _ack(queue, reclaimed, "rescuer")


def test_paused_tenant_fair_lease_no_reclaim_among_mixed_tenants(queue: JobQueue):
    queue.enqueue(
        tenant_id="paused",
        payload={"p": 1},
        visibility_timeout_seconds=1,
        max_attempts=5,
    )
    queue.enqueue(tenant_id="active", payload={"a": 1})

    held = queue.lease(
        worker_id="w1",
        visibility_timeout_seconds=1,
    )
    assert held is not None
    # Ensure we hold the paused tenant's job specifically.
    if held.tenant_id != "paused":
        _ack(queue, held, "w1")
        held = queue.lease(worker_id="w1", visibility_timeout_seconds=1)
        assert held is not None
        assert held.tenant_id == "paused"

    queue.pause_tenant("paused")
    time.sleep(1.2)

    # Fair lease should only see active tenant work (or nothing if already done).
    for _ in range(3):
        job = queue.lease(worker_id="w2")
        if job is None:
            break
        assert job.tenant_id != "paused"
        _ack(queue, job, "w2")

    still = queue.get(held.id, tenant_id="paused")
    assert still is not None
    assert still.status == JobStatus.LEASED
    assert still.leased_by == "w1"


# ---------------------------------------------------------------------------
# HTTP webhooks (durable outbox)
# ---------------------------------------------------------------------------


class _WebhookCapture:
    """Minimal HTTP server that records POSTs for tests."""

    def __init__(self, *, fail_times: int = 0, status: int = 200):
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import threading

        self.received: list[dict] = []
        self.fail_times = fail_times
        self._fails_left = fail_times
        self.status = status
        self._lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                import json

                with parent._lock:
                    should_fail = parent._fails_left > 0
                    if should_fail:
                        parent._fails_left -= 1
                    parent.received.append(
                        {
                            "path": self.path,
                            "body": json.loads(body.decode("utf-8")),
                            "signature": self.headers.get("X-Job-Queue-Signature"),
                        }
                    )
                if should_fail:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(b"fail")
                else:
                    self.send_response(parent.status)
                    self.end_headers()
                    self.wfile.write(b"ok")

            def log_message(self, format, *args):  # noqa: A003
                return

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}/hook"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._thread.join(timeout=5)


def test_webhook_on_complete_persisted_then_delivered(queue: JobQueue):
    from job_queue import WebhookEvent

    capture = _WebhookCapture()
    try:
        ep = queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="s3cret",
        )
        assert ep.enabled is True

        queue.enqueue(tenant_id="t1", payload={"hello": "world"})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1", result={"score": 1})

        # Durable outbox row exists before delivery.
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        assert pending[0].event == "job.completed"
        assert pending[0].payload["result"] == {"score": 1}
        assert pending[0].job_id == job.id

        results = queue.process_webhook_deliveries(limit=10)
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].delivery.status == "succeeded"

        assert len(capture.received) == 1
        assert capture.received[0]["body"]["event"] == "job.completed"
        assert capture.received[0]["signature"] is not None

        assert queue.list_webhook_deliveries(tenant_id="t1", status="pending") == []
        assert len(queue.list_webhook_deliveries(tenant_id="t1", status="succeeded")) == 1
    finally:
        capture.stop()


def test_webhook_retry_backoff_then_succeed(queue: JobQueue):
    capture = _WebhookCapture(fail_times=2)
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            max_attempts=5,
            backoff_base_seconds=1,
            backoff_max_seconds=5,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        # Attempt 1 fails → still pending with backoff.
        r1 = queue.process_webhook_deliveries()
        assert len(r1) == 1
        assert r1[0].success is False
        assert r1[0].delivery.status == "pending"
        assert r1[0].delivery.attempts == 1
        assert r1[0].delivery.next_attempt_at > r1[0].delivery.updated_at

        # Not due yet.
        assert queue.process_webhook_deliveries() == []

        time.sleep(1.15)
        r2 = queue.process_webhook_deliveries()
        assert len(r2) == 1
        assert r2[0].success is False
        assert r2[0].delivery.attempts == 2

        time.sleep(2.15)  # exponential: 2s after attempt 2
        r3 = queue.process_webhook_deliveries()
        assert len(r3) == 1
        assert r3[0].success is True
        assert r3[0].delivery.status == "succeeded"
        assert len(capture.received) == 3
    finally:
        capture.stop()


def test_webhook_dead_letters_after_max_attempts(queue: JobQueue):
    capture = _WebhookCapture(fail_times=100)
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            max_attempts=2,
            backoff_base_seconds=0,
            backoff_max_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        r1 = queue.process_webhook_deliveries()
        assert r1[0].delivery.status == "pending"
        r2 = queue.process_webhook_deliveries()
        assert r2[0].delivery.status == "dead"
        assert r2[0].delivery.attempts == 2
        assert queue.list_webhook_deliveries(tenant_id="t1", status="dead")
    finally:
        capture.stop()


def test_webhook_on_failed_and_dead_events(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.failed", "job.dead"],
            backoff_base_seconds=0,
        )
        # failed via requeue=False
        queue.enqueue(
            tenant_id="t1",
            payload={"f": 1},
            max_attempts=3,
            retry_backoff_base_seconds=0,
        )
        j1 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j1 is not None
        n1 = _nack(queue, j1, "w1", error="nope", requeue=False)
        assert n1.status == JobStatus.FAILED

        # dead via max attempts
        queue.enqueue(
            tenant_id="t1",
            payload={"d": 1},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        j2 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j2 is not None
        n2 = _nack(queue, j2, "w1", error="gone", requeue=True)
        assert n2.status == JobStatus.DEAD

        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        events = {d.event for d in pending}
        assert events == {"job.failed", "job.dead"}

        results = queue.process_webhook_deliveries(limit=10)
        assert len(results) == 2
        assert all(r.success for r in results)
        received_events = {r["body"]["event"] for r in capture.received}
        assert received_events == {"job.failed", "job.dead"}
    finally:
        capture.stop()


def test_webhook_only_matching_tenant_and_events(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="a",
            url=capture.url,
            events=["job.completed"],
        )
        queue.register_webhook(
            tenant_id="b",
            url=capture.url,
            events=["job.dead"],
        )
        queue.enqueue(tenant_id="a", payload={})
        ja = queue.lease(tenant_id="a", worker_id="w")
        assert ja is not None
        _ack(queue, ja, "w")

        # Only tenant a completion delivery.
        pending = queue.list_webhook_deliveries(tenant_id="a", status="pending")
        assert len(pending) == 1
        assert queue.list_webhook_deliveries(tenant_id="b", status="pending") == []

        queue.process_webhook_deliveries()
        assert len(capture.received) == 1
        assert capture.received[0]["body"]["tenant_id"] == "a"
    finally:
        capture.stop()


def test_webhook_dispatcher_run_once(queue: JobQueue):
    from job_queue import WebhookDispatcher

    capture = _WebhookCapture()
    try:
        queue.register_webhook(tenant_id="t1", url=capture.url, events=["job.completed"])
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        disp = WebhookDispatcher(queue, batch_limit=10)
        results = disp.run_once()
        assert len(results) == 1
        assert results[0].success
    finally:
        capture.stop()


def test_webhook_no_double_send_after_success_before_db_finalize(queue: JobQueue):
    """Simulate crash after HTTP 200 but before status=succeeded.

    Claim+POST leave last_status_code=2xx while status stays delivering.
    Next process must NOT POST again; it finalizes as succeeded.
    """
    from datetime import datetime, timedelta, timezone
    from job_queue.webhooks import post_webhook

    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={"x": 1})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        delivery = pending[0]

        # --- Simulate phase1 claim + successful POST + crash before succeeded ---
        now = datetime.now(timezone.utc)
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = 'delivering',
                    attempts = 1,
                    delivering_until = %s,
                    last_status_code = 200,
                    updated_at = %s
                WHERE id = %s
                """,
                (now + timedelta(seconds=60), now, delivery.id),
            )
            conn.commit()

        # Record that the real first POST already happened (simulate).
        code, _ = post_webhook(
            capture.url,
            delivery.payload,
            idempotency_key=str(delivery.id),
        )
        assert 200 <= code < 300
        assert len(capture.received) == 1

        # Still within claim lease: process must not re-select as pending.
        r_mid = queue.process_webhook_deliveries(claim_seconds=60)
        # May recover as succeeded if lease not expired... with 60s claim_seconds
        # and delivering_until in future, reclaim query needs delivering_until < now.
        # So nothing due.
        assert all(
            x.delivery.id != delivery.id or x.delivery.status == "succeeded"
            for x in r_mid
        )
        # Force stuck lease expiry (crash recovery window).
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET delivering_until = %s
                WHERE id = %s AND status = 'delivering'
                """,
                (datetime.now(timezone.utc) - timedelta(seconds=1), delivery.id),
            )
            conn.commit()

        before = len(capture.received)
        results = queue.process_webhook_deliveries(claim_seconds=30)
        # Recovered via last_status_code=200 → succeeded, no new POST.
        assert len(capture.received) == before
        got = queue.get_webhook_delivery(delivery.id, tenant_id="t1")
        assert got is not None
        assert got.status == "succeeded"
        assert any(r.success and r.delivery.id == delivery.id for r in results)
    finally:
        capture.stop()


def test_webhook_claim_before_post_not_pending_until_done(queue: JobQueue):
    """After claim, row is delivering so concurrent process won't double-claim."""
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        # Manually claim like phase1
        now = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        )
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        did = pending[0].id
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = 'delivering',
                    attempts = 1,
                    delivering_until = %s
                WHERE id = %s
                """,
                (now + __import__("datetime").timedelta(seconds=120), did),
            )
            conn.commit()

        # Concurrent process sees no pending due rows.
        assert queue.list_webhook_deliveries(tenant_id="t1", status="pending") == []
        assert queue.list_webhook_deliveries(tenant_id="t1", status="delivering")
        r = queue.process_webhook_deliveries()
        assert r == []  # still leased, not expired
        assert len(capture.received) == 0
    finally:
        capture.stop()


def test_paused_tenant_does_not_enqueue_webhooks(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed", "job.failed", "job.dead"],
            backoff_base_seconds=0,
        )
        queue.pause_tenant("t1")

        # Complete while paused → no delivery row.
        queue.enqueue(tenant_id="t1", payload={"a": 1})
        # Resume only to lease, then pause again before ack? In-flight can
        # complete while paused — still must not enqueue webhooks.
        queue.resume_tenant("t1")
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        queue.pause_tenant("t1")
        _ack(queue, job, "w1", result={"ok": True})

        assert queue.list_webhook_deliveries(tenant_id="t1") == []
        assert queue.process_webhook_deliveries() == []
        assert capture.received == []

        # Dead while paused also creates no delivery.
        queue.resume_tenant("t1")
        queue.enqueue(
            tenant_id="t1",
            payload={"d": 1},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        j2 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j2 is not None
        queue.pause_tenant("t1")
        n2 = _nack(queue, j2, "w1", error="x", requeue=True)
        assert n2.status == JobStatus.DEAD
        assert queue.list_webhook_deliveries(tenant_id="t1") == []
    finally:
        capture.stop()


def test_paused_tenant_does_not_claim_pending_webhook_deliveries(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        # Create delivery while active.
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1

        queue.pause_tenant("t1")
        # Must not claim/send while paused.
        assert queue.process_webhook_deliveries() == []
        assert capture.received == []
        still = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(still) == 1

        # Other tenant still delivers.
        queue.register_webhook(
            tenant_id="t2",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t2", payload={})
        j2 = queue.lease(tenant_id="t2", worker_id="w2")
        assert j2 is not None
        _ack(queue, j2, "w2")
        r = queue.process_webhook_deliveries()
        assert len(r) == 1
        assert r[0].success
        assert capture.received[-1]["body"]["tenant_id"] == "t2"

        # Resume → pending t1 delivery goes out.
        queue.resume_tenant("t1")
        r2 = queue.process_webhook_deliveries()
        assert len(r2) == 1
        assert r2[0].success
        assert r2[0].delivery.tenant_id == "t1"
        assert any(x["body"]["tenant_id"] == "t1" for x in capture.received)
    finally:
        capture.stop()


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


# ---------------------------------------------------------------------------
# Max runtime hard stop (stuck heartbeat cannot hold forever)
# ---------------------------------------------------------------------------


def test_heartbeat_cannot_extend_past_max_runtime(queue: JobQueue):
    job = queue.enqueue(
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
    from job_queue import LeaseError

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


# ---------------------------------------------------------------------------
# Tenant offboard
# ---------------------------------------------------------------------------


def test_offboard_cancels_pending_deletes_schedules_webhooks(queue: JobQueue):
    from job_queue import TenantOffboardResult

    queue.enqueue(tenant_id="gone", payload={"n": 1})
    queue.enqueue(tenant_id="gone", payload={"n": 2})
    queue.create_schedule(
        tenant_id="gone",
        interval_seconds=60,
        payload={"tick": True},
        name="tick",
    )
    ep = queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
    )
    assert ep.tenant_id == "gone"

    # Other tenant must be untouched.
    queue.enqueue(tenant_id="keep", payload={"ok": True})
    queue.create_schedule(
        tenant_id="keep",
        interval_seconds=60,
        payload={"k": 1},
        name="keep-tick",
    )

    result = queue.offboard_tenant("gone")
    assert isinstance(result, TenantOffboardResult)
    assert result.tenant_id == "gone"
    assert result.pending_cancelled == 2
    assert result.schedules_deleted == 1
    assert result.webhooks_deleted == 1
    assert result.leased_remaining == 0
    assert result.webhooks_delivering_remaining == 0
    assert result.empty is True
    assert result.offboarding is False
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False

    # Keep tenant still works.
    assert queue.count(tenant_id="keep", status=JobStatus.PENDING) == 1
    assert len(queue.list_schedules(tenant_id="keep")) == 1
    job = queue.lease(tenant_id="keep", worker_id="w1")
    assert job is not None
    _ack(queue, job, "w1")


def test_offboard_allows_in_flight_ack_then_empties(queue: JobQueue):
    queue.enqueue(tenant_id="gone", payload={"run": 1})
    queue.enqueue(tenant_id="gone", payload={"wait": 1})  # pending, will cancel
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    assert leased.payload == {"run": 1}

    queue.create_schedule(
        tenant_id="gone", interval_seconds=30, payload={"s": 1}
    )
    queue.register_webhook(tenant_id="gone", url="http://example.test/h")

    result = queue.offboard_tenant("gone")
    assert result.pending_cancelled >= 1
    assert result.schedules_deleted == 1
    assert result.webhooks_deleted == 1
    assert result.leased_remaining == 1
    assert result.empty is False
    assert result.offboarding is True
    assert queue.is_tenant_offboarding("gone") is True
    assert queue.is_tenant_paused("gone") is True
    assert queue.tenant_is_empty("gone") is False

    # Enqueue / schedule / webhook / resume blocked while offboarding.
    with pytest.raises(ValueError, match="offboarding"):
        queue.enqueue(tenant_id="gone", payload={"nope": 1})
    with pytest.raises(ValueError, match="offboarding"):
        queue.create_schedule(
            tenant_id="gone", interval_seconds=10, payload={}
        )
    with pytest.raises(ValueError, match="offboarding"):
        queue.register_webhook(tenant_id="gone", url="http://example.test/x")
    with pytest.raises(ValueError, match="offboarding"):
        queue.resume_tenant("gone")

    # No new leases.
    assert queue.lease(tenant_id="gone", worker_id="w2") is None
    assert queue.lease(worker_id="fair") is None  # fair also skips

    # In-flight may heartbeat + ack.
    _hb(queue, leased, "w1", extend_by_seconds=30)
    done = _ack(queue, leased, "w1", result={"ok": True})
    assert done.status == JobStatus.COMPLETED

    # After last lease finishes, tenant is fully empty.
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False
    assert queue.get(leased.id, tenant_id="gone") is None


def test_offboard_in_flight_nack_then_empties(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="gone",
        payload={"x": 1},
        max_attempts=3,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None and leased.id == j.id

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1
    assert result.empty is False

    # Nack (even with requeue) must not leave residual work.
    nacked = _nack(queue, leased, "w1", error="boom", requeue=True)
    # Either requeued-then-purged or terminalized; tenant empty either way.
    assert queue.tenant_is_empty("gone") is True
    assert queue.get(nacked.id, tenant_id="gone") is None


def test_offboard_multiple_in_flight_empties_after_last(queue: JobQueue):
    queue.set_concurrency_limit("gone", 5)
    for i in range(3):
        queue.enqueue(tenant_id="gone", payload={"i": i})
    jobs = []
    for i in range(3):
        j = queue.lease(tenant_id="gone", worker_id=f"w{i}")
        assert j is not None
        jobs.append(j)

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 3
    assert result.empty is False

    _ack(queue, jobs[0], "w0")
    assert queue.tenant_is_empty("gone") is False
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 2

    _ack(queue, jobs[1], "w1")
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 1

    _ack(queue, jobs[2], "w2")
    assert queue.tenant_is_empty("gone") is True


def test_offboard_reaps_expired_abandoned_lease(queue: JobQueue):
    """Expired leases on offboarding tenants must be cancelled (not stuck)."""
    queue.enqueue(
        tenant_id="gone",
        payload={"x": 1},
        visibility_timeout_seconds=1,
        max_attempts=5,
    )
    leased = queue.lease(
        tenant_id="gone",
        worker_id="zombie",
        visibility_timeout_seconds=1,
    )
    assert leased is not None

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1

    # Wait for lease expiry; worker is gone and cannot ack.
    time.sleep(1.2)

    # Any lease attempt reaps expired offboarding leases.
    assert queue.lease(worker_id="other") is None
    assert queue.tenant_is_empty("gone") is True


def test_offboard_idempotent_when_already_empty(queue: JobQueue):
    result = queue.offboard_tenant("never-existed")
    assert result.empty is True
    assert result.pending_cancelled == 0
    assert result.leased_remaining == 0
    assert queue.tenant_is_empty("never-existed") is True

    # Second call still fine.
    result2 = queue.offboard_tenant("never-existed")
    assert result2.empty is True


def test_offboard_cancel_running_job_can_empty(queue: JobQueue):
    queue.enqueue(tenant_id="gone", payload={"x": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    token = leased.lease_token

    queue.offboard_tenant("gone")
    cancelled = queue.cancel(leased.id, tenant_id="gone")
    assert cancelled.status == JobStatus.CANCELLED
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="gone",
            worker_id="w1",
            lease_token=token,  # type: ignore[arg-type]
        )
    assert queue.tenant_is_empty("gone") is True


def test_offboard_keeps_claimed_webhook_for_finalize(queue: JobQueue):
    """Claimed delivering webhooks survive offboard so finalize can complete."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json
    import threading

    received: list[dict] = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(n)
            received.append(json.loads(body.decode("utf-8")))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            return

    sink = HTTPServer(("127.0.0.1", 0), H)
    port = sink.server_address[1]
    threading.Thread(target=sink.serve_forever, daemon=True).start()
    sink_url = f"http://127.0.0.1:{port}/hook"

    queue.register_webhook(
        tenant_id="gone",
        url=sink_url,
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1", result={"ok": True})

    pending = queue.list_webhook_deliveries(tenant_id="gone", status="pending")
    assert len(pending) == 1

    # Agent claims before offboard (or mid-flight).
    claimed, auto = queue.claim_webhook_deliveries(limit=10, claim_seconds=30.0)
    assert auto == []
    assert len(claimed) == 1
    item = claimed[0]
    assert item.delivery.status == "delivering"
    assert item.url == sink_url
    delivery_id = item.delivery.id

    # Also leave a non-claimed pending delivery that should be purged.
    # (Create via another completed job after claim is hard because pause —
    # instead ensure only the delivering one remains after offboard.)
    result = queue.offboard_tenant("gone")
    assert result.empty is False
    assert result.offboarding is True
    assert result.webhooks_delivering_remaining == 1
    # Endpoint kept while claim is live (not counted as deleted).
    assert result.webhooks_deleted == 0
    assert queue.tenant_is_empty("gone") is False

    # Delivery still exists and is still delivering.
    still = queue.get_webhook_delivery(delivery_id, tenant_id="gone")
    assert still is not None
    assert still.status == "delivering"
    # Endpoint still present (disabled) so FK holds.
    eps = queue.list_webhooks(tenant_id="gone")
    assert len(eps) == 1
    assert eps[0].enabled is False

    # Agent POSTs with in-memory claim (as real agents do).
    from job_queue.webhooks import post_webhook

    code, _ = post_webhook(
        item.url,
        item.delivery.payload,
        secret=item.secret,
        timeout_seconds=5.0,
        idempotency_key=str(delivery_id),
    )
    assert 200 <= code < 300

    fin = queue.finalize_webhook_delivery(delivery_id, status_code=code)
    assert fin is not None
    assert fin.success is True
    assert received  # sink got the POST

    # After last claimed webhook finalizes, tenant is fully empty.
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False
    assert queue.get_webhook_delivery(delivery_id, tenant_id="gone") is None


def test_offboard_finalize_failed_webhook_does_not_requeue(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://127.0.0.1:1/nope",  # nothing listening
        events=["job.completed"],
        max_attempts=5,
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=30.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id

    result = queue.offboard_tenant("gone")
    assert result.webhooks_delivering_remaining == 1

    # Failed POST finalize during offboard → dead, not pending retry.
    fin = queue.finalize_webhook_delivery(
        did, status_code=500, error="boom"
    )
    assert fin is not None
    assert fin.success is False
    # Tenant emptied (dead delivery purged on finalize-offboard).
    assert queue.tenant_is_empty("gone") is True


def test_offboard_reaps_expired_claimed_webhook(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=1.0)
    assert len(claimed) == 1

    result = queue.offboard_tenant("gone")
    assert result.webhooks_delivering_remaining == 1

    time.sleep(1.2)
    # Claim poll reaps abandoned delivering claims for offboarding tenants.
    claimed2, auto = queue.claim_webhook_deliveries(limit=5)
    assert claimed2 == []
    assert queue.tenant_is_empty("gone") is True


def test_offboard_waits_for_both_lease_and_webhook(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    # Job that will complete and enqueue webhook, then we claim webhook and
    # lease a second job so both in-flight types coexist at offboard time.
    j1 = queue.enqueue(tenant_id="gone", payload={"phase": 1}, priority=10)
    j2 = queue.enqueue(tenant_id="gone", payload={"phase": 2}, priority=1)
    l1 = queue.lease(tenant_id="gone", worker_id="w1")
    assert l1 is not None and l1.id == j1.id
    _ack(queue, l1, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=60.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id

    l2 = queue.lease(tenant_id="gone", worker_id="w2")
    assert l2 is not None and l2.id == j2.id

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1
    assert result.webhooks_delivering_remaining == 1
    assert result.empty is False

    # Finish webhook first — still not empty (lease remains).
    fin = queue.finalize_webhook_delivery(did, status_code=200)
    assert fin is not None and fin.success
    assert queue.tenant_is_empty("gone") is False
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 1

    # Finish lease last — now empty.
    _ack(queue, l2, "w2")
    assert queue.tenant_is_empty("gone") is True


# ---------------------------------------------------------------------------
# Tenant merge (ownership transfer A → B)
# ---------------------------------------------------------------------------


def test_merge_moves_pending_jobs_and_empties_source(queue: JobQueue):
    from job_queue import TenantMergeResult

    a1 = queue.enqueue(tenant_id="alpha", payload={"i": 1})
    a2 = queue.enqueue(tenant_id="alpha", payload={"i": 2})
    b1 = queue.enqueue(tenant_id="beta", payload={"i": 10})

    result = queue.merge_tenant("alpha", "beta")
    assert isinstance(result, TenantMergeResult)
    assert result.source_tenant_id == "alpha"
    assert result.target_tenant_id == "beta"
    assert result.jobs_moved == 2
    assert result.jobs_leased_moved == 0
    assert result.source_empty is True
    assert queue.tenant_is_empty("alpha") is True

    assert queue.count(tenant_id="alpha") == 0
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 3
    assert queue.get(a1.id, tenant_id="alpha") is None
    assert queue.get(a1.id, tenant_id="beta") is not None
    assert queue.get(a2.id, tenant_id="beta") is not None
    assert queue.get(b1.id, tenant_id="beta") is not None


def test_merge_moves_running_job_without_stopping_worker(queue: JobQueue):
    """Worker still heartbeats/acks with pre-merge tenant via lease_tenant_id."""
    job = queue.enqueue(tenant_id="alpha", payload={"run": True})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    assert leased.tenant_id == "alpha"
    assert leased.lease_tenant_id == "alpha"
    assert leased.lease_token is not None

    result = queue.merge_tenant("alpha", "beta")
    assert result.jobs_moved == 1
    assert result.jobs_leased_moved == 1
    assert queue.tenant_is_empty("alpha") is True

    # Row now lives under beta; claim tenant remains alpha.
    moved = queue.get(leased.id, tenant_id="beta")
    assert moved is not None
    assert moved.status == JobStatus.LEASED
    assert moved.tenant_id == "beta"
    assert moved.lease_tenant_id == "alpha"
    assert moved.lease_token == leased.lease_token
    assert moved.leased_by == "w1"

    # Pre-merge tenant still works (lease_tenant_id).
    hb = queue.heartbeat(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert hb.tenant_id == "beta"
    assert hb.lease_tenant_id == "alpha"

    # Current owner also works.
    hb2 = queue.heartbeat(
        leased.id,
        tenant_id="beta",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert hb2.tenant_id == "beta"

    # Unrelated third tenant still rejected even with valid token.
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.heartbeat(
            leased.id,
            tenant_id="gamma",
            worker_id="w1",
            lease_token=leased.lease_token,
        )

    held = queue.ensure_lease(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert held.tenant_id == "beta"
    assert held.lease_tenant_id == "alpha"

    acked = queue.ack(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    assert acked.status == JobStatus.COMPLETED
    assert acked.tenant_id == "beta"
    assert acked.lease_tenant_id is None
    assert queue.get_result(acked.id, tenant_id="beta") == {"ok": True}


def test_merge_moves_schedules_and_webhooks(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="alpha",
        name="nightly",
        interval_seconds=60,
        payload={"s": 1},
    )
    ep = queue.register_webhook(
        tenant_id="alpha",
        url="http://example.test/a",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="alpha", payload={"n": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    deliveries = queue.list_webhook_deliveries(tenant_id="alpha")
    assert len(deliveries) >= 1

    result = queue.merge_tenant("alpha", "beta")
    assert result.schedules_moved == 1
    assert result.webhook_endpoints_moved == 1
    assert result.webhook_deliveries_moved >= 1
    assert queue.tenant_is_empty("alpha") is True

    assert queue.get_schedule(sched.id, tenant_id="alpha") is None
    moved_sched = queue.get_schedule(sched.id, tenant_id="beta")
    assert moved_sched is not None
    assert moved_sched.name == "nightly"
    assert moved_sched.tenant_id == "beta"

    eps = queue.list_webhooks(tenant_id="beta")
    assert any(e.id == ep.id for e in eps)
    assert queue.list_webhooks(tenant_id="alpha") == []

    beta_dels = queue.list_webhook_deliveries(tenant_id="beta")
    assert any(d.job_id == job.id for d in beta_dels)


def test_merge_renames_conflicting_schedule_names(queue: JobQueue):
    queue.create_schedule(
        tenant_id="alpha",
        name="shared",
        interval_seconds=60,
        payload={"from": "a"},
    )
    queue.create_schedule(
        tenant_id="beta",
        name="shared",
        interval_seconds=60,
        payload={"from": "b"},
    )
    result = queue.merge_tenant("alpha", "beta")
    assert result.schedule_names_renamed == 1
    names = {s.name for s in queue.list_schedules(tenant_id="beta")}
    assert "shared" in names
    assert "shared__from_alpha" in names


def test_merge_clears_conflicting_active_dedupe_keys(queue: JobQueue):
    ja = queue.enqueue(tenant_id="alpha", payload={"a": 1}, dedupe_key="k")
    jb = queue.enqueue(tenant_id="beta", payload={"b": 1}, dedupe_key="k")
    result = queue.merge_tenant("alpha", "beta")
    assert result.dedupe_keys_cleared == 1
    # Both jobs still present under beta; source key cleared.
    ma = queue.get(ja.id, tenant_id="beta")
    mb = queue.get(jb.id, tenant_id="beta")
    assert ma is not None and mb is not None
    assert mb.dedupe_key == "k"
    assert ma.dedupe_key is None


def test_merge_rejects_same_tenant_and_offboarding_target(queue: JobQueue):
    from job_queue import MergeError

    with pytest.raises(ValueError, match="must differ"):
        queue.merge_tenant("a", "a")

    queue.enqueue(tenant_id="src", payload={"x": 1})
    queue.offboard_tenant("tgt")  # empty offboard still marks? empty becomes empty without settings
    # Force target into offboarding with residual leased work.
    queue.enqueue(tenant_id="tgt", payload={"y": 1})
    leased = queue.lease(tenant_id="tgt", worker_id="w1")
    assert leased is not None
    queue.offboard_tenant("tgt")
    assert queue.is_tenant_offboarding("tgt") is True

    with pytest.raises(MergeError, match="offboarding"):
        queue.merge_tenant("src", "tgt")


def test_merge_rejects_when_target_pending_cap_would_exceed(queue: JobQueue):
    from job_queue import MergeError

    queue.set_pending_limit("beta", 2)
    queue.enqueue(tenant_id="beta", payload={"b": 1})
    queue.enqueue(tenant_id="alpha", payload={"a": 1})
    queue.enqueue(tenant_id="alpha", payload={"a": 2})

    with pytest.raises(MergeError, match="pending job cap"):
        queue.merge_tenant("alpha", "beta")

    # Source still intact (transaction rolled back).
    assert queue.count(tenant_id="alpha", status=JobStatus.PENDING) == 2
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 1


def test_merge_blocks_source_enqueue_after(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={"a": 1})
    queue.merge_tenant("alpha", "beta")
    # Source has no residual state; new enqueue under alpha is a fresh tenant.
    j = queue.enqueue(tenant_id="alpha", payload={"fresh": True})
    assert j.tenant_id == "alpha"
    assert queue.count(tenant_id="alpha") == 1


def test_merge_claimed_webhook_finalize_after_transfer(queue: JobQueue):
    queue.register_webhook(
        tenant_id="alpha",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="alpha", payload={"n": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=60.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id
    assert claimed[0].delivery.tenant_id == "alpha"

    queue.merge_tenant("alpha", "beta")
    assert queue.tenant_is_empty("alpha") is True

    fin = queue.finalize_webhook_delivery(did, status_code=200)
    assert fin is not None and fin.success
    d = queue.get_webhook_delivery(did, tenant_id="beta")
    assert d is not None
    assert d.status == "succeeded"
    assert d.job_id == job.id


def test_merge_idempotent_empty_source(queue: JobQueue):
    result = queue.merge_tenant("never-had-work", "beta")
    assert result.jobs_moved == 0
    assert result.source_empty is True
    assert queue.tenant_is_empty("never-had-work") is True


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


# ---------------------------------------------------------------------------
# Deadlines + preemption
# ---------------------------------------------------------------------------



def test_deadline_preempts_low_priority_at_concurrency_cap(queue: JobQueue):
    """At cap, urgent deadline job preempts long low-urgency leased work."""
    from datetime import timedelta

    queue.set_concurrency_limit("t1", 1)
    long = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "long", "steps": 50, "step_ms": 200},
        priority=0,
        estimated_duration_seconds=100,
        preemptible=True,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w-low")
    assert leased is not None and leased.id == long.id
    assert leased.lease_token is not None
    # Save progress so resume is meaningful.
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w-low",
        lease_token=leased.lease_token,
        progress={"step": 4, "total": 50},
    )

    deadline = _utcnow() + timedelta(seconds=8)
    urgent = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "urgent", "steps": 2, "step_ms": 50},
        priority=0,
        deadline_at=deadline,
        estimated_duration_seconds=2,
    )
    # Cap full — lease should preempt long job then claim urgent.
    got = queue.lease(tenant_id="t1", worker_id="w-urgent")
    assert got is not None
    assert got.id == urgent.id
    assert got.leased_by == "w-urgent"

    parked = queue.get(long.id, tenant_id="t1")
    assert parked is not None
    assert parked.status == JobStatus.PENDING
    assert parked.checkpoint == {"step": 4, "total": 50}
    assert parked.preempt_count == 1
    assert parked.lease_token is None

    # Old holder cannot finish (fence).
    with pytest.raises(LeaseError):
        queue.ack(
            long.id,
            tenant_id="t1",
            worker_id="w-low",
            lease_token=leased.lease_token,
            result={"wrong": True},
        )
    with pytest.raises(LeaseError):
        queue.heartbeat(
            long.id,
            tenant_id="t1",
            worker_id="w-low",
            lease_token=leased.lease_token,
        )

    # Finish urgent; long resumes from checkpoint.
    queue.ack(
        got.id,
        tenant_id="t1",
        worker_id="w-urgent",
        lease_token=got.lease_token,
        result={"ok": True},
    )
    resumed = queue.lease(tenant_id="t1", worker_id="w-resume")
    assert resumed is not None and resumed.id == long.id
    assert resumed.checkpoint == {"step": 4, "total": 50}


def test_process_deadlines_preempts_proactively(queue: JobQueue):
    from datetime import timedelta
    from job_queue import PreemptResult

    queue.set_concurrency_limit("t1", 1)
    low = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "bulk"},
        estimated_duration_seconds=120,
    )
    l = queue.lease(tenant_id="t1", worker_id="w1")
    assert l is not None and l.id == low.id

    u = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "page"},
        deadline_at=_utcnow() + timedelta(seconds=5),
        estimated_duration_seconds=3,
    )
    results = queue.process_deadlines(tenant_id="t1")
    assert len(results) == 1
    assert isinstance(results[0], PreemptResult)
    assert results[0].urgent_job_id == u.id
    assert results[0].victim_job_id == low.id
    assert queue.get(low.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0


def test_no_preempt_when_can_wait_for_holder(queue: JobQueue):
    """If less-urgent work finishes before slack runs out, do not preempt."""
    from datetime import timedelta

    queue.set_concurrency_limit("t1", 1)
    short = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "short"},
        estimated_duration_seconds=1,  # almost done
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    # Far deadline, small remaining on holder → can wait.
    far = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "later"},
        deadline_at=_utcnow() + timedelta(seconds=120),
        estimated_duration_seconds=5,
    )
    results = queue.process_deadlines(tenant_id="t1")
    assert results == []
    still = queue.get(short.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.LEASED
    # Cap still full for new lease of far job.
    assert queue.lease(tenant_id="t1", worker_id="w2") is None
    assert queue.get(far.id, tenant_id="t1").status == JobStatus.PENDING


def test_non_preemptible_not_victim(queue: JobQueue):
    from datetime import timedelta

    queue.set_concurrency_limit("t1", 1)
    protected = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "crit-running"},
        estimated_duration_seconds=200,
        preemptible=False,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.id == protected.id
    queue.enqueue(
        tenant_id="t1",
        payload={"kind": "urgent"},
        deadline_at=_utcnow() + timedelta(seconds=3),
        estimated_duration_seconds=2,
    )
    assert queue.process_deadlines(tenant_id="t1") == []
    assert queue.get(protected.id, tenant_id="t1").status == JobStatus.LEASED


def test_preempt_count_caps_thrashing(queue: JobQueue):
    from datetime import timedelta
    from job_queue.queue import _MAX_PREEMPTS_PER_JOB

    queue.set_concurrency_limit("t1", 1)
    victim = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "v"},
        estimated_duration_seconds=500,
    )
    # Artificially max out preempt_count via repeated preempt path.
    for i in range(_MAX_PREEMPTS_PER_JOB):
        l = queue.lease(tenant_id="t1", worker_id=f"w{i}")
        assert l is not None
        u = queue.enqueue(
            tenant_id="t1",
            payload={"kind": f"u{i}"},
            deadline_at=_utcnow() + timedelta(seconds=2),
            estimated_duration_seconds=1,
        )
        pr = queue.process_deadlines(tenant_id="t1")
        assert len(pr) == 1
        # Complete urgent so we can re-lease victim.
        ug = queue.lease(tenant_id="t1", worker_id=f"wu{i}")
        assert ug is not None and ug.id == u.id
        queue.ack(
            ug.id,
            tenant_id="t1",
            worker_id=f"wu{i}",
            lease_token=ug.lease_token,
        )
    # Victim at max preempts — one more urgent must not preempt it.
    l = queue.lease(tenant_id="t1", worker_id="w-final")
    assert l is not None and l.id == victim.id
    assert l.preempt_count >= _MAX_PREEMPTS_PER_JOB
    queue.enqueue(
        tenant_id="t1",
        payload={"kind": "final-u"},
        deadline_at=_utcnow() + timedelta(seconds=2),
        estimated_duration_seconds=1,
    )
    assert queue.process_deadlines(tenant_id="t1") == []


def test_deadline_ordering_among_pending(queue: JobQueue):
    from datetime import timedelta

    now = _utcnow()
    late = queue.enqueue(
        tenant_id="t1",
        payload={"k": "late"},
        priority=5,
        deadline_at=now + timedelta(seconds=60),
        estimated_duration_seconds=5,
    )
    soon = queue.enqueue(
        tenant_id="t1",
        payload={"k": "soon"},
        priority=5,
        deadline_at=now + timedelta(seconds=10),
        estimated_duration_seconds=5,
    )
    first = queue.lease(tenant_id="t1", worker_id="w1")
    assert first is not None and first.id == soon.id
    second = queue.lease(tenant_id="t1", worker_id="w1")
    assert second is not None and second.id == late.id


def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


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


# ---------------------------------------------------------------------------
# Immutable job audit stream
# ---------------------------------------------------------------------------


def test_audit_enqueued_leased_completed(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert len(events) == 1
    assert events[0].event_type == "enqueued"
    assert events[0].seq == 1
    assert events[0].to_status == "pending"

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued", "leased"]
    assert events[-1].to_status == "leased"
    assert events[-1].seq == 2

    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued", "leased", "completed"]
    assert events[-1].to_status == "completed"
    report = queue.verify_job_audit(job.id, tenant_id="t1")
    assert report.ok is True
    assert report.event_count == 3
    assert report.job_status == "completed"
    assert report.audit_final_status == "completed"


def test_audit_matches_after_nack_cancel_checkpoint(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    queue.nack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        error="boom",
        requeue=True,
    )
    types = [e.event_type for e in queue.list_job_audit(job.id, tenant_id="t1")]
    assert types == ["enqueued", "leased", "checkpointed", "requeued"]
    assert queue.verify_job_audit(job.id, tenant_id="t1").ok

    leased2 = queue.lease(tenant_id="t1", worker_id="w2")
    assert leased2 is not None
    queue.cancel(leased2.id, tenant_id="t1")
    assert queue.verify_job_audit(job.id, tenant_id="t1").ok
    assert queue.get(job.id, tenant_id="t1").status == JobStatus.CANCELLED
    final = queue.list_job_audit(job.id, tenant_id="t1")[-1]
    assert final.event_type == "cancelled"
    assert final.to_status == "cancelled"


def test_audit_tenant_scoped_and_after_seq(queue: JobQueue):
    j = queue.enqueue(tenant_id="alpha", payload={})
    queue.enqueue(tenant_id="beta", payload={})
    events = queue.list_job_audit(j.id, tenant_id="alpha")
    assert len(events) == 1
    assert queue.list_job_audit(j.id, tenant_id="beta") == []
    more = queue.list_job_audit(j.id, tenant_id="alpha", after_seq=1)
    assert more == []
    stream = queue.list_tenant_audit("alpha")
    assert any(e.job_id == j.id for e in stream)


def test_verify_tenant_audit_all_ok(queue: JobQueue):
    ids = []
    for i in range(5):
        j = queue.enqueue(tenant_id="t1", payload={"i": i})
        ids.append(j.id)
        leased = queue.lease(tenant_id="t1", worker_id="w1")
        assert leased is not None
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    reports = queue.verify_tenant_audit("t1")
    assert len(reports) == 5
    assert all(r.ok for r in reports)


def test_audit_history_survives_tenant_merge(queue: JobQueue):
    """After merge, target owner sees full seq including pre-merge events."""
    job = queue.enqueue(tenant_id="alpha", payload={"m": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    pre = queue.list_job_audit(job.id, tenant_id="alpha")
    assert [e.event_type for e in pre] == ["enqueued", "leased", "checkpointed"]
    assert all(e.tenant_id == "alpha" for e in pre)
    assert [e.seq for e in pre] == [1, 2, 3]

    queue.merge_tenant("alpha", "beta")
    assert queue.get(job.id, tenant_id="alpha") is None
    moved = queue.get(job.id, tenant_id="beta")
    assert moved is not None
    assert moved.status == JobStatus.LEASED

    # Target sees full history; pre-merge rows still stamped alpha.
    full = queue.list_job_audit(job.id, tenant_id="beta")
    types = [e.event_type for e in full]
    assert types[:3] == ["enqueued", "leased", "checkpointed"]
    assert "tenant_merged" in types
    assert [e.seq for e in full] == list(range(1, len(full) + 1))
    assert full[0].tenant_id == "alpha"
    merge_ev = next(e for e in full if e.event_type == "tenant_merged")
    assert merge_ev.tenant_id == "beta"
    assert merge_ev.detail.get("from_tenant") == "alpha"
    assert merge_ev.detail.get("to_tenant") == "beta"

    # Source no longer owns the job → no access via list_job_audit.
    assert queue.list_job_audit(job.id, tenant_id="alpha") == []

    # Unrelated tenant blocked.
    assert queue.list_job_audit(job.id, tenant_id="gamma") == []

    report = queue.verify_job_audit(job.id, tenant_id="beta")
    assert report.ok is True
    assert report.event_count >= 4
    assert report.job_status == "leased"
    assert report.audit_final_status == "leased"

    # Tenant-wide catch-up for beta includes pre-merge events for owned jobs.
    stream = queue.list_tenant_audit("beta", limit=100)
    job_events = [e for e in stream if e.job_id == job.id]
    assert len(job_events) >= 4
    assert any(e.tenant_id == "alpha" for e in job_events)

    # Finish under beta; history remains contiguous.
    queue.ack(
        moved.id,
        tenant_id="beta",
        worker_id="w1",
        lease_token=moved.lease_token,
        result={"ok": True},
    )
    final = queue.list_job_audit(job.id, tenant_id="beta")
    assert final[-1].event_type == "completed"
    assert final[-1].seq == len(final)
    assert queue.verify_job_audit(job.id, tenant_id="beta").ok


# ---------------------------------------------------------------------------
# Rebuild job from audit after row loss
# ---------------------------------------------------------------------------


def test_rebuild_job_from_audit_after_delete(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"k": "v", "n": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 2, "total": 5},
    )
    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    jid = job.id
    # Simulate partial write / bad migrate: drop job row, keep audit.
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    assert queue.get(jid, tenant_id="t1") is None
    orphans = queue.list_orphaned_audit_jobs(tenant_id="t1")
    assert jid in orphans

    rebuilt = queue.rebuild_job_from_audit(jid, tenant_id="t1")
    assert rebuilt.id == jid
    assert rebuilt.tenant_id == "t1"
    assert rebuilt.status == JobStatus.COMPLETED
    assert rebuilt.payload == {"k": "v", "n": 1}
    assert rebuilt.checkpoint == {"step": 2, "total": 5}
    assert rebuilt.result == {"ok": True}
    assert queue.verify_job_audit(jid, tenant_id="t1").ok

    # Refuse overwrite if row exists.
    with pytest.raises(RebuildError, match="already exists"):
        queue.rebuild_job_from_audit(jid, tenant_id="t1")


def test_rebuild_after_merge_uses_target_tenant(queue: JobQueue):
    job = queue.enqueue(tenant_id="alpha", payload={"x": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    queue.merge_tenant("alpha", "beta")
    jid = job.id
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()

    with pytest.raises(Exception) as ei:
        queue.rebuild_job_from_audit(jid, tenant_id="alpha")
    assert "rebuild owner" in str(ei.value).lower() or "not" in str(ei.value).lower()

    rebuilt = queue.rebuild_job_from_audit(jid, tenant_id="beta")
    assert rebuilt.tenant_id == "beta"
    assert rebuilt.checkpoint == {"step": 1}
    assert rebuilt.status == JobStatus.LEASED
    events = queue.list_job_audit(jid, tenant_id="beta")
    assert events[0].tenant_id == "alpha"
    assert any(e.event_type == "tenant_merged" for e in events)
    assert events[-1].event_type == "rebuilt"
    assert queue.verify_job_audit(jid, tenant_id="beta").ok


def test_rebuild_rejects_corrupt_audit(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    jid = job.id
    with queue._pool.connection() as conn:
        # Corrupt: wipe snapshot from last event.
        conn.execute(
            """
            UPDATE job_audit_events
            SET detail = '{}'::jsonb
            WHERE job_id = %s AND seq = 1
            """,
            (jid,),
        )
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    with pytest.raises(RebuildError, match="missing detail.job"):
        queue.rebuild_job_from_audit(jid)


def test_rebuild_rejects_seq_gap(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    jid = job.id
    with queue._pool.connection() as conn:
        conn.execute(
            "DELETE FROM job_audit_events WHERE job_id = %s AND seq = 2",
            (jid,),
        )
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    with pytest.raises(RebuildError, match="seq gap"):
        queue.rebuild_job_from_audit(jid)
