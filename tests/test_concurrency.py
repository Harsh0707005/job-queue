"""Per-tenant concurrency limits."""

from __future__ import annotations

import threading
import time

import pytest

from job_queue import JobQueue, JobStatus

from tests.helpers_queue import (
    _ack,
)

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
    assert stats.enqueue_rate_per_sec is None
    assert stats.lease_rate_per_sec is None


def test_enqueue_rate_limit_rejects_burst_flood(queue: JobQueue):
    from job_queue import RateLimitError

    queue.set_enqueue_rate_limit("noisy", 2.0, burst=2)
    rate, burst = queue.get_enqueue_rate_limit("noisy")
    assert rate == 2.0 and burst == 2
    queue.enqueue(tenant_id="noisy", payload={"i": 1})
    queue.enqueue(tenant_id="noisy", payload={"i": 2})
    with pytest.raises(RateLimitError, match="enqueue rate limit") as ei:
        queue.enqueue(tenant_id="noisy", payload={"i": 3})
    assert ei.value.kind == "enqueue"
    assert ei.value.tenant_id == "noisy"
    assert ei.value.retry_after_seconds is not None
    assert ei.value.retry_after_seconds > 0
    # Quiet tenant is unaffected.
    queue.enqueue(tenant_id="quiet", payload={"ok": True})
    # After refill window, enqueue works again.
    time.sleep(0.6)
    queue.enqueue(tenant_id="noisy", payload={"i": 4})


def test_enqueue_rate_limit_clear(queue: JobQueue):
    from job_queue import RateLimitError

    queue.set_enqueue_rate_limit("t1", 1.0, burst=1)
    queue.enqueue(tenant_id="t1", payload={"i": 1})
    with pytest.raises(RateLimitError):
        queue.enqueue(tenant_id="t1", payload={"i": 2})
    queue.set_enqueue_rate_limit("t1", None)
    assert queue.get_enqueue_rate_limit("t1") == (None, None)
    queue.enqueue(tenant_id="t1", payload={"i": 2})


def test_lease_rate_limit_scoped_partial_batch(queue: JobQueue):
    queue.set_lease_rate_limit("t1", 1.0, burst=1)
    for i in range(5):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    first = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=5)
    assert len(first) == 1
    # Still pending others; no more tokens yet.
    second = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=5)
    assert second == []
    time.sleep(1.1)
    third = queue.lease_batch(tenant_id="t1", worker_id="w1", limit=5)
    assert len(third) == 1


def test_lease_rate_limit_fair_rotates_to_other_tenants(queue: JobQueue):
    """Noisy tenant at lease rate cap does not starve fair multi-tenant lease."""
    queue.set_lease_rate_limit("noisy", 1.0, burst=1)
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=10)
    for i in range(5):
        queue.enqueue(tenant_id="quiet", payload={"i": i}, priority=0)

    # First fair claim may go to noisy (higher priority).
    got = queue.lease_batch(worker_id="fair", limit=6)
    tenants = {j.tenant_id for j in got}
    # With lease rate 1, noisy can take at most 1; rest should be quiet.
    noisy_n = sum(1 for j in got if j.tenant_id == "noisy")
    quiet_n = sum(1 for j in got if j.tenant_id == "quiet")
    assert noisy_n <= 1
    assert quiet_n >= 1
    assert "quiet" in tenants


def test_lease_rate_limit_does_not_block_reclaim(queue: JobQueue):
    queue.set_lease_rate_limit("t1", 1.0, burst=1)
    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=1,
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    j1 = queue.lease(tenant_id="t1", worker_id="w1", visibility_timeout_seconds=1)
    assert j1 is not None and j1.id == job.id
    # Burst spent; pending lease would be blocked, but reclaim after expiry is free.
    queue.enqueue(tenant_id="t1", payload={"other": 1})
    assert queue.lease(tenant_id="t1", worker_id="w2") is None
    time.sleep(1.2)
    reclaimed = queue.lease(
        tenant_id="t1", worker_id="w2", visibility_timeout_seconds=30
    )
    assert reclaimed is not None
    assert reclaimed.id == job.id


def test_rate_limit_tenant_stats(queue: JobQueue):
    queue.set_enqueue_rate_limit("t1", 5.0, burst=10)
    queue.set_lease_rate_limit("t1", 3.0, burst=3)
    stats = queue.get_tenant_stats("t1")
    assert stats.enqueue_rate_per_sec == 5.0
    assert stats.enqueue_burst == 10
    assert stats.lease_rate_per_sec == 3.0
    assert stats.lease_burst == 3


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
