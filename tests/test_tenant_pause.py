"""Tenant pause / resume / metrics."""

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
    _nack,
)

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

