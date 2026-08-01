"""Deadlines + preemption."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus, LeaseError

from tests.helpers_queue import (
    _utcnow,
)

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
    from job_queue.queue._common import _MAX_PREEMPTS_PER_JOB

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
