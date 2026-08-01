"""Delayed jobs."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue

from tests.helpers_queue import (
    _ack,
)

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
