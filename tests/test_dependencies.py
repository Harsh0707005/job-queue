"""Job dependencies."""

from __future__ import annotations

import threading
import time

import pytest

from job_queue import Job, JobQueue, JobStatus, Worker

from tests.helpers_queue import (
    _ack,
    _nack,
)

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


def test_enqueue_parent_cancel_during_insert_cascades(queue: JobQueue):
    """Cancel overlapping a depends_on enqueue must not leave a pending child."""
    parent = queue.enqueue(tenant_id="dep-race", payload={"p": 1})
    orig = queue._validate_dependencies
    cancel_thread: threading.Thread | None = None

    def validate_then_cancel(conn, *, tenant_id, depends_on):
        nonlocal cancel_thread
        orig(conn, tenant_id=tenant_id, depends_on=depends_on)
        started = threading.Event()

        def cancel_parent() -> None:
            started.set()
            queue.cancel(parent.id, tenant_id=tenant_id)

        cancel_thread = threading.Thread(target=cancel_parent)
        cancel_thread.start()
        assert started.wait(timeout=5)
        time.sleep(0.15)
        assert cancel_thread is not None and cancel_thread.is_alive()

    queue._validate_dependencies = validate_then_cancel  # type: ignore[method-assign]
    try:
        child = queue.enqueue(
            tenant_id="dep-race",
            payload={"c": 1},
            depends_on=parent.id,
        )
    finally:
        queue._validate_dependencies = orig  # type: ignore[method-assign]

    assert cancel_thread is not None
    cancel_thread.join(timeout=10)
    assert not cancel_thread.is_alive()
    parent_after = queue.get(parent.id, tenant_id="dep-race")
    child_after = queue.get(child.id, tenant_id="dep-race")
    assert parent_after is not None and parent_after.status == JobStatus.CANCELLED
    assert child_after is not None and child_after.status == JobStatus.CANCELLED


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
