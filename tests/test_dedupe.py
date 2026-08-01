"""Deduplication."""

from __future__ import annotations

import threading

import pytest

from job_queue import JobQueue, JobStatus

from tests.helpers_queue import (
    _ack,
)

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
