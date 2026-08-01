"""Worker helper."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, Worker

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
