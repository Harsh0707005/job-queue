"""Job results on ack."""

from __future__ import annotations

from uuid import uuid4

from job_queue import Job, JobQueue, JobStatus, Worker

from tests.helpers_queue import (
    _ack,
)

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
