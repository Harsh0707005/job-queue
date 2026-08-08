"""Job required_skills vs worker capabilities on lease."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

from job_queue import JobQueue
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_enqueue_required_skills_normalized(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        required_skills=[" gpu ", "cuda", "gpu"],
    )
    assert j.required_skills == ("cuda", "gpu")
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None
    assert got.required_skills == ("cuda", "gpu")


def test_unskilled_job_still_leases_without_registration(queue: JobQueue):
    j = queue.enqueue(tenant_id="t1", payload={"plain": True})
    leased = queue.lease(tenant_id="t1", worker_id="anon")
    assert leased is not None
    assert leased.id == j.id
    assert leased.required_skills == ()


def test_unregistered_worker_cannot_lease_skilled_job(queue: JobQueue):
    queue.enqueue(
        tenant_id="t1", payload={"need": "gpu"}, required_skills=["gpu"]
    )
    assert queue.lease(tenant_id="t1", worker_id="anon") is None
    fair = queue.lease(worker_id="anon")
    assert fair is None


def test_worker_must_have_all_required_skills(queue: JobQueue):
    gpu_only = queue.enqueue(
        tenant_id="t1", payload={"k": "gpu"}, required_skills=["gpu"]
    )
    both = queue.enqueue(
        tenant_id="t1",
        payload={"k": "both"},
        required_skills=["gpu", "cuda"],
    )
    queue.register_worker("w-gpu", skills=["gpu"])
    got = queue.lease(tenant_id="t1", worker_id="w-gpu")
    assert got is not None and got.id == gpu_only.id
    assert queue.lease(tenant_id="t1", worker_id="w-gpu") is None

    queue.register_worker("w-cuda", skills=["gpu", "cuda"])
    got2 = queue.lease(tenant_id="t1", worker_id="w-cuda")
    assert got2 is not None and got2.id == both.id


def test_fair_lease_skips_unmatched_and_serves_other_tenants(queue: JobQueue):
    queue.enqueue(
        tenant_id="ta", payload={"gpu": 1}, required_skills=["gpu"], priority=10
    )
    cpu = queue.enqueue(
        tenant_id="tb", payload={"cpu": 1}, required_skills=["cpu"], priority=0
    )
    queue.register_worker("w-cpu", skills=["cpu"])
    leased = queue.lease(worker_id="w-cpu")
    assert leased is not None
    assert leased.id == cpu.id
    assert leased.tenant_id == "tb"


def test_heartbeat_can_add_skills_live(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1", payload={"need": "gpu"}, required_skills=["gpu"]
    )
    queue.register_worker("w1", skills=["cpu"])
    assert queue.lease(tenant_id="t1", worker_id="w1") is None
    queue.worker_heartbeat("w1", skills=["cpu", "gpu"])
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.id == j.id


def test_register_without_skills_keeps_existing(queue: JobQueue):
    queue.register_worker("w1", skills=["gpu"])
    again = queue.register_worker("w1")
    assert "gpu" in again["skills"]
    j = queue.enqueue(
        tenant_id="t1", payload={"need": "gpu"}, required_skills=["gpu"]
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.id == j.id


def test_schedule_copies_required_skills(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        name="gpu-tick",
        payload={"from": "sched"},
        interval_seconds=60,
        required_skills=["gpu"],
    )
    assert sched.required_skills == ("gpu",)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert fires
    job = fires[0].job
    assert job.required_skills == ("gpu",)
    queue.register_worker("cpu", skills=["cpu"])
    assert queue.lease(tenant_id="t1", worker_id="cpu") is None
    queue.register_worker("gpu", skills=["gpu"])
    leased = queue.lease(tenant_id="t1", worker_id="gpu")
    assert leased is not None and leased.id == job.id


def test_spawn_inherits_and_can_override_skills(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="t1",
        payload={"p": 1},
        required_skills=["gpu"],
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    # unskilled worker cannot take parent
    assert leased is None
    queue.register_worker("w", skills=["gpu"])
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="w",
        lease_token=leased.lease_token,
        children=[
            {"payload": {"c": 1}},
            {"payload": {"c": 2}, "required_skills": ["cpu"]},
        ],
        wait=True,
    )
    assert result.children[0].required_skills == ("gpu",)
    assert result.children[1].required_skills == ("cpu",)


def test_http_skills_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        job = client.enqueue(
            tenant_id="http-s",
            payload={"need": "gpu"},
            required_skills=["gpu"],
        )
        assert job.required_skills == ("gpu",)
        client.register_worker("cpu-w", skills=["cpu"])
        assert client.lease(worker_id="cpu-w", tenant_id="http-s") is None
        client.register_worker("gpu-w", skills=["gpu"])
        leased = client.lease(worker_id="gpu-w", tenant_id="http-s")
        assert leased is not None
        assert leased.id == job.id
        assert leased.payload == {"need": "gpu"}
        _ack(queue, leased, "gpu-w")
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_inprocess_worker_registers_skills(queue: JobQueue):
    from job_queue import Worker

    queue.enqueue(
        tenant_id="t1", payload={"need": "gpu"}, required_skills=["gpu"]
    )
    queue.enqueue(tenant_id="t1", payload={"plain": True})
    seen: list[str] = []

    w = Worker(queue, worker_id="gpu-w", tenant_id="t1", skills=["gpu"])

    @w.handler
    def _handle(job):
        seen.append(job.payload.get("need") or "plain")

    w.run(max_jobs=2, stop_when_idle=True)
    assert "gpu" in seen
    # unskilled leftover may still be pending if the worker only pulled skilled
    # jobs first; drain remaining with a general worker.
    leftover = queue.lease(tenant_id="t1", worker_id="any")
    if leftover is not None:
        _ack(queue, leftover, "any")
