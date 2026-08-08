"""Job required_skills vs worker capabilities on lease."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer
from uuid import uuid4

from job_queue import JobQueue
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def _enqueue_on_partitions(
    queue: JobQueue,
    *,
    tenant_id: str,
    partitions: set[int],
    required_skills: list[str] | None,
    count: int,
    payload_kind: str,
) -> list:
    """Enqueue until *count* jobs land on *partitions* (bounded retries)."""
    out = []
    for _ in range(count * 80):
        if len(out) >= count:
            break
        j = queue.enqueue(
            tenant_id=tenant_id,
            payload={"kind": payload_kind, "n": len(out)},
            required_skills=required_skills,
            job_id=uuid4(),
        )
        if j.partition_id in partitions:
            out.append(j)
        else:
            queue.cancel(j.id, tenant_id=tenant_id)
    assert len(out) == count, (
        f"only placed {len(out)}/{count} {payload_kind} jobs on {partitions}"
    )
    return out


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


def test_skilled_job_not_stuck_on_foreign_partition(queue: JobQueue):
    """GPU job on a CPU-owned shard must still be claimable by a GPU worker."""
    queue.require_worker_registration = True
    queue.set_partition_count(4)
    queue.register_worker("cpu-w", skills=["cpu"])
    queue.register_worker("gpu-w", skills=["gpu"])
    cpu_parts = set(queue.list_worker_partitions("cpu-w"))
    gpu_parts = set(queue.list_worker_partitions("gpu-w"))
    assert cpu_parts and gpu_parts and cpu_parts.isdisjoint(gpu_parts)

    foreign = _enqueue_on_partitions(
        queue,
        tenant_id="cross",
        partitions=cpu_parts,
        required_skills=["gpu"],
        count=1,
        payload_kind="gpu",
    )[0]
    assert foreign.partition_id in cpu_parts

    assert queue.lease(worker_id="cpu-w", tenant_id="cross") is None
    got = queue.lease(worker_id="gpu-w", tenant_id="cross")
    assert got is not None and got.id == foreign.id
    assert got.partition_id in cpu_parts
    _ack(queue, got, "gpu-w")

    # Fair path: another GPU job on the CPU shard.
    foreign2 = _enqueue_on_partitions(
        queue,
        tenant_id="cross-fair",
        partitions=cpu_parts,
        required_skills=["gpu"],
        count=1,
        payload_kind="gpu",
    )[0]
    fair = queue.lease(worker_id="gpu-w")
    assert fair is not None and fair.id == foreign2.id
    _ack(queue, fair, "gpu-w")


def test_unskilled_jobs_still_cannot_cross_partitions(queue: JobQueue):
    queue.require_worker_registration = True
    queue.set_partition_count(4)
    queue.register_worker("cpu-w", skills=["cpu"])
    queue.register_worker("gpu-w", skills=["gpu"])
    cpu_parts = set(queue.list_worker_partitions("cpu-w"))
    gpu_parts = set(queue.list_worker_partitions("gpu-w"))

    stuck = _enqueue_on_partitions(
        queue,
        tenant_id="pin",
        partitions=cpu_parts,
        required_skills=None,
        count=3,
        payload_kind="plain",
    )
    # GPU worker must not steal unskilled work from CPU shards.
    assert queue.lease(worker_id="gpu-w", tenant_id="pin") is None
    claimed = 0
    while True:
        j = queue.lease(worker_id="cpu-w", tenant_id="pin")
        if j is None:
            break
        assert j.partition_id in cpu_parts
        assert j.id in {x.id for x in stuck}
        _ack(queue, j, "cpu-w")
        claimed += 1
    assert claimed == 3
    assert gpu_parts  # still assigned; just unused here


def test_concurrent_gpu_workers_drain_jobs_on_cpu_shards(queue: JobQueue):
    """Under concurrent lease, skilled work on foreign shards must all finish."""
    queue.require_worker_registration = True
    queue.set_partition_count(8)
    queue.register_worker("cpu-a", skills=["cpu"])
    queue.register_worker("cpu-b", skills=["cpu"])
    queue.register_worker("gpu-a", skills=["gpu"])
    queue.register_worker("gpu-b", skills=["gpu"])
    cpu_parts = set(queue.list_worker_partitions("cpu-a")) | set(
        queue.list_worker_partitions("cpu-b")
    )
    assert cpu_parts

    n = 24
    placed = []
    for i in range(4):
        placed.extend(
            _enqueue_on_partitions(
                queue,
                tenant_id=f"load-{i}",
                partitions=cpu_parts,
                required_skills=["gpu"],
                count=n // 4,
                payload_kind="gpu",
            )
        )
    assert len(placed) == n
    ids = {j.id for j in placed}

    claimed: list[tuple[str, object]] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def run_worker(wid: str) -> None:
        try:
            idle = 0
            while idle < 8:
                job = queue.lease(worker_id=wid)
                if job is None:
                    idle += 1
                    continue
                idle = 0
                assert "gpu" in job.required_skills
                assert job.id in ids
                _ack(queue, job, wid)
                with lock:
                    claimed.append((wid, job.id))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=run_worker, args=(w,))
        for w in ("gpu-a", "gpu-b", "cpu-a", "cpu-b")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()
    assert not errors
    assert {jid for _, jid in claimed} == ids
    assert all(wid.startswith("gpu") for wid, _ in claimed)
    assert {wid for wid, _ in claimed} <= {"gpu-a", "gpu-b"}


def test_load_mixed_skills_and_tenants_no_starvation(queue: JobQueue):
    """CPU and GPU work both drain; neither kind is stolen by the other."""
    queue.require_worker_registration = True
    queue.set_partition_count(8)
    queue.register_worker("cpu-w", skills=["cpu"])
    queue.register_worker("gpu-w", skills=["gpu"])

    tenants = ["mix-a", "mix-b", "mix-c"]
    gpu_ids: set = set()
    cpu_ids: set = set()
    for tid in tenants:
        for i in range(8):
            g = queue.enqueue(
                tenant_id=tid,
                payload={"kind": "gpu", "i": i},
                required_skills=["gpu"],
            )
            c = queue.enqueue(
                tenant_id=tid,
                payload={"kind": "cpu", "i": i},
                required_skills=["cpu"],
            )
            gpu_ids.add(g.id)
            cpu_ids.add(c.id)

    seen_gpu: list[object] = []
    seen_cpu: list[object] = []
    errors: list[BaseException] = []

    def drain(wid: str, bucket: list, expected: set) -> None:
        try:
            idle = 0
            while idle < 10:
                job = queue.lease(worker_id=wid)
                if job is None:
                    idle += 1
                    continue
                idle = 0
                assert job.id in expected
                _ack(queue, job, wid)
                bucket.append(job.id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    tg = threading.Thread(target=drain, args=("gpu-w", seen_gpu, gpu_ids))
    tc = threading.Thread(target=drain, args=("cpu-w", seen_cpu, cpu_ids))
    tg.start()
    tc.start()
    tg.join(timeout=30)
    tc.join(timeout=30)
    assert not errors
    assert set(seen_gpu) == gpu_ids
    assert set(seen_cpu) == cpu_ids
