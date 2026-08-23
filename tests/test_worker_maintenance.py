"""Worker maintenance mode: stop new leases, finish held, resume intact."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, LeaseError, WorkerError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _hb


def test_drain_stops_new_leases_without_registration_required(queue: JobQueue):
    queue.register_worker("mw", skills=["gpu"])
    pending = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, required_skills=["gpu"]
    )
    held = queue.lease(tenant_id="t1", worker_id="mw")
    assert held is not None and held.id == pending.id

    extra = queue.enqueue(
        tenant_id="t1", payload={"n": 2}, required_skills=["gpu"]
    )
    out = queue.set_worker_maintenance("mw", draining=True)
    assert out["status"] == "draining"
    assert out["draining"] is True
    assert out["maintenance"] is True
    assert out["skills"] == ["gpu"]

    assert queue.lease(tenant_id="t1", worker_id="mw") is None
    assert queue.lease(worker_id="mw") is None
    still = queue.get(extra.id, tenant_id="t1")
    assert still is not None
    assert still.status in (JobStatus.PENDING, JobStatus.AWAITING_SKILLS)
    assert still.leased_by is None

    _hb(queue, held, "mw")
    done = _ack(queue, held, "mw", result={"ok": True})
    assert done.status == JobStatus.COMPLETED
    assert queue.get_worker("mw")["skills"] == ["gpu"]


def test_drain_rebalances_shards_and_peer_takes_new_work(queue: JobQueue):
    queue.require_worker_registration = True
    queue.set_partition_count(4)
    queue.register_worker("keep", skills=["cpu"])
    queue.register_worker("leave", skills=["cpu"])
    assert queue.list_worker_partitions("leave")
    queue.set_worker_maintenance("leave", draining=True)
    assert queue.list_worker_partitions("leave") == []
    assert set(queue.list_worker_partitions("keep")) == set(range(4))

    for i in range(8):
        queue.enqueue(tenant_id="pt", payload={"i": i})
    taken = 0
    for _ in range(16):
        assert queue.lease(worker_id="leave", tenant_id="pt") is None
        j = queue.lease(worker_id="keep", tenant_id="pt")
        if j is None:
            continue
        _ack(queue, j, "keep")
        taken += 1
    assert taken == 8


def test_heartbeat_and_reregister_keep_draining_and_skills(queue: JobQueue):
    first = queue.register_worker("stay", skills=["gpu", "cuda"])
    assert first["status"] == "active"
    queue.set_worker_maintenance("stay", draining=True)

    hb = queue.worker_heartbeat("stay", skills=["gpu", "cuda"])
    assert hb["status"] == "draining"
    assert hb["skills"] == ["cuda", "gpu"] or hb["skills"] == ["gpu", "cuda"]
    again = queue.register_worker("stay")
    assert again["status"] == "draining"
    assert "gpu" in again["skills"] and "cuda" in again["skills"]

    j = queue.enqueue(
        tenant_id="t1", payload={"need": "gpu"}, required_skills=["gpu"]
    )
    assert queue.lease(tenant_id="t1", worker_id="stay") is None
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None and got.status in (
        JobStatus.PENDING,
        JobStatus.AWAITING_SKILLS,
    )


def test_resume_leases_again_without_losing_skills(queue: JobQueue):
    queue.register_worker("back", skills=["tpu", "gpu"])
    queue.set_worker_maintenance("back", draining=True)
    j = queue.enqueue(
        tenant_id="t1", payload={"need": "tpu"}, required_skills=["tpu"]
    )
    assert queue.lease(tenant_id="t1", worker_id="back") is None

    out = queue.set_worker_maintenance("back", draining=False)
    assert out["status"] == "active"
    assert out["draining"] is False
    assert out["maintenance"] is False
    assert "tpu" in out["skills"] and "gpu" in out["skills"]

    leased = queue.lease(tenant_id="t1", worker_id="back")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "back")


def test_unknown_worker_drain_is_not_found(queue: JobQueue):
    with pytest.raises(WorkerError) as exc:
        queue.set_worker_maintenance("ghost", draining=True)
    assert exc.value.code == "worker_not_found"
    assert queue.get_worker("ghost") is None
    assert queue.list_draining_workers() == []


def test_stale_draining_worker_expires_then_revives_still_draining(queue: JobQueue):
    queue.register_worker("stale", skills=["cpu"], heartbeat_ttl_seconds=1)
    queue.set_worker_maintenance("stale", draining=True)
    time.sleep(1.2)
    n = queue.process_worker_timeouts(heartbeat_ttl_seconds=1)
    assert n == 1
    dead = queue.get_worker("stale")
    assert dead is not None
    assert dead["status"] == "dead"
    assert dead["maintenance"] is True
    assert "cpu" in dead["skills"]

    hb = queue.worker_heartbeat("stale", heartbeat_ttl_seconds=30)
    assert hb["status"] == "draining"
    assert hb["maintenance"] is True
    assert "cpu" in hb["skills"]
    assert queue.lease(tenant_id="t1", worker_id="stale") is None

    queue.set_worker_maintenance("stale", draining=False)
    j = queue.enqueue(tenant_id="t1", payload={"x": 1})
    leased = queue.lease(tenant_id="t1", worker_id="stale")
    assert leased is not None and leased.id == j.id
    _ack(queue, leased, "stale")


def test_list_workers_and_health_show_draining(queue: JobQueue):
    queue.register_worker("a", skills=["cpu"])
    queue.register_worker("b", skills=["gpu"])
    queue.set_worker_maintenance("b", draining=True)
    listed = queue.list_workers()
    by_id = {w["worker_id"]: w for w in listed}
    assert by_id["a"]["status"] == "active"
    assert by_id["b"]["status"] == "draining"
    assert queue.list_draining_workers() == ["b"]

    coord = Coordinator(queue, node_id="n1")
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        health = client.health()
        assert health["ok"] is True
        assert health["draining_workers"] == ["b"]
        workers = client.list_workers()
        assert "b" in workers["draining_workers"]
        got = client.get_worker("b")
        assert got["status"] == "draining"
        assert got["skills"] == ["gpu"]
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_http_drain_resume_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.register_worker("http-w", skills=["gpu", "cuda"])
        job = client.enqueue(
            tenant_id="http-m",
            payload={"need": "cuda"},
            required_skills=["gpu", "cuda"],
        )
        drained = client.drain_worker("http-w")
        assert drained["status"] == "draining"
        assert client.lease(worker_id="http-w", tenant_id="http-m") is None
        assert client.health()["draining_workers"] == ["http-w"]

        hb = client.worker_heartbeat("http-w", skills=["gpu", "cuda"])
        assert hb["status"] == "draining"
        assert "cuda" in hb["skills"] and "gpu" in hb["skills"]

        resumed = client.resume_worker("http-w")
        assert resumed["status"] == "active"
        assert "cuda" in resumed["skills"]
        leased = client.lease(worker_id="http-w", tenant_id="http-m")
        assert leased is not None and leased.id == job.id
        _ack(queue, leased, "http-w")
        assert client.health()["draining_workers"] == []
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_http_unknown_worker_raises(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        with pytest.raises(WorkerError) as exc:
            client.drain_worker("missing")
        assert exc.value.code == "worker_not_found"
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_inprocess_worker_idles_while_draining_then_resumes(queue: JobQueue):
    from job_queue import Worker

    queue.register_worker("loop-w", skills=["cpu"])
    seen: list[int] = []
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="loop-w",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=30,
        skills=["cpu"],
    )

    @worker.handler
    def _handle(job):
        seen.append(int(job.payload["n"]))

    queue.set_worker_maintenance("loop-w", draining=True)
    queue.enqueue(tenant_id="t1", payload={"n": 1})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    time.sleep(0.3)
    assert seen == []
    queue.set_worker_maintenance("loop-w", draining=False)
    deadline = time.monotonic() + 5
    while not seen and time.monotonic() < deadline:
        time.sleep(0.05)
    worker.stop()
    t.join(timeout=3)
    assert seen == [1]


def test_drain_release_held_returns_jobs_for_peer(queue: JobQueue):
    queue.register_worker("hold", skills=["cpu"])
    queue.register_worker("peer", skills=["cpu"])
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="hold")
    assert held is not None and held.id == job.id
    token = held.lease_token
    attempts = held.attempts

    out = queue.set_worker_maintenance("hold", draining=True, release_held=True)
    assert out["status"] == "draining"
    assert out["release_held"] is True
    assert out["released"] == 1
    assert str(job.id) in out["released_job_ids"]
    assert out["leased"] == 0
    assert out["skills"] == ["cpu"]

    got = queue.get(job.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.PENDING
    assert got.leased_by is None
    assert got.lease_token is None
    assert got.attempts == attempts

    with pytest.raises(LeaseError):
        queue.ack(
            held.id,
            tenant_id="t1",
            worker_id="hold",
            lease_token=token,  # type: ignore[arg-type]
            result={"stolen": True},
        )
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.PENDING

    peer = queue.lease(tenant_id="t1", worker_id="peer")
    assert peer is not None and peer.id == job.id
    assert peer.leased_by == "peer"
    done = _ack(queue, peer, "peer", result={"ok": True})
    assert done.status == JobStatus.COMPLETED
    with pytest.raises(LeaseError):
        queue.ack(
            held.id,
            tenant_id="t1",
            worker_id="hold",
            lease_token=token,  # type: ignore[arg-type]
            result={"late": True},
        )
    final = queue.get(job.id, tenant_id="t1")
    assert final is not None and final.status == JobStatus.COMPLETED
    assert final.result == {"ok": True}


def test_drain_without_release_keeps_held_lease(queue: JobQueue):
    queue.register_worker("keep", skills=["cpu"])
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="keep")
    assert held is not None
    out = queue.set_worker_maintenance("keep", draining=True, release_held=False)
    assert out["released"] == 0
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.LEASED
    assert still.leased_by == "keep"
    _ack(queue, held, "keep")


def test_release_held_after_drain_and_queue_default(queue: JobQueue):
    queue.register_worker("late", skills=["cpu"])
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="late")
    assert held is not None
    queue.set_worker_maintenance("late", draining=True, release_held=False)
    assert queue.get(job.id, tenant_id="t1").status == JobStatus.LEASED

    dropped = queue.release_worker_held_leases("late")
    assert dropped["released"] == 1
    assert queue.get(job.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.get_worker("late")["status"] == "draining"
    peer = queue.lease(tenant_id="t1", worker_id="peer")
    assert peer is not None and peer.id == job.id
    _ack(queue, peer, "peer")

    job2 = queue.enqueue(tenant_id="t1", payload={"n": 2})
    # Default off: omit release_held, job stays (but worker is draining so
    # it cannot lease job2). Register a second worker and use the setting.
    queue.register_worker("defw", skills=["cpu"])
    held2 = queue.lease(tenant_id="t1", worker_id="defw")
    assert held2 is not None and held2.id == job2.id
    assert queue.get_worker_drain_release_held() is False
    queue.set_worker_drain_release_held(True)
    assert queue.get_worker_drain_release_held() is True
    inherited = queue.set_worker_maintenance("defw", draining=True)
    assert inherited["release_held"] is True
    assert inherited["released"] == 1
    assert queue.get(job2.id, tenant_id="t1").status == JobStatus.PENDING
    queue.set_worker_drain_release_held(False)


def test_release_held_does_not_lose_raced_ack(queue: JobQueue):
    queue.register_worker("race", skills=["cpu"])
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="race")
    assert held is not None and held.lease_token is not None
    # Complete first, then release: already-acked job must stay completed.
    _ack(queue, held, "race", result={"first": True})
    out = queue.set_worker_maintenance("race", draining=True, release_held=True)
    assert out["released"] == 0
    done = queue.get(job.id, tenant_id="t1")
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.result == {"first": True}


def test_http_drain_release_held_roundtrip(queue: JobQueue):
    from job_queue import LeaseError

    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.register_worker("http-hold", skills=["gpu"])
        client.register_worker("http-peer", skills=["gpu"])
        job = client.enqueue(tenant_id="http-rel", payload={"n": 1})
        held = client.lease(worker_id="http-hold", tenant_id="http-rel")
        assert held is not None
        token = held.lease_token
        policy = client.get_worker_drain_policy()
        assert policy["release_held"] is False
        drained = client.drain_worker("http-hold", release_held=True)
        assert drained["status"] == "draining"
        assert drained["released"] == 1
        assert client.lease(worker_id="http-hold", tenant_id="http-rel") is None
        with pytest.raises(LeaseError):
            client.ack(
                held.id,
                tenant_id="http-rel",
                worker_id="http-hold",
                lease_token=token,  # type: ignore[arg-type]
                result={"nope": True},
            )
        peer = client.lease(worker_id="http-peer", tenant_id="http-rel")
        assert peer is not None and peer.id == job.id
        _ack(queue, peer, "http-peer")
        client.set_worker_drain_policy(release_held=True)
        assert client.get_worker_drain_policy()["release_held"] is True
        client.set_worker_drain_policy(release_held=False)
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_inprocess_worker_drops_held_on_release(queue: JobQueue):
    from job_queue import Worker

    started = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="drop-w",
        poll_interval_seconds=0.05,
        heartbeat_interval_seconds=0.1,
        visibility_timeout_seconds=30,
        skills=["cpu"],
    )
    queue.register_worker("drop-w", skills=["cpu"])

    @worker.handler
    def _handle(job, lease):
        started.set()
        for _ in range(80):
            lease.ensure_held()
            time.sleep(0.05)
        return {"should_not": True}

    queue.enqueue(tenant_id="t1", payload={"n": 1})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    out = queue.set_worker_maintenance(
        "drop-w", draining=True, release_held=True
    )
    assert out["released"] >= 1
    peer_job = None
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        peer_job = queue.lease(tenant_id="t1", worker_id="peer")
        if peer_job is not None:
            break
        time.sleep(0.05)
    assert peer_job is not None
    _ack(queue, peer_job, "peer", result={"peer": True})
    worker.stop()
    t.join(timeout=3)
    final = queue.get(peer_job.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.COMPLETED
    assert final.result == {"peer": True}


def test_release_held_refused_when_worker_is_active(queue: JobQueue):
    queue.register_worker("live", skills=["cpu"])
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="live")
    assert held is not None and held.lease_token is not None
    with pytest.raises(WorkerError) as exc:
        queue.release_worker_held_leases("live")
    assert exc.value.code == "worker_not_draining"
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None
    assert still.status == JobStatus.LEASED
    assert still.leased_by == "live"
    assert still.lease_token == held.lease_token
    # Same active worker must not be able to claim a second copy.
    assert queue.lease(tenant_id="t1", worker_id="live") is None
    _ack(queue, held, "live", result={"once": True})
    done = queue.get(job.id, tenant_id="t1")
    assert done is not None and done.status == JobStatus.COMPLETED
    assert done.result == {"once": True}


def test_active_worker_cannot_double_run_after_refused_release(queue: JobQueue):
    from job_queue import Worker

    started = threading.Event()
    runs: list[int] = []
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="dup-w",
        poll_interval_seconds=0.05,
        heartbeat_interval_seconds=0.1,
        visibility_timeout_seconds=30,
        skills=["cpu"],
    )
    queue.register_worker("dup-w", skills=["cpu"])

    @worker.handler
    def _handle(job, lease):
        runs.append(int(job.payload["n"]))
        started.set()
        time.sleep(0.4)
        lease.ensure_held()
        return {"run": len(runs)}

    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)
    with pytest.raises(WorkerError) as exc:
        queue.release_worker_held_leases("dup-w")
    assert exc.value.code == "worker_not_draining"
    held = queue.get(job.id, tenant_id="t1")
    assert held is not None and held.status == JobStatus.LEASED
    assert held.leased_by == "dup-w"
    assert queue.lease(tenant_id="t1", worker_id="dup-w") is None
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        got = queue.get(job.id, tenant_id="t1")
        if got is not None and got.status == JobStatus.COMPLETED:
            break
        time.sleep(0.05)
    worker.stop()
    t.join(timeout=3)
    final = queue.get(job.id, tenant_id="t1")
    assert final is not None
    assert final.status == JobStatus.COMPLETED
    assert runs == [1]
    assert final.result == {"run": 1}


def test_http_release_held_refused_on_active_worker(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.register_worker("http-live", skills=["cpu"])
        job = client.enqueue(tenant_id="http-live", payload={"n": 1})
        held = client.lease(worker_id="http-live", tenant_id="http-live")
        assert held is not None
        with pytest.raises(WorkerError) as exc:
            client.release_worker_held("http-live")
        assert exc.value.code == "worker_not_draining"
        still = queue.get(job.id, tenant_id="http-live")
        assert still is not None and still.status == JobStatus.LEASED
        assert still.leased_by == "http-live"
        client.drain_worker("http-live")
        dropped = client.release_worker_held("http-live")
        assert dropped["released"] == 1
        pending = queue.get(job.id, tenant_id="http-live")
        assert pending is not None and pending.status == JobStatus.PENDING
        assert client.lease(worker_id="http-live", tenant_id="http-live") is None
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_release_held_refused_after_resume(queue: JobQueue):
    queue.register_worker("back", skills=["cpu"])
    queue.set_worker_maintenance("back", draining=True)
    queue.set_worker_maintenance("back", draining=False)
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    held = queue.lease(tenant_id="t1", worker_id="back")
    assert held is not None
    with pytest.raises(WorkerError) as exc:
        queue.release_worker_held_leases("back")
    assert exc.value.code == "worker_not_draining"
    still = queue.get(job.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.LEASED
    _ack(queue, held, "back")


def test_drain_is_idempotent_and_resume_is_idempotent(queue: JobQueue):
    queue.register_worker("idemp", skills=["cpu"])
    a = queue.set_worker_maintenance("idemp", draining=True)
    b = queue.set_worker_maintenance("idemp", draining=True)
    assert a["status"] == b["status"] == "draining"
    c = queue.set_worker_maintenance("idemp", draining=False)
    d = queue.set_worker_maintenance("idemp", draining=False)
    assert c["status"] == d["status"] == "active"
    assert c["skills"] == ["cpu"]
