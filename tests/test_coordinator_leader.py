"""One coordinator leads control ticks; failover when it dies."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

from job_queue import JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _make_due


def test_acquire_is_exclusive(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    first = queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert first is not None
    assert first.node_id == "n1"
    assert first.epoch == 1
    assert (
        queue.acquire_or_renew_control_leader(node_id="n2", lease_token=b) is None
    )
    again = queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert again is not None
    assert again.epoch == 1
    got = queue.get_control_leader()
    assert got is not None
    assert got.node_id == "n1"
    assert queue.holds_control_leader(node_id="n1", lease_token=a)
    assert not queue.holds_control_leader(node_id="n2", lease_token=b)


def test_expired_lease_is_stolen_and_epoch_advances(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    first = queue.acquire_or_renew_control_leader(
        node_id="n1", lease_token=a, ttl_seconds=0.15
    )
    assert first is not None
    time.sleep(0.2)
    stolen = queue.acquire_or_renew_control_leader(
        node_id="n2", lease_token=b, ttl_seconds=2.0
    )
    assert stolen is not None
    assert stolen.node_id == "n2"
    assert stolen.epoch == 2
    assert not queue.holds_control_leader(node_id="n1", lease_token=a)
    assert (
        queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
        is None
    )


def test_release_lets_peer_take_over(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    assert queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert queue.release_control_leader(node_id="n1", lease_token=a) is True
    next_l = queue.acquire_or_renew_control_leader(node_id="n2", lease_token=b)
    assert next_l is not None
    assert next_l.node_id == "n2"
    assert next_l.epoch == 2


def test_bump_ticks_only_while_holding(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    bumped = queue.bump_control_tick(lease_token=a, kind="schedules", count=3)
    assert bumped is not None
    assert bumped.schedule_ticks == 3
    assert queue.bump_control_tick(lease_token=b, kind="schedules", count=9) is None
    got = queue.get_control_leader()
    assert got is not None
    assert got.schedule_ticks == 3


def test_two_control_loops_one_leader(queue: JobQueue):
    c1 = Coordinator(queue, node_id="loop-a")
    c2 = Coordinator(queue, node_id="loop-b")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    c2.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    try:
        deadline = time.time() + 3
        leaders: set[str] = set()
        while time.time() < deadline:
            leaders = {
                n
                for n, c in (("loop-a", c1), ("loop-b", c2))
                if c._control_loop is not None and c._control_loop.is_leader()
            }
            if len(leaders) == 1:
                break
            time.sleep(0.05)
        assert len(leaders) == 1
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id in ("loop-a", "loop-b")
    finally:
        c1.stop_control()
        c2.stop_control()


def test_failover_when_leader_stops(queue: JobQueue):
    c1 = Coordinator(queue, node_id="lead")
    c2 = Coordinator(queue, node_id="standby")
    c1.start_control(ttl_seconds=0.4, interval_seconds=0.08)
    c2.start_control(ttl_seconds=0.4, interval_seconds=0.08)
    try:
        deadline = time.time() + 3
        first = None
        while time.time() < deadline:
            if c1._control_loop is not None and c1._control_loop.is_leader():
                first, other = c1, c2
                break
            if c2._control_loop is not None and c2._control_loop.is_leader():
                first, other = c2, c1
                break
            time.sleep(0.05)
        else:
            raise AssertionError("neither coordinator became leader")
        first.stop_control()
        deadline = time.time() + 4
        while time.time() < deadline:
            if other._control_loop is not None and other._control_loop.is_leader():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("standby never took over")
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id == other.node_id
        assert lease.epoch >= 2
    finally:
        c1.stop_control()
        c2.stop_control()


def test_leader_fires_schedule_follower_does_not_double(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    c1 = Coordinator(queue, node_id="sched-a")
    c2 = Coordinator(queue, node_id="sched-b")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.08)
    c2.start_control(ttl_seconds=2.0, interval_seconds=0.08)
    try:
        deadline = time.time() + 4
        jobs = []
        while time.time() < deadline:
            jobs = queue.list_jobs(tenant_id="t1")
            if jobs:
                break
            time.sleep(0.05)
        assert len(jobs) == 1
        assert jobs[0].schedule_id == sched.id
        time.sleep(0.3)
        assert len(queue.list_jobs(tenant_id="t1")) == 1
    finally:
        c1.stop_control()
        c2.stop_control()


def _serve(coord: Coordinator) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_follower_http_control_returns_503(queue: JobQueue):
    c1 = Coordinator(queue, node_id="http-lead")
    c2 = Coordinator(queue, node_id="http-follow")
    c1.start_control(ttl_seconds=3.0, interval_seconds=0.1)
    c2.start_control(ttl_seconds=3.0, interval_seconds=0.1)
    s1, url1 = _serve(c1)
    s2, url2 = _serve(c2)
    try:
        deadline = time.time() + 5
        lead = follow = None
        while time.time() < deadline:
            if c1._control_loop is not None and c1._control_loop.is_leader():
                lead, follow, follow_url = c1, c2, url2
                break
            if c2._control_loop is not None and c2._control_loop.is_leader():
                lead, follow, follow_url = c2, c1, url1
                break
            time.sleep(0.05)
        else:
            raise AssertionError("leader not elected")
        status, body = follow.handle("POST", "/v1/schedules/process-due", {}, {})
        assert status == 503
        assert body.get("code") == "not_leader"
        status, body = follow.handle("POST", "/v1/alerts/evaluate", {}, {})
        assert status == 503
        status, body = follow.handle("POST", "/v1/admin/purge-expired-jobs", {}, {})
        assert status == 503
        # Worker APIs stay up on the follower.
        status, body = follow.handle(
            "POST",
            "/v1/jobs",
            {"tenant_id": "t1", "payload": {"ok": True}},
            {},
        )
        assert status in (200, 201)
        h = CoordinatorClient(follow_url).health()
        assert h.get("ok") is True
        assert h.get("leader") is False
        assert h.get("leader_node") == lead.node_id
        status, body = lead.handle("GET", "/v1/coordinator/leader", {}, {})
        assert status == 200
        assert body.get("leader") is True
        assert isinstance(body.get("lease"), dict)
        assert body["lease"]["node_id"] == lead.node_id
    finally:
        s1.shutdown()
        s2.shutdown()
        c1.stop_control()
        c2.stop_control()


def test_http_process_due_without_control_still_works(queue: JobQueue):
    coord = Coordinator(queue, node_id="solo")
    server, url = _serve(coord)
    try:
        client = CoordinatorClient(url)
        assert client.health()["ok"] is True
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={"n": 1},
        )
        n = client.process_due_schedules()
        assert n >= 1
    finally:
        server.shutdown()


def test_workers_keep_leasing_while_leader_ticks(queue: JobQueue):
    c1 = Coordinator(queue, node_id="w-lead")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    try:
        for i in range(4):
            queue.enqueue(tenant_id="t1", payload={"i": i})
        done = 0
        for _ in range(8):
            job = queue.lease(tenant_id="t1", worker_id="w")
            if job is None:
                break
            _ack(queue, job, "w")
            done += 1
        assert done == 4
        assert all(
            j.status == JobStatus.COMPLETED for j in queue.list_jobs(tenant_id="t1")
        )
    finally:
        c1.stop_control()
