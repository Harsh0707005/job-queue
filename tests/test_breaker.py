"""Tenant failure circuit breaker: trip, skip, reset, isolation."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _hb, _nack


def _fail_once(queue: JobQueue, tenant_id: str, *, n: int = 1) -> None:
    for i in range(n):
        queue.enqueue(
            tenant_id=tenant_id,
            payload={"i": i},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        job = queue.lease(tenant_id=tenant_id, worker_id=f"w-{i}")
        assert job is not None
        out = _nack(queue, job, f"w-{i}", error="boom", requeue=True)
        assert out.status == JobStatus.DEAD


def test_breaker_trips_and_skips_scoped_and_fair(queue: JobQueue):
    queue.set_tenant_breaker("bad", failure_threshold=2, window_seconds=60)
    queue.enqueue(tenant_id="good", payload={"ok": True})
    _fail_once(queue, "bad", n=2)

    br = queue.get_tenant_breaker("bad")
    assert br.open is True
    assert br.failures == 2
    assert br.opened_at is not None
    assert queue.is_tenant_breaker_open("bad") is True
    assert queue.is_tenant_breaker_open("good") is False

    queue.enqueue(tenant_id="bad", payload={"wait": True})
    assert queue.lease(tenant_id="bad", worker_id="w-scoped") is None

    job = queue.lease(worker_id="fair")
    assert job is not None
    assert job.tenant_id == "good"
    _ack(queue, job, "fair")
    assert queue.lease(worker_id="fair") is None
    assert queue.count(tenant_id="bad", status=JobStatus.PENDING) == 1

    stats = queue.get_tenant_stats("bad")
    assert stats.breaker_open is True
    assert stats.claimable == 0


def test_breaker_enqueue_still_works_while_open(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=1, window_seconds=60)
    _fail_once(queue, "t1", n=1)
    waiting = queue.enqueue(tenant_id="t1", payload={"later": True})
    assert waiting.status == JobStatus.PENDING
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    assert queue.get(waiting.id, tenant_id="t1").status == JobStatus.PENDING


def test_breaker_allows_in_flight_heartbeat_ack(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=3, window_seconds=60)
    live = queue.enqueue(tenant_id="t1", payload={"live": True}, max_attempts=3)
    leased = queue.lease(tenant_id="t1", worker_id="holder")
    assert leased is not None
    _fail_once(queue, "t1", n=3)
    assert queue.get_tenant_breaker("t1").open is True
    extended = _hb(queue, leased, "holder", extend_by_seconds=30)
    assert extended.lease_expires_at is not None
    done = _ack(queue, leased, "holder", result={"ok": True})
    assert done.status == JobStatus.COMPLETED
    assert queue.get_result(live.id, tenant_id="t1") == {"ok": True}


def test_breaker_reset_restores_leasing(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=1, window_seconds=60)
    _fail_once(queue, "t1", n=1)
    queue.enqueue(tenant_id="t1", payload={"a": 1})
    assert queue.lease(tenant_id="t1", worker_id="w") is None

    reset = queue.reset_tenant_breaker("t1")
    assert reset.open is False
    assert reset.failures == 0
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    assert job.payload == {"a": 1}
    _ack(queue, job, "w")


def test_breaker_cooldown_auto_closes(queue: JobQueue):
    queue.set_tenant_breaker(
        "t1",
        failure_threshold=1,
        window_seconds=60,
        cooldown_seconds=1,
    )
    _fail_once(queue, "t1", n=1)
    assert queue.get_tenant_breaker("t1").open is True
    queue.enqueue(tenant_id="t1", payload={"after": True})
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    time.sleep(1.15)
    br = queue.get_tenant_breaker("t1")
    assert br.open is False
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _ack(queue, job, "w")


def test_breaker_disabled_by_default(queue: JobQueue):
    for i in range(8):
        queue.enqueue(
            tenant_id="free",
            payload={"i": i},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        job = queue.lease(tenant_id="free", worker_id="w")
        assert job is not None
        _nack(queue, job, "w", error="x", requeue=True)
    waiting = queue.enqueue(tenant_id="free", payload={"ok": True})
    got = queue.lease(tenant_id="free", worker_id="w")
    assert got is not None
    assert got.id == waiting.id
    assert queue.get_tenant_breaker("free").open is False


def test_breaker_cancel_and_ttl_do_not_count(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=1, window_seconds=60)
    c = queue.enqueue(tenant_id="t1", payload={"c": 1})
    queue.cancel(c.id, tenant_id="t1")
    ttl = queue.enqueue(
        tenant_id="t1",
        payload={"need": "tpu"},
        required_skills=["tpu"],
        ttl_seconds=1,
        expiry_action="dead",
    )
    time.sleep(1.1)
    expired = queue.process_expired_jobs(tenant_id="t1")
    assert expired["dead"] == 1
    assert queue.get(ttl.id, tenant_id="t1").status == JobStatus.DEAD
    assert queue.get_tenant_breaker("t1").open is False
    assert queue.get_tenant_breaker("t1").failures == 0

    ok = queue.enqueue(tenant_id="t1", payload={"ok": True})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    assert leased.id == ok.id
    _ack(queue, leased, "w")


def test_breaker_window_rolls_while_closed(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=2, window_seconds=1)
    queue.enqueue(
        tenant_id="t1",
        payload={"i": 1},
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _nack(queue, job, "w", error="x", requeue=True)
    assert queue.get_tenant_breaker("t1").failures == 1
    assert queue.get_tenant_breaker("t1").open is False
    time.sleep(1.15)
    queue.enqueue(
        tenant_id="t1",
        payload={"i": 2},
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    job2 = queue.lease(tenant_id="t1", worker_id="w")
    assert job2 is not None
    _nack(queue, job2, "w", error="x", requeue=True)
    br = queue.get_tenant_breaker("t1")
    assert br.open is False
    assert br.failures == 1


def test_breaker_fair_lease_does_not_stall_fleet(queue: JobQueue):
    queue.set_tenant_breaker("noisy", failure_threshold=1, window_seconds=60)
    _fail_once(queue, "noisy", n=1)
    for i in range(40):
        queue.enqueue(tenant_id="noisy", payload={"backlog": i})
    for i in range(5):
        queue.enqueue(tenant_id="ok", payload={"i": i})

    order: list[str] = []
    for _ in range(8):
        job = queue.lease(worker_id="w")
        if job is None:
            break
        order.append(job.tenant_id)
        _ack(queue, job, "w")
    assert order == ["ok"] * 5
    assert queue.count(tenant_id="noisy", status=JobStatus.PENDING) == 40


def test_breaker_expired_lease_not_reclaimed(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=1, window_seconds=60)
    queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=5,
        visibility_timeout_seconds=1,
    )
    leased = queue.lease(
        tenant_id="t1",
        worker_id="holder",
        visibility_timeout_seconds=1,
    )
    assert leased is not None
    _fail_once(queue, "t1", n=1)
    time.sleep(1.2)
    assert queue.lease(tenant_id="t1", worker_id="other") is None
    assert queue.lease(worker_id="fair-other") is None
    got = queue.get(leased.id, tenant_id="t1")
    assert got is not None
    assert got.status == JobStatus.LEASED
    assert got.leased_by == "holder"

    queue.reset_tenant_breaker("t1")
    reclaimed = queue.lease(
        tenant_id="t1",
        worker_id="rescuer",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None
    assert reclaimed.id == leased.id
    _ack(queue, reclaimed, "rescuer")


def test_breaker_schedules_skip_while_open(queue: JobQueue):
    queue.set_tenant_breaker("paused-sched", failure_threshold=1, window_seconds=60)
    queue.create_schedule(
        tenant_id="paused-sched",
        interval_seconds=1,
        payload={"tick": True},
        name="t1",
    )
    _fail_once(queue, "paused-sched", n=1)
    for _ in range(4):
        assert queue.process_due_schedules(tenant_id="paused-sched") == []
        time.sleep(0.2)
    assert queue.count(tenant_id="paused-sched", status=JobStatus.PENDING) == 0

    queue.create_schedule(
        tenant_id="ok-sched",
        interval_seconds=60,
        payload={"ok": True},
        name="ok",
    )
    ok_fires = queue.process_due_schedules(tenant_id="ok-sched")
    assert len(ok_fires) == 1

    queue.reset_tenant_breaker("paused-sched")
    fires = queue.process_due_schedules(tenant_id="paused-sched")
    assert len(fires) == 1
    assert queue.process_due_schedules(tenant_id="paused-sched") == []


def test_breaker_payload_cannot_trip_or_reset(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=3, window_seconds=60)
    queue.enqueue(
        tenant_id="t1",
        payload={
            "breaker_failures": 99,
            "breaker_opened_at": "2000-01-01T00:00:00Z",
            "failure_threshold": 1,
        },
    )
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    _ack(queue, job, "w")
    br = queue.get_tenant_breaker("t1")
    assert br.open is False
    assert br.failures == 0


def test_breaker_clear_disables(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=1, window_seconds=60)
    queue.set_tenant_breaker("t1", clear=True)
    _fail_once(queue, "t1", n=3)
    assert queue.get_tenant_breaker("t1").open is False
    waiting = queue.enqueue(tenant_id="t1", payload={"ok": True})
    got = queue.lease(tenant_id="t1", worker_id="w")
    assert got is not None
    assert got.id == waiting.id


def test_breaker_config_validation(queue: JobQueue):
    with pytest.raises(ValueError, match="window_seconds"):
        queue.set_tenant_breaker("t1", failure_threshold=1)
    with pytest.raises(ValueError, match="failure_threshold"):
        queue.set_tenant_breaker("t1", failure_threshold=0, window_seconds=10)
    with pytest.raises(ValueError, match="cooldown"):
        queue.set_tenant_breaker(
            "t1", failure_threshold=1, window_seconds=10, cooldown_seconds=0
        )


def test_breaker_nack_counts_once_when_also_dead(queue: JobQueue):
    queue.set_tenant_breaker("t1", failure_threshold=2, window_seconds=60)
    _fail_once(queue, "t1", n=1)
    br = queue.get_tenant_breaker("t1")
    assert br.open is False
    assert br.failures == 1
    _fail_once(queue, "t1", n=1)
    assert queue.get_tenant_breaker("t1").open is True


def test_breaker_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        set_br = client.set_tenant_breaker(
            "http-br",
            failure_threshold=1,
            window_seconds=60,
            cooldown_seconds=30,
        )
        assert set_br["failure_threshold"] == 1
        assert set_br["open"] is False
        client.enqueue(
            tenant_id="http-br",
            payload={"n": 1},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        job = client.lease(tenant_id="http-br", worker_id="w")
        assert job is not None
        assert job.lease_token is not None
        client.nack(
            job.id,
            tenant_id="http-br",
            worker_id="w",
            lease_token=job.lease_token,
            error="boom",
            requeue=True,
        )
        got = client.get_tenant_breaker("http-br")
        assert got["open"] is True
        client.enqueue(tenant_id="http-br", payload={"wait": True})
        assert client.lease(tenant_id="http-br", worker_id="w2") is None
        reset = client.reset_tenant_breaker("http-br")
        assert reset["open"] is False
        leased = client.lease(tenant_id="http-br", worker_id="w2")
        assert leased is not None
        cleared = client.set_tenant_breaker("http-br", clear=True)
        assert cleared["failure_threshold"] is None
    finally:
        server.shutdown()
