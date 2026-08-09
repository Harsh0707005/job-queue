"""Per-tenant billing usage: exact counts, isolation, crash rollback, load."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, TenantUsage
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _nack


def test_enqueue_lease_ack_usage(queue: JobQueue):
    j = queue.enqueue(tenant_id="bill-a", payload={"i": 1})
    usage = queue.get_tenant_usage("bill-a")
    assert usage.enqueues == 1
    assert usage.leases == 0
    leased = queue.lease(tenant_id="bill-a", worker_id="w1")
    assert leased is not None and leased.id == j.id
    usage = queue.get_tenant_usage("bill-a")
    assert usage.leases == 1
    _ack(queue, leased, "w1")
    usage = queue.get_tenant_usage("bill-a")
    assert usage.acks == 1
    assert usage.nacks == 0
    assert usage.runtime_ms >= 0
    assert usage.runtime_seconds == usage.runtime_ms / 1000.0


def test_nack_fail_and_dead_letter_usage(queue: JobQueue):
    queue.enqueue(
        tenant_id="bill-n",
        payload={"i": 1},
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="bill-n", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="boom", requeue=True)
    usage = queue.get_tenant_usage("bill-n")
    assert usage.nacks == 1
    assert usage.dead_letters == 1
    assert usage.fails == 0

    queue.enqueue(tenant_id="bill-f", payload={"i": 2}, max_attempts=3)
    leased = queue.lease(tenant_id="bill-f", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="nope", requeue=False)
    usage = queue.get_tenant_usage("bill-f")
    assert usage.nacks == 1
    assert usage.fails == 1
    assert usage.dead_letters == 0


def test_dedupe_hit_does_not_double_enqueue(queue: JobQueue):
    a = queue.enqueue(
        tenant_id="bill-d", payload={"i": 1}, dedupe_key="only-once"
    )
    b = queue.enqueue(
        tenant_id="bill-d", payload={"i": 2}, dedupe_key="only-once"
    )
    assert a.id == b.id
    assert queue.get_tenant_usage("bill-d").enqueues == 1


def test_tenants_do_not_share_usage(queue: JobQueue):
    queue.enqueue(tenant_id="iso-a", payload={})
    queue.enqueue(tenant_id="iso-b", payload={})
    queue.enqueue(tenant_id="iso-b", payload={})
    ja = queue.lease(tenant_id="iso-a", worker_id="w")
    jb = queue.lease(tenant_id="iso-b", worker_id="w")
    assert ja and jb
    _ack(queue, ja, "w")
    assert queue.get_tenant_usage("iso-a").enqueues == 1
    assert queue.get_tenant_usage("iso-a").acks == 1
    assert queue.get_tenant_usage("iso-b").enqueues == 2
    assert queue.get_tenant_usage("iso-b").acks == 0
    listed = {u.tenant_id: u for u in queue.list_tenant_usage()}
    assert listed["iso-a"].enqueues == 1
    assert listed["iso-b"].enqueues == 2


def test_cancel_and_release_usage(queue: JobQueue):
    j = queue.enqueue(tenant_id="bill-c", payload={})
    queue.cancel(j.id, tenant_id="bill-c")
    assert queue.get_tenant_usage("bill-c").cancels == 1

    j2 = queue.enqueue(tenant_id="bill-c", payload={})
    leased = queue.lease(tenant_id="bill-c", worker_id="w")
    assert leased is not None and leased.id == j2.id
    queue.release(
        leased.id,
        tenant_id="bill-c",
        worker_id="w",
        lease_token=leased.lease_token,
    )
    usage = queue.get_tenant_usage("bill-c")
    assert usage.releases == 1
    assert usage.enqueues == 2


def test_runtime_accumulates_on_ack(queue: JobQueue):
    queue.enqueue(tenant_id="bill-rt", payload={})
    leased = queue.lease(tenant_id="bill-rt", worker_id="w")
    assert leased is not None
    time.sleep(0.05)
    _ack(queue, leased, "w")
    usage = queue.get_tenant_usage("bill-rt")
    assert usage.runtime_ms >= 40


def test_usage_txn_rollback_leaves_no_stale(queue: JobQueue):
    from job_queue.queue._common import _utcnow

    now = _utcnow()
    try:
        with queue._pool.connection() as conn:
            with conn.transaction():
                queue._record_usage(
                    conn, tenant_id="crash-u", now=now, enqueues=9, acks=3
                )
                raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    usage = queue.get_tenant_usage("crash-u")
    assert usage.enqueues == 0
    assert usage.acks == 0

    # Real enqueue crash: job + usage share one txn.
    try:
        with queue._pool.connection() as conn:
            with conn.transaction():
                queue._record_usage(
                    conn, tenant_id="crash-u", now=now, enqueues=1
                )
                conn.execute("SELECT 1/0")
    except Exception:
        pass
    assert queue.get_tenant_usage("crash-u").enqueues == 0
    j = queue.enqueue(tenant_id="crash-u", payload={"ok": True})
    assert j.status == JobStatus.PENDING
    assert queue.get_tenant_usage("crash-u").enqueues == 1


def test_failed_ack_does_not_count(queue: JobQueue):
    from job_queue import LeaseError

    queue.enqueue(tenant_id="bill-bad", payload={})
    leased = queue.lease(tenant_id="bill-bad", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="bill-bad",
            worker_id="w",
            lease_token=leased.lease_token,
        )
    usage = queue.get_tenant_usage("bill-bad")
    assert usage.acks == 1
    assert usage.leases == 1


def test_concurrent_multi_tenant_usage_exact(queue: JobQueue):
    tenants = [f"load-{i}" for i in range(4)]
    per = 20
    errors: list[BaseException] = []
    lock = threading.Lock()
    ids: dict[str, list] = {t: [] for t in tenants}

    def enq(tid: str, n: int) -> None:
        try:
            j = queue.enqueue(
                tenant_id=tid, payload={"n": n}, job_id=uuid4()
            )
            with lock:
                ids[tid].append(j.id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=enq, args=(tid, n))
        for tid in tenants
        for n in range(per)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()
    assert not errors

    claimed: list = []

    def drain(wid: str) -> None:
        try:
            idle = 0
            while idle < 20:
                job = queue.lease(worker_id=wid)
                if job is None:
                    idle += 1
                    time.sleep(0.005)
                    continue
                idle = 0
                _ack(queue, job, wid)
                with lock:
                    claimed.append(job.id)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    workers = [
        threading.Thread(target=drain, args=(f"w-{i}",)) for i in range(6)
    ]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=45)
        assert not t.is_alive()
    assert not errors

    all_ids = {j for tid in tenants for j in ids[tid]}
    assert set(claimed) == all_ids
    for tid in tenants:
        usage = queue.get_tenant_usage(tid)
        assert usage.enqueues == per, (tid, usage)
        assert usage.acks == per
        assert usage.leases == per
        other = sum(
            queue.get_tenant_usage(o).enqueues for o in tenants if o != tid
        )
        assert other == per * (len(tenants) - 1)

    listed = queue.list_tenant_usage()
    by_tid = {u.tenant_id: u for u in listed}
    assert sum(by_tid[t].enqueues for t in tenants) == per * len(tenants)


def test_schedule_and_spawn_usage(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    queue.create_schedule(
        tenant_id="bill-s",
        name="tick",
        payload={"from": "sched"},
        interval_seconds=60,
        start_at=now - timedelta(seconds=1),
    )
    fires = queue.process_due_schedules(tenant_id="bill-s")
    assert fires
    usage = queue.get_tenant_usage("bill-s")
    assert usage.schedule_fires == 1
    assert usage.enqueues == 1

    parent = queue.enqueue(tenant_id="bill-sp", payload={"p": 1})
    leased = queue.lease(tenant_id="bill-sp", worker_id="w")
    assert leased is not None
    queue.spawn_children(
        parent.id,
        tenant_id="bill-sp",
        worker_id="w",
        lease_token=leased.lease_token,
        children=[{"payload": {"c": 1}}, {"payload": {"c": 2}}],
        wait=False,
    )
    usage = queue.get_tenant_usage("bill-sp")
    assert usage.enqueues == 3  # parent + 2 children
    assert usage.spawn_children == 2


def test_merge_adds_usage_without_double_count(queue: JobQueue):
    for _ in range(2):
        queue.enqueue(tenant_id="src-u", payload={})
    for _ in range(3):
        queue.enqueue(tenant_id="dst-u", payload={})
    assert queue.get_tenant_usage("src-u").enqueues == 2
    assert queue.get_tenant_usage("dst-u").enqueues == 3
    queue.merge_tenant("src-u", "dst-u")
    assert queue.get_tenant_usage("src-u").enqueues == 0
    assert queue.get_tenant_usage("dst-u").enqueues == 5


def test_http_usage_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.enqueue(tenant_id="http-u", payload={"x": 1})
        leased = client.lease(worker_id="w", tenant_id="http-u")
        assert leased is not None
        client.ack(
            leased.id,
            tenant_id="http-u",
            worker_id="w",
            lease_token=leased.lease_token,
        )
        usage = client.get_tenant_usage("http-u")
        assert usage["enqueues"] == 1
        assert usage["leases"] == 1
        assert usage["acks"] == 1
        buckets = client.list_tenant_usage_buckets("http-u")
        assert buckets["count"] >= 1
        all_u = client.list_tenant_usage()
        assert any(r["tenant_id"] == "http-u" for r in (all_u.get("usage") or []))
    finally:
        server.shutdown()


def test_usage_model_exported():
    assert TenantUsage(tenant_id="t", period_start=None, period_end=None).acks == 0
