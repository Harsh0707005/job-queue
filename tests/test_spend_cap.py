"""Per-tenant spend caps: tumbling window, no double-fire, no client tamper."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, SpendCapError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_spend_enqueue_cap_refuses_after_limit(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-e", window_seconds=3600, max_enqueues=3)
    for i in range(3):
        queue.enqueue(tenant_id="cap-e", payload={"i": i})
    with pytest.raises(SpendCapError, match="enqueue cap") as ei:
        queue.enqueue(tenant_id="cap-e", payload={"i": 3})
    assert ei.value.kind == "enqueue"
    assert ei.value.tenant_id == "cap-e"
    assert ei.value.retry_after_seconds is not None
    assert ei.value.retry_after_seconds > 0
    assert len(queue.list_jobs(tenant_id="cap-e")) == 3
    cap = queue.get_tenant_spend_cap("cap-e")
    assert cap.enqueues == 3
    assert cap.enqueue_remaining == 0
    assert cap.exhausted is True
    assert "enqueue" in cap.exhausted_kinds
    # Billing ledger matches the spend cursor (no extra charge on reject).
    assert queue.get_tenant_usage("cap-e").enqueues == 3


def test_spend_cancel_does_not_free_enqueue_quota(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-c", window_seconds=3600, max_enqueues=2)
    a = queue.enqueue(tenant_id="cap-c", payload={"i": 1})
    b = queue.enqueue(tenant_id="cap-c", payload={"i": 2})
    queue.cancel(a.id, tenant_id="cap-c")
    queue.cancel(b.id, tenant_id="cap-c")
    with pytest.raises(SpendCapError, match="enqueue cap"):
        queue.enqueue(tenant_id="cap-c", payload={"i": 3})
    assert queue.get_tenant_spend_cap("cap-c").enqueues == 2


def test_spend_payload_cannot_set_or_reset_counters(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-t", window_seconds=3600, max_enqueues=1)
    queue.enqueue(
        tenant_id="cap-t",
        payload={
            "spend_enqueues": 0,
            "max_enqueues": 999,
            "spend_window_start": None,
        },
    )
    with pytest.raises(SpendCapError):
        queue.enqueue(
            tenant_id="cap-t",
            payload={"spend_enqueues": -100, "enqueues": 0},
        )
    assert queue.get_tenant_spend_cap("cap-t").enqueues == 1


def test_spend_window_reset_allows_new_work(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-w", window_seconds=1, max_enqueues=1)
    queue.enqueue(tenant_id="cap-w", payload={"i": 1})
    with pytest.raises(SpendCapError):
        queue.enqueue(tenant_id="cap-w", payload={"i": 2})
    deadline = time.time() + 3.0
    while time.time() < deadline:
        rolled = queue.get_tenant_spend_cap("cap-w")
        if rolled.exhausted is False and rolled.enqueues == 0:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("spend window did not roll")
    again = queue.enqueue(tenant_id="cap-w", payload={"i": 3})
    assert again.payload["i"] == 3
    # Tumbling windows are epoch-aligned. A 1s window can roll between
    # enqueue and the following read, so accept 0 (already rolled) or 1.
    cap = queue.get_tenant_spend_cap("cap-w")
    assert cap.enqueues in (0, 1)
    assert len(queue.list_jobs(tenant_id="cap-w")) == 2


def test_spend_cap_isolated_across_tenants(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-iso-a", window_seconds=3600, max_enqueues=1)
    queue.enqueue(tenant_id="cap-iso-a", payload={"a": 1})
    # Uncapped tenant is unaffected.
    for i in range(4):
        queue.enqueue(tenant_id="cap-iso-b", payload={"b": i})
    with pytest.raises(SpendCapError):
        queue.enqueue(tenant_id="cap-iso-a", payload={"a": 2})
    assert len(queue.list_jobs(tenant_id="cap-iso-b")) == 4
    assert queue.get_tenant_spend_cap("cap-iso-b").max_enqueues is None


def test_spend_concurrent_enqueues_do_not_overshoot(queue: JobQueue):
    limit = 5
    queue.set_tenant_spend_cap("cap-cc", window_seconds=3600, max_enqueues=limit)
    ok: list = []
    err: list = []

    def go(i: int) -> None:
        try:
            ok.append(queue.enqueue(tenant_id="cap-cc", payload={"i": i}))
        except SpendCapError:
            err.append(i)

    threads = [threading.Thread(target=go, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive()
    assert len(ok) == limit
    assert len(err) == 20 - limit
    assert len(queue.list_jobs(tenant_id="cap-cc")) == limit
    assert queue.get_tenant_spend_cap("cap-cc").enqueues == limit
    assert queue.get_tenant_usage("cap-cc").enqueues == limit


def test_spend_dag_is_atomic_against_cap(queue: JobQueue):
    queue.set_tenant_spend_cap("cap-dag", window_seconds=3600, max_enqueues=2)
    queue.enqueue(tenant_id="cap-dag", payload={"keep": True})
    with pytest.raises(SpendCapError, match="enqueue cap"):
        queue.enqueue_dag(
            tenant_id="cap-dag",
            batch_key="too-big",
            jobs=[
                {"ref": "a", "payload": {"n": 1}},
                {"ref": "b", "payload": {"n": 2}, "depends_on": ["a"]},
            ],
        )
    jobs = queue.list_jobs(tenant_id="cap-dag")
    assert len(jobs) == 1
    assert queue.get_tenant_spend_cap("cap-dag").enqueues == 1
    assert queue.get_tenant_usage("cap-dag").enqueues == 1


def test_spend_runtime_cap_refuses_new_enqueue_but_drains_accepted(queue: JobQueue):
    """Runtime exhaustion must not park already-accepted jobs forever.

    Those jobs were already counted. Only *new* enqueues are refused until
    the window resets; pending work stays claimable.
    """
    queue.set_tenant_spend_cap(
        "cap-rt", window_seconds=3600, max_runtime_ms=1
    )
    first = queue.enqueue(tenant_id="cap-rt", payload={"n": 1})
    second = queue.enqueue(tenant_id="cap-rt", payload={"n": 2})
    third = queue.enqueue(tenant_id="cap-rt", payload={"n": 3})

    leased = queue.lease(tenant_id="cap-rt", worker_id="w")
    assert leased is not None and leased.id == first.id
    time.sleep(0.02)
    _ack(queue, leased, "w")

    cap = queue.get_tenant_spend_cap("cap-rt")
    assert cap.runtime_ms >= 1
    assert cap.exhausted is True
    assert "runtime" in cap.exhausted_kinds

    with pytest.raises(SpendCapError, match="runtime cap"):
        queue.enqueue(tenant_id="cap-rt", payload={"n": 4})
    assert queue.get(third.id, tenant_id="cap-rt") is not None
    assert len(queue.list_jobs(tenant_id="cap-rt")) == 3

    # Already-accepted leftover jobs must still lease (scoped and fair).
    mid = queue.lease(tenant_id="cap-rt", worker_id="w2")
    assert mid is not None and mid.id == second.id
    _ack(queue, mid, "w2")
    last = queue.lease(worker_id="w3")
    assert last is not None and last.id == third.id
    _ack(queue, last, "w3")
    assert queue.lease(tenant_id="cap-rt", worker_id="w4") is None

    for job_id in (first.id, second.id, third.id):
        got = queue.get(job_id, tenant_id="cap-rt")
        assert got is not None and got.status == JobStatus.COMPLETED


def test_spend_clear_and_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        set_cap = client.set_tenant_spend_cap(
            "http-cap", window_seconds=3600, max_enqueues=1
        )
        assert set_cap["max_enqueues"] == 1
        client.enqueue(tenant_id="http-cap", payload={"n": 1})
        with pytest.raises(SpendCapError) as ei:
            client.enqueue(tenant_id="http-cap", payload={"n": 2})
        assert ei.value.kind == "enqueue"
        got = client.get_tenant_spend_cap("http-cap")
        assert got["enqueues"] == 1
        assert got["exhausted"] is True
        cleared = client.set_tenant_spend_cap("http-cap", clear=True)
        assert cleared["max_enqueues"] is None
        client.enqueue(tenant_id="http-cap", payload={"n": 3})
        assert len(queue.list_jobs(tenant_id="http-cap")) == 2
    finally:
        server.shutdown()
