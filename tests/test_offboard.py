"""Tenant offboard."""

from __future__ import annotations

import threading
import time
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
    _hb,
    _nack,
)

# ---------------------------------------------------------------------------
# Tenant offboard
# ---------------------------------------------------------------------------

def test_offboard_cancels_pending_deletes_schedules_webhooks(queue: JobQueue):
    from job_queue import TenantOffboardResult

    queue.enqueue(tenant_id="gone", payload={"n": 1})
    queue.enqueue(tenant_id="gone", payload={"n": 2})
    queue.create_schedule(
        tenant_id="gone",
        interval_seconds=60,
        payload={"tick": True},
        name="tick",
    )
    ep = queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
    )
    assert ep.tenant_id == "gone"

    # Other tenant must be untouched.
    queue.enqueue(tenant_id="keep", payload={"ok": True})
    queue.create_schedule(
        tenant_id="keep",
        interval_seconds=60,
        payload={"k": 1},
        name="keep-tick",
    )

    result = queue.offboard_tenant("gone")
    assert isinstance(result, TenantOffboardResult)
    assert result.tenant_id == "gone"
    assert result.pending_cancelled == 2
    assert result.schedules_deleted == 1
    assert result.webhooks_deleted == 1
    assert result.leased_remaining == 0
    assert result.webhooks_delivering_remaining == 0
    assert result.empty is True
    assert result.offboarding is False
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False

    # Keep tenant still works.
    assert queue.count(tenant_id="keep", status=JobStatus.PENDING) == 1
    assert len(queue.list_schedules(tenant_id="keep")) == 1
    job = queue.lease(tenant_id="keep", worker_id="w1")
    assert job is not None
    _ack(queue, job, "w1")

def test_offboard_allows_in_flight_ack_then_empties(queue: JobQueue):
    queue.enqueue(tenant_id="gone", payload={"run": 1})
    queue.enqueue(tenant_id="gone", payload={"wait": 1})  # pending, will cancel
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    assert leased.payload == {"run": 1}

    queue.create_schedule(
        tenant_id="gone", interval_seconds=30, payload={"s": 1}
    )
    queue.register_webhook(tenant_id="gone", url="http://example.test/h")

    result = queue.offboard_tenant("gone")
    assert result.pending_cancelled >= 1
    assert result.schedules_deleted == 1
    assert result.webhooks_deleted == 1
    assert result.leased_remaining == 1
    assert result.empty is False
    assert result.offboarding is True
    assert queue.is_tenant_offboarding("gone") is True
    assert queue.is_tenant_paused("gone") is True
    assert queue.tenant_is_empty("gone") is False

    # Enqueue / schedule / webhook / resume blocked while offboarding.
    with pytest.raises(ValueError, match="offboarding"):
        queue.enqueue(tenant_id="gone", payload={"nope": 1})
    with pytest.raises(ValueError, match="offboarding"):
        queue.create_schedule(
            tenant_id="gone", interval_seconds=10, payload={}
        )
    with pytest.raises(ValueError, match="offboarding"):
        queue.register_webhook(tenant_id="gone", url="http://example.test/x")
    with pytest.raises(ValueError, match="offboarding"):
        queue.resume_tenant("gone")

    # No new leases.
    assert queue.lease(tenant_id="gone", worker_id="w2") is None
    assert queue.lease(worker_id="fair") is None  # fair also skips

    # In-flight may heartbeat + ack.
    _hb(queue, leased, "w1", extend_by_seconds=30)
    done = _ack(queue, leased, "w1", result={"ok": True})
    assert done.status == JobStatus.COMPLETED

    # After last lease finishes, tenant is fully empty.
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False
    assert queue.get(leased.id, tenant_id="gone") is None

def test_offboard_in_flight_nack_then_empties(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="gone",
        payload={"x": 1},
        max_attempts=3,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None and leased.id == j.id

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1
    assert result.empty is False

    # Nack (even with requeue) must not leave residual work.
    nacked = _nack(queue, leased, "w1", error="boom", requeue=True)
    # Either requeued-then-purged or terminalized; tenant empty either way.
    assert queue.tenant_is_empty("gone") is True
    assert queue.get(nacked.id, tenant_id="gone") is None

def test_offboard_multiple_in_flight_empties_after_last(queue: JobQueue):
    queue.set_concurrency_limit("gone", 5)
    for i in range(3):
        queue.enqueue(tenant_id="gone", payload={"i": i})
    jobs = []
    for i in range(3):
        j = queue.lease(tenant_id="gone", worker_id=f"w{i}")
        assert j is not None
        jobs.append(j)

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 3
    assert result.empty is False

    _ack(queue, jobs[0], "w0")
    assert queue.tenant_is_empty("gone") is False
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 2

    _ack(queue, jobs[1], "w1")
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 1

    _ack(queue, jobs[2], "w2")
    assert queue.tenant_is_empty("gone") is True

def test_offboard_reaps_expired_abandoned_lease(queue: JobQueue):
    """Expired leases on offboarding tenants must be cancelled (not stuck)."""
    queue.enqueue(
        tenant_id="gone",
        payload={"x": 1},
        visibility_timeout_seconds=1,
        max_attempts=5,
    )
    leased = queue.lease(
        tenant_id="gone",
        worker_id="zombie",
        visibility_timeout_seconds=1,
    )
    assert leased is not None

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1

    # Wait for lease expiry; worker is gone and cannot ack.
    time.sleep(1.2)

    # Any lease attempt reaps expired offboarding leases.
    assert queue.lease(worker_id="other") is None
    assert queue.tenant_is_empty("gone") is True

def test_offboard_idempotent_when_already_empty(queue: JobQueue):
    result = queue.offboard_tenant("never-existed")
    assert result.empty is True
    assert result.pending_cancelled == 0
    assert result.leased_remaining == 0
    assert queue.tenant_is_empty("never-existed") is True

    # Second call still fine.
    result2 = queue.offboard_tenant("never-existed")
    assert result2.empty is True

def test_offboard_cancel_running_job_can_empty(queue: JobQueue):
    queue.enqueue(tenant_id="gone", payload={"x": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    token = leased.lease_token

    queue.offboard_tenant("gone")
    cancelled = queue.cancel(leased.id, tenant_id="gone")
    assert cancelled.status == JobStatus.CANCELLED
    with pytest.raises(LeaseError):
        queue.ack(
            leased.id,
            tenant_id="gone",
            worker_id="w1",
            lease_token=token,  # type: ignore[arg-type]
        )
    assert queue.tenant_is_empty("gone") is True

def test_offboard_keeps_claimed_webhook_for_finalize(queue: JobQueue):
    """Claimed delivering webhooks survive offboard so finalize can complete."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json
    import threading

    received: list[dict] = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(n)
            received.append(json.loads(body.decode("utf-8")))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            return

    sink = HTTPServer(("127.0.0.1", 0), H)
    port = sink.server_address[1]
    threading.Thread(target=sink.serve_forever, daemon=True).start()
    sink_url = f"http://127.0.0.1:{port}/hook"

    queue.register_webhook(
        tenant_id="gone",
        url=sink_url,
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1", result={"ok": True})

    pending = queue.list_webhook_deliveries(tenant_id="gone", status="pending")
    assert len(pending) == 1

    # Agent claims before offboard (or mid-flight).
    claimed, auto = queue.claim_webhook_deliveries(limit=10, claim_seconds=30.0)
    assert auto == []
    assert len(claimed) == 1
    item = claimed[0]
    assert item.delivery.status == "delivering"
    assert item.url == sink_url
    delivery_id = item.delivery.id

    # Also leave a non-claimed pending delivery that should be purged.
    # (Create via another completed job after claim is hard because pause —
    # instead ensure only the delivering one remains after offboard.)
    result = queue.offboard_tenant("gone")
    assert result.empty is False
    assert result.offboarding is True
    assert result.webhooks_delivering_remaining == 1
    # Endpoint kept while claim is live (not counted as deleted).
    assert result.webhooks_deleted == 0
    assert queue.tenant_is_empty("gone") is False

    # Delivery still exists and is still delivering.
    still = queue.get_webhook_delivery(delivery_id, tenant_id="gone")
    assert still is not None
    assert still.status == "delivering"
    # Endpoint still present (disabled) so FK holds.
    eps = queue.list_webhooks(tenant_id="gone")
    assert len(eps) == 1
    assert eps[0].enabled is False

    # Agent POSTs with in-memory claim (as real agents do).
    from job_queue.webhooks import post_webhook

    code, _ = post_webhook(
        item.url,
        item.delivery.payload,
        secret=item.secret,
        timeout_seconds=5.0,
        idempotency_key=str(delivery_id),
    )
    assert 200 <= code < 300

    fin = queue.finalize_webhook_delivery(delivery_id, status_code=code)
    assert fin is not None
    assert fin.success is True
    assert received  # sink got the POST

    # After last claimed webhook finalizes, tenant is fully empty.
    assert queue.tenant_is_empty("gone") is True
    assert queue.is_tenant_offboarding("gone") is False
    assert queue.get_webhook_delivery(delivery_id, tenant_id="gone") is None

def test_offboard_finalize_failed_webhook_does_not_requeue(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://127.0.0.1:1/nope",  # nothing listening
        events=["job.completed"],
        max_attempts=5,
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=30.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id

    result = queue.offboard_tenant("gone")
    assert result.webhooks_delivering_remaining == 1

    # Failed POST finalize during offboard → dead, not pending retry.
    fin = queue.finalize_webhook_delivery(
        did, status_code=500, error="boom"
    )
    assert fin is not None
    assert fin.success is False
    # Tenant emptied (dead delivery purged on finalize-offboard).
    assert queue.tenant_is_empty("gone") is True

def test_offboard_reaps_expired_claimed_webhook(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="gone", payload={"n": 1})
    leased = queue.lease(tenant_id="gone", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=1.0)
    assert len(claimed) == 1

    result = queue.offboard_tenant("gone")
    assert result.webhooks_delivering_remaining == 1

    time.sleep(1.2)
    # Claim poll reaps abandoned delivering claims for offboarding tenants.
    claimed2, auto = queue.claim_webhook_deliveries(limit=5)
    assert claimed2 == []
    assert queue.tenant_is_empty("gone") is True

def test_offboard_waits_for_both_lease_and_webhook(queue: JobQueue):
    queue.register_webhook(
        tenant_id="gone",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    # Job that will complete and enqueue webhook, then we claim webhook and
    # lease a second job so both in-flight types coexist at offboard time.
    j1 = queue.enqueue(tenant_id="gone", payload={"phase": 1}, priority=10)
    j2 = queue.enqueue(tenant_id="gone", payload={"phase": 2}, priority=1)
    l1 = queue.lease(tenant_id="gone", worker_id="w1")
    assert l1 is not None and l1.id == j1.id
    _ack(queue, l1, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=60.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id

    l2 = queue.lease(tenant_id="gone", worker_id="w2")
    assert l2 is not None and l2.id == j2.id

    result = queue.offboard_tenant("gone")
    assert result.leased_remaining == 1
    assert result.webhooks_delivering_remaining == 1
    assert result.empty is False

    # Finish webhook first — still not empty (lease remains).
    fin = queue.finalize_webhook_delivery(did, status_code=200)
    assert fin is not None and fin.success
    assert queue.tenant_is_empty("gone") is False
    assert queue.count(tenant_id="gone", status=JobStatus.LEASED) == 1

    # Finish lease last — now empty.
    _ack(queue, l2, "w2")
    assert queue.tenant_is_empty("gone") is True

