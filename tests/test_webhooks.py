"""HTTP webhooks (durable outbox)."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _WebhookCapture,
    _ack,
    _nack,
)

# ---------------------------------------------------------------------------
# HTTP webhooks (durable outbox)
# ---------------------------------------------------------------------------

def test_webhook_on_complete_persisted_then_delivered(queue: JobQueue):
    from job_queue import WebhookEvent

    capture = _WebhookCapture()
    try:
        ep = queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="s3cret",
        )
        assert ep.enabled is True

        queue.enqueue(tenant_id="t1", payload={"hello": "world"})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1", result={"score": 1})

        # Durable outbox row exists before delivery.
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        assert pending[0].event == "job.completed"
        assert pending[0].payload["result"] == {"score": 1}
        assert pending[0].job_id == job.id

        results = queue.process_webhook_deliveries(limit=10)
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].delivery.status == "succeeded"

        assert len(capture.received) == 1
        assert capture.received[0]["body"]["event"] == "job.completed"
        assert capture.received[0]["signature"] is not None

        assert queue.list_webhook_deliveries(tenant_id="t1", status="pending") == []
        assert len(queue.list_webhook_deliveries(tenant_id="t1", status="succeeded")) == 1
    finally:
        capture.stop()

def test_webhook_retry_backoff_then_succeed(queue: JobQueue):
    capture = _WebhookCapture(fail_times=2)
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            max_attempts=5,
            backoff_base_seconds=1,
            backoff_max_seconds=5,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        # Attempt 1 fails → still pending with backoff.
        r1 = queue.process_webhook_deliveries()
        assert len(r1) == 1
        assert r1[0].success is False
        assert r1[0].delivery.status == "pending"
        assert r1[0].delivery.attempts == 1
        assert r1[0].delivery.next_attempt_at > r1[0].delivery.updated_at

        # Not due yet.
        assert queue.process_webhook_deliveries() == []

        time.sleep(1.15)
        r2 = queue.process_webhook_deliveries()
        assert len(r2) == 1
        assert r2[0].success is False
        assert r2[0].delivery.attempts == 2

        time.sleep(2.15)  # exponential: 2s after attempt 2
        r3 = queue.process_webhook_deliveries()
        assert len(r3) == 1
        assert r3[0].success is True
        assert r3[0].delivery.status == "succeeded"
        assert len(capture.received) == 3
    finally:
        capture.stop()

def test_webhook_dead_letters_after_max_attempts(queue: JobQueue):
    capture = _WebhookCapture(fail_times=100)
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            max_attempts=2,
            backoff_base_seconds=0,
            backoff_max_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        r1 = queue.process_webhook_deliveries()
        assert r1[0].delivery.status == "pending"
        r2 = queue.process_webhook_deliveries()
        assert r2[0].delivery.status == "dead"
        assert r2[0].delivery.attempts == 2
        assert queue.list_webhook_deliveries(tenant_id="t1", status="dead")
    finally:
        capture.stop()

def test_webhook_on_failed_and_dead_events(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.failed", "job.dead"],
            backoff_base_seconds=0,
        )
        # failed via requeue=False
        queue.enqueue(
            tenant_id="t1",
            payload={"f": 1},
            max_attempts=3,
            retry_backoff_base_seconds=0,
        )
        j1 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j1 is not None
        n1 = _nack(queue, j1, "w1", error="nope", requeue=False)
        assert n1.status == JobStatus.FAILED

        # dead via max attempts
        queue.enqueue(
            tenant_id="t1",
            payload={"d": 1},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        j2 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j2 is not None
        n2 = _nack(queue, j2, "w1", error="gone", requeue=True)
        assert n2.status == JobStatus.DEAD

        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        events = {d.event for d in pending}
        assert events == {"job.failed", "job.dead"}

        results = queue.process_webhook_deliveries(limit=10)
        assert len(results) == 2
        assert all(r.success for r in results)
        received_events = {r["body"]["event"] for r in capture.received}
        assert received_events == {"job.failed", "job.dead"}
    finally:
        capture.stop()

def test_webhook_only_matching_tenant_and_events(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="a",
            url=capture.url,
            events=["job.completed"],
        )
        queue.register_webhook(
            tenant_id="b",
            url=capture.url,
            events=["job.dead"],
        )
        queue.enqueue(tenant_id="a", payload={})
        ja = queue.lease(tenant_id="a", worker_id="w")
        assert ja is not None
        _ack(queue, ja, "w")

        # Only tenant a completion delivery.
        pending = queue.list_webhook_deliveries(tenant_id="a", status="pending")
        assert len(pending) == 1
        assert queue.list_webhook_deliveries(tenant_id="b", status="pending") == []

        queue.process_webhook_deliveries()
        assert len(capture.received) == 1
        assert capture.received[0]["body"]["tenant_id"] == "a"
    finally:
        capture.stop()

def test_webhook_dispatcher_run_once(queue: JobQueue):
    from job_queue import WebhookDispatcher

    capture = _WebhookCapture()
    try:
        queue.register_webhook(tenant_id="t1", url=capture.url, events=["job.completed"])
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        disp = WebhookDispatcher(queue, batch_limit=10)
        results = disp.run_once()
        assert len(results) == 1
        assert results[0].success
    finally:
        capture.stop()

def test_webhook_no_double_send_after_success_before_db_finalize(queue: JobQueue):
    """Simulate crash after HTTP 200 but before status=succeeded.

    Claim+POST leave last_status_code=2xx while status stays delivering.
    Next process must NOT POST again; it finalizes as succeeded.
    """
    from datetime import datetime, timedelta, timezone
    from job_queue.webhooks import post_webhook

    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={"x": 1})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        delivery = pending[0]

        # --- Simulate phase1 claim + successful POST + crash before succeeded ---
        now = datetime.now(timezone.utc)
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = 'delivering',
                    attempts = 1,
                    delivering_until = %s,
                    last_status_code = 200,
                    updated_at = %s
                WHERE id = %s
                """,
                (now + timedelta(seconds=60), now, delivery.id),
            )
            conn.commit()

        # Record that the real first POST already happened (simulate).
        code, _ = post_webhook(
            capture.url,
            delivery.payload,
            idempotency_key=str(delivery.id),
        )
        assert 200 <= code < 300
        assert len(capture.received) == 1

        # Still within claim lease: process must not re-select as pending.
        r_mid = queue.process_webhook_deliveries(claim_seconds=60)
        # May recover as succeeded if lease not expired... with 60s claim_seconds
        # and delivering_until in future, reclaim query needs delivering_until < now.
        # So nothing due.
        assert all(
            x.delivery.id != delivery.id or x.delivery.status == "succeeded"
            for x in r_mid
        )
        # Force stuck lease expiry (crash recovery window).
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET delivering_until = %s
                WHERE id = %s AND status = 'delivering'
                """,
                (datetime.now(timezone.utc) - timedelta(seconds=1), delivery.id),
            )
            conn.commit()

        before = len(capture.received)
        results = queue.process_webhook_deliveries(claim_seconds=30)
        # Recovered via last_status_code=200 → succeeded, no new POST.
        assert len(capture.received) == before
        got = queue.get_webhook_delivery(delivery.id, tenant_id="t1")
        assert got is not None
        assert got.status == "succeeded"
        assert any(r.success and r.delivery.id == delivery.id for r in results)
    finally:
        capture.stop()

def test_webhook_claim_before_post_not_pending_until_done(queue: JobQueue):
    """After claim, row is delivering so concurrent process won't double-claim."""
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        # Manually claim like phase1
        now = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        )
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        did = pending[0].id
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET status = 'delivering',
                    attempts = 1,
                    delivering_until = %s
                WHERE id = %s
                """,
                (now + __import__("datetime").timedelta(seconds=120), did),
            )
            conn.commit()

        # Concurrent process sees no pending due rows.
        assert queue.list_webhook_deliveries(tenant_id="t1", status="pending") == []
        assert queue.list_webhook_deliveries(tenant_id="t1", status="delivering")
        r = queue.process_webhook_deliveries()
        assert r == []  # still leased, not expired
        assert len(capture.received) == 0
    finally:
        capture.stop()

def test_paused_tenant_does_not_enqueue_webhooks(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed", "job.failed", "job.dead"],
            backoff_base_seconds=0,
        )
        queue.pause_tenant("t1")

        # Complete while paused → no delivery row.
        queue.enqueue(tenant_id="t1", payload={"a": 1})
        # Resume only to lease, then pause again before ack? In-flight can
        # complete while paused — still must not enqueue webhooks.
        queue.resume_tenant("t1")
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        queue.pause_tenant("t1")
        _ack(queue, job, "w1", result={"ok": True})

        assert queue.list_webhook_deliveries(tenant_id="t1") == []
        assert queue.process_webhook_deliveries() == []
        assert capture.received == []

        # Dead while paused also creates no delivery.
        queue.resume_tenant("t1")
        queue.enqueue(
            tenant_id="t1",
            payload={"d": 1},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        j2 = queue.lease(tenant_id="t1", worker_id="w1")
        assert j2 is not None
        queue.pause_tenant("t1")
        n2 = _nack(queue, j2, "w1", error="x", requeue=True)
        assert n2.status == JobStatus.DEAD
        assert queue.list_webhook_deliveries(tenant_id="t1") == []
    finally:
        capture.stop()

def test_paused_tenant_does_not_claim_pending_webhook_deliveries(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        # Create delivery while active.
        queue.enqueue(tenant_id="t1", payload={})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1

        queue.pause_tenant("t1")
        # Must not claim/send while paused.
        assert queue.process_webhook_deliveries() == []
        assert capture.received == []
        still = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(still) == 1

        # Other tenant still delivers.
        queue.register_webhook(
            tenant_id="t2",
            url=capture.url,
            events=["job.completed"],
            backoff_base_seconds=0,
        )
        queue.enqueue(tenant_id="t2", payload={})
        j2 = queue.lease(tenant_id="t2", worker_id="w2")
        assert j2 is not None
        _ack(queue, j2, "w2")
        r = queue.process_webhook_deliveries()
        assert len(r) == 1
        assert r[0].success
        assert capture.received[-1]["body"]["tenant_id"] == "t2"

        # Resume → pending t1 delivery goes out.
        queue.resume_tenant("t1")
        r2 = queue.process_webhook_deliveries()
        assert len(r2) == 1
        assert r2[0].success
        assert r2[0].delivery.tenant_id == "t1"
        assert any(x["body"]["tenant_id"] == "t1" for x in capture.received)
    finally:
        capture.stop()

