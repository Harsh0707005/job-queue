"""Tenant export / restore while other work keeps running."""

from __future__ import annotations

import json

import pytest

from job_queue import JobQueue, JobStatus, RestoreError
from tests.helpers_queue import _ack


def test_export_restore_pending_job_and_schedule(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="src-x",
        payload={"secret": "export-me", "n": 1},
        queue_name="email",
        labels=["wave:1"],
    )
    sched = queue.create_schedule(
        tenant_id="src-x",
        name="tick",
        payload={"from": "sched", "secret": "sched-secret"},
        interval_seconds=60,
    )
    snap = queue.export_tenant("src-x")
    assert snap.status == "ready"
    assert snap.counts.get("jobs", 0) >= 1
    assert snap.counts.get("schedules", 0) >= 1
    # Source still has its work.
    assert queue.get(src.id, tenant_id="src-x") is not None

    rst = queue.restore_tenant(
        "dst-x", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    assert queue.count(tenant_id="dst-x") >= 1
    leased = queue.lease(tenant_id="dst-x", worker_id="w", queue_name="email")
    assert leased is not None
    assert leased.id != src.id
    assert leased.payload == {"secret": "export-me", "n": 1}
    assert "wave:1" in leased.labels
    with queue._pool.connection() as conn:
        txt = conn.execute(
            "SELECT payload::text AS t FROM jobs WHERE tenant_id = %s",
            ("dst-x",),
        ).fetchone()["t"]
    assert "export-me" not in txt

    dst_scheds = queue.list_schedules(tenant_id="dst-x")
    assert len(dst_scheds) == 1
    assert dst_scheds[0].id != sched.id
    assert dst_scheds[0].payload["secret"] == "sched-secret"

    # Source still leases independently.
    src_leased = queue.lease(tenant_id="src-x", worker_id="w2", queue_name="email")
    assert src_leased is not None and src_leased.id == src.id
    _ack(queue, src_leased, "w2")
    _ack(queue, leased, "w")


def test_restore_rejects_non_empty_target(queue: JobQueue):
    queue.enqueue(tenant_id="s1", payload={"a": 1})
    queue.enqueue(tenant_id="t1", payload={"b": 1})
    snap = queue.export_tenant("s1")
    with pytest.raises(RestoreError, match="not empty"):
        queue.restore_tenant("t1", snapshot_id=snap.id)


def test_restore_resumes_after_crash_mid_batch(queue: JobQueue):
    for i in range(12):
        queue.enqueue(tenant_id="crash-src", payload={"i": i, "secret": f"s-{i}"})
    snap = queue.export_tenant("crash-src")
    first = queue.restore_tenant(
        "crash-dst", snapshot_id=snap.id, batch_size=3, run_until_complete=False
    )
    assert first.status == "restoring"
    assert queue.is_tenant_paused("crash-dst")
    with pytest.raises(ValueError, match="restoring"):
        queue.enqueue(tenant_id="crash-dst", payload={"no": 1})
    # Simulate coordinator death: just call advance again.
    done = None
    for _ in range(40):
        done = queue.advance_tenant_restore("crash-dst", batch_size=3)
        if done.status == "complete":
            break
    assert done is not None and done.status == "complete"
    assert queue.count(tenant_id="crash-dst") == 12
    seen = []
    while True:
        j = queue.lease(tenant_id="crash-dst", worker_id="w")
        if j is None:
            break
        seen.append(j.payload["i"])
        _ack(queue, j, "w")
    assert sorted(seen) == list(range(12))
    assert not queue.is_tenant_paused("crash-dst")


def test_restore_webhook_and_result_under_encryption(queue: JobQueue):
    from job_queue import WebhookEvent
    from tests.helpers_queue import _WebhookCapture

    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="wh-src",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook",
        )
        j = queue.enqueue(tenant_id="wh-src", payload={"token": "wh-restore-secret"})
        leased = queue.lease(tenant_id="wh-src", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"ok": True})
        snap = queue.export_tenant("wh-src")
        queue.restore_tenant(
            "wh-dst", snapshot_id=snap.id, batch_size=50, run_until_complete=True
        )
        dst_jobs = queue.list_jobs(tenant_id="wh-dst")
        assert dst_jobs
        completed = [x for x in dst_jobs if x.status == JobStatus.COMPLETED]
        assert completed
        assert completed[0].payload["token"] == "wh-restore-secret"
        assert completed[0].result == {"ok": True}
        deliveries = queue.list_webhook_deliveries(tenant_id="wh-dst")
        assert deliveries
        assert deliveries[0].payload["payload"]["token"] == "wh-restore-secret"
        with queue._pool.connection() as conn:
            blob = "\n".join(
                r["t"]
                for r in conn.execute(
                    """
                    SELECT payload::text AS t FROM jobs WHERE tenant_id = %s
                    UNION ALL
                    SELECT payload::text FROM webhook_deliveries WHERE tenant_id = %s
                    """,
                    ("wh-dst", "wh-dst"),
                ).fetchall()
            )
        assert "wh-restore-secret" not in blob
    finally:
        capture.stop()


def test_http_export_restore_roundtrip(queue: JobQueue):
    import threading
    from http.server import ThreadingHTTPServer

    from job_queue.coordinator import Coordinator, _make_handler
    from job_queue.remote import CoordinatorClient

    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.enqueue(tenant_id="http-src", payload={"k": "v"})
        exp = client.export_tenant("http-src")
        sid = exp["snapshot"]["id"]
        rst = client.restore_tenant(
            "http-dst", snapshot_id=sid, batch_size=50, run_until_complete=True
        )
        assert rst["restore"]["status"] == "complete"
        leased = client.lease(worker_id="w", tenant_id="http-dst")
        assert leased is not None
        assert leased.payload == {"k": "v"}
    finally:
        server.shutdown()
        t.join(timeout=5)
