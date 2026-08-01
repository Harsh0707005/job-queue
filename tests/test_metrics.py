"""Operational metrics: counters, history, multi-format, noisy ranking."""

from __future__ import annotations

import json
import time
from datetime import timedelta

import pytest

from job_queue import JobQueue, JobStatus, LeaseError
from job_queue.queue.metrics import METRIC_ACK, METRIC_LEASE, OVERALL_TENANT
from tests.helpers_queue import _ack, _nack


def test_lease_ack_metrics_and_snapshot(queue: JobQueue):
    queue.enqueue(tenant_id="m1", payload={"i": 1})
    queue.enqueue(tenant_id="m1", payload={"i": 2})
    j1 = queue.lease(tenant_id="m1", worker_id="w1")
    j2 = queue.lease(tenant_id="m1", worker_id="w1")
    assert j1 is not None and j2 is not None
    _ack(queue, j1, "w1")
    _nack(queue, j2, "w1", error="boom", requeue=False)

    snap = queue.get_metrics_snapshot(tenant_id="m1", window_seconds=600)
    thr = snap["throughput"]
    assert thr[METRIC_LEASE]["count"] == 2
    assert thr[METRIC_ACK]["count"] == 1
    assert thr["nack"]["count"] == 1
    assert thr["failed"]["count"] == 1
    assert snap["gauges"]["completed"] >= 1
    assert snap["gauges"]["leased"] == 0

    overall = queue.get_metrics_snapshot(window_seconds=600)
    assert overall["throughput"][METRIC_LEASE]["count"] >= 2
    assert overall["tenants"]
    assert any(t["tenant_id"] == "m1" for t in overall["tenants"])


def test_wait_before_claim_recorded(queue: JobQueue):
    job = queue.enqueue(tenant_id="wait", payload={})
    # Make it "wait" slightly by delaying availability in the past... already available.
    leased = queue.lease(tenant_id="wait", worker_id="w")
    assert leased is not None and leased.id == job.id
    hist = queue.get_metrics_history(tenant_id="wait")
    wait_rows = [b for b in hist["buckets"] if b["metric"] == "wait_before_claim"]
    assert wait_rows
    assert sum(b["count"] for b in wait_rows) == 1
    assert wait_rows[0]["avg_ms"] is not None
    assert wait_rows[0]["avg_ms"] >= 0


def test_metrics_sample_and_history_snapshots(queue: JobQueue):
    queue.enqueue(tenant_id="s1", payload={})
    snaps = queue.sample_metrics()
    assert any(s["tenant_id"] == "s1" for s in snaps)
    assert any(s["tenant_id"] == OVERALL_TENANT for s in snaps)
    hist = queue.get_metrics_history()
    assert hist["snapshots"]
    assert any(s["tenant_id"] == OVERALL_TENANT for s in hist["snapshots"])


def test_webhook_and_schedule_metrics(queue: JobQueue):
    from tests.helpers_queue import _WebhookCapture

    sink = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="wh",
            url=sink.url,
            events=["job.completed"],
            secret="s" * 16,
        )
        queue.enqueue(tenant_id="wh", payload={})
        leased = queue.lease(tenant_id="wh", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"ok": True})
        # Process deliveries
        processed = []
        for _ in range(20):
            processed = queue.process_webhook_deliveries(
                limit=10, http_timeout_seconds=2
            )
            if processed:
                break
            time.sleep(0.05)
        assert len(processed) >= 1
        snap = queue.get_metrics_snapshot(tenant_id="wh", window_seconds=600)
        assert snap["throughput"]["webhook_success"]["count"] >= 1
    finally:
        sink.stop()

    # schedule fire
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    queue.create_schedule(
        tenant_id="sch",
        name="t",
        payload={"x": 1},
        interval_seconds=60,
        start_at=now - timedelta(seconds=1),
    )
    fires = queue.process_due_schedules(limit=10)
    assert len(fires) >= 1
    snap = queue.get_metrics_snapshot(tenant_id="sch", window_seconds=600)
    assert snap["throughput"]["schedule_fire"]["count"] >= 1


def test_export_json_csv_parquet(queue: JobQueue):
    queue.enqueue(tenant_id="exp", payload={})
    queue.lease(tenant_id="exp", worker_id="w")
    j = queue.export_metrics(kind="snapshot", format="json", tenant_id="exp")
    assert j["format"] == "json"
    assert "throughput" in j["data"]

    csv_ex = queue.export_metrics(kind="snapshot", format="csv", tenant_id="exp")
    assert csv_ex["format"] == "csv"
    assert b"metric" in csv_ex["body"] or b"gauge_" in csv_ex["body"]

    try:
        import pyarrow  # noqa: F401
    except ImportError:
        with pytest.raises(RuntimeError, match="pyarrow"):
            queue.export_metrics(kind="history", format="parquet", tenant_id="exp")
    else:
        pq = queue.export_metrics(kind="history", format="parquet", tenant_id="exp")
        assert pq["format"] == "parquet"
        assert len(pq["body"]) > 4
        # parquet magic
        assert pq["body"][:4] == b"PAR1" or b"PAR1" in pq["body"][-4:]


def test_metrics_http_coordinator(postgres_url: str):
    from job_queue.coordinator.app import Coordinator, _make_handler
    from http.server import ThreadingHTTPServer
    import threading
    import urllib.request

    q = JobQueue.connect(postgres_url, min_size=1, max_size=4)
    try:
        with q._pool.connection() as conn:
            conn.execute("TRUNCATE jobs CASCADE")
            conn.execute("TRUNCATE queue_metric_buckets")
            conn.execute("TRUNCATE queue_metric_snapshots")
            conn.commit()
        coord = Coordinator(q)
        handler = _make_handler(coord)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            q.enqueue(tenant_id="http", payload={})
            leased = q.lease(tenant_id="http", worker_id="w")
            assert leased is not None
            _ack(q, leased, "w")

            def get(path: str) -> tuple[int, bytes, str]:
                req = urllib.request.Request(f"http://127.0.0.1:{port}{path}")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return (
                        resp.status,
                        resp.read(),
                        resp.headers.get("Content-Type", ""),
                    )

            code, body, ctype = get("/v1/metrics/snapshot?tenant_id=http")
            assert code == 200
            assert "json" in ctype
            data = json.loads(body.decode())
            assert data["throughput"]["lease"]["count"] >= 1
            assert data["throughput"]["ack"]["count"] >= 1

            code, body, ctype = get("/v1/metrics/snapshot?format=csv")
            assert code == 200
            assert "csv" in ctype
            assert b"metric" in body or b"gauge_" in body

            code, body, _ = get("/v1/metrics/noisy?min_rate=0")
            assert code == 200
            noisy = json.loads(body.decode())
            assert "noisy_metrics" in noisy

            # sample
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/v1/metrics/sample",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                sample = json.loads(resp.read().decode())
            assert sample["count"] >= 1
        finally:
            server.shutdown()
    finally:
        q.close()


def test_max_runtime_dead_increments_dead_counter(queue: JobQueue):
    """System max-runtime dead-letter must count ``dead`` (not only worker nack)."""
    import time

    job = queue.enqueue(
        tenant_id="rt",
        payload={},
        visibility_timeout_seconds=30,
        max_runtime_seconds=1,
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="rt", worker_id="w1")
    assert leased is not None
    time.sleep(1.1)
    with pytest.raises(LeaseError, match="max runtime"):
        queue.heartbeat(
            leased.id,
            tenant_id="rt",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    got = queue.get(job.id, tenant_id="rt")
    assert got is not None and got.status == JobStatus.DEAD
    snap = queue.get_metrics_snapshot(tenant_id="rt", window_seconds=600)
    assert snap["throughput"]["dead"]["count"] >= 1


def test_lease_expiry_exhausted_increments_dead_counter(queue: JobQueue):
    """Expired leases past max_attempts must count ``dead`` on reclaim path."""
    import time

    job = queue.enqueue(
        tenant_id="exp",
        payload={},
        visibility_timeout_seconds=1,
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(
        tenant_id="exp", worker_id="w1", visibility_timeout_seconds=1
    )
    assert leased is not None and leased.id == job.id
    time.sleep(1.2)
    # Next lease attempt dead-letters the exhausted expired lease.
    claimed = queue.lease(tenant_id="exp", worker_id="w2", visibility_timeout_seconds=30)
    assert claimed is None  # nothing left claimable
    got = queue.get(job.id, tenant_id="exp")
    assert got is not None and got.status == JobStatus.DEAD
    snap = queue.get_metrics_snapshot(tenant_id="exp", window_seconds=600)
    assert snap["throughput"]["dead"]["count"] >= 1


def test_remote_client_metrics(postgres_url: str):
    from job_queue.coordinator.app import Coordinator, _make_handler
    from job_queue.remote import CoordinatorClient
    from http.server import ThreadingHTTPServer
    import threading

    q = JobQueue.connect(postgres_url, min_size=1, max_size=4)
    try:
        with q._pool.connection() as conn:
            conn.execute("TRUNCATE jobs CASCADE")
            conn.execute("TRUNCATE queue_metric_buckets")
            conn.commit()
        coord = Coordinator(q)
        server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        try:
            q.enqueue(tenant_id="rc", payload={})
            j = q.lease(tenant_id="rc", worker_id="w")
            assert j is not None
            _ack(q, j, "w")
            snap = client.metrics_snapshot(tenant_id="rc")
            assert isinstance(snap, dict)
            assert snap["throughput"]["ack"]["count"] >= 1
            ctype, raw = client.metrics_snapshot(format="csv")  # type: ignore[misc]
            assert "csv" in ctype
            assert raw
            client.metrics_sample()
            hist = client.metrics_history(tenant_id="rc")
            assert isinstance(hist, dict)
            assert "buckets" in hist
        finally:
            server.shutdown()
    finally:
        q.close()
