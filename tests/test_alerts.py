"""Metric threshold alerts: rules, evaluation, history, webhooks."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, JobStatus, WebhookEvent
from job_queue.queue.alerts import ALERTS_OVERALL_TENANT
from tests.helpers_queue import _WebhookCapture, _ack, _nack


def test_dead_count_alert_fires_and_history(queue: JobQueue):
    rule = queue.create_alert_rule(
        name="dead-threshold",
        metric="dead_count",
        threshold=0,
        comparator="gt",
        scope="tenant",
        tenant_id="al1",
        cooldown_seconds=0,
        window_seconds=600,
    )
    assert rule.enabled
    # Drive one job to dead via nack.
    queue.enqueue(tenant_id="al1", payload={}, max_attempts=1)
    leased = queue.lease(tenant_id="al1", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="boom", requeue=True)
    assert queue.count(tenant_id="al1", status=JobStatus.DEAD) >= 1

    fired = queue.evaluate_alerts()
    assert any(e.rule_name == "dead-threshold" for e in fired)
    hist = queue.list_alert_events(tenant_id="al1")
    assert len(hist) >= 1
    assert hist[0].metric == "dead_count"
    assert hist[0].value >= 1


def test_webhook_fail_rate_alert_and_stream(queue: JobQueue):
    sink = _WebhookCapture(fail_times=0)
    try:
        queue.register_webhook(
            tenant_id=ALERTS_OVERALL_TENANT,
            url=sink.url,
            events=[WebhookEvent.ALERT_FIRED.value],
            secret="s" * 16,
        )
        queue.create_alert_rule(
            name="wh-fail-rate",
            metric="webhook_fail_rate",
            threshold=0.0,
            comparator="gt",
            scope="overall",
            cooldown_seconds=0,
            window_seconds=600,
        )
        # Produce webhook_fail metrics by registering a job webhook that fails.
        bad = _WebhookCapture(fail_times=100)
        try:
            queue.register_webhook(
                tenant_id="whf",
                url=bad.url,
                events=["job.completed"],
                secret="t" * 16,
                max_attempts=1,
            )
            queue.enqueue(tenant_id="whf", payload={})
            j = queue.lease(tenant_id="whf", worker_id="w")
            assert j is not None
            _ack(queue, j, "w")
            for _ in range(10):
                queue.process_webhook_deliveries(limit=20, http_timeout_seconds=2)
                time.sleep(0.05)
        finally:
            bad.stop()

        fired = queue.evaluate_alerts()
        assert any(e.rule_name == "wh-fail-rate" for e in fired)
        # Alert webhook should be pending/delivered for __overall__
        for _ in range(20):
            queue.process_webhook_deliveries(limit=20, http_timeout_seconds=2)
            if sink.received:
                break
            time.sleep(0.05)
        assert sink.received, "alert.fired webhook not delivered"
        body = sink.received[-1]["body"]
        assert body["event"] == "alert.fired"
        assert body["metric"] == "webhook_fail_rate"
    finally:
        sink.stop()


def test_dead_letter_lag_and_worker_leased(queue: JobQueue):
    queue.create_alert_rule(
        name="dl-lag",
        metric="dead_letter_lag_seconds",
        threshold=-1,
        comparator="gt",
        scope="tenant",
        tenant_id="lag1",
        cooldown_seconds=0,
    )
    # Create a dead job.
    queue.enqueue(tenant_id="lag1", payload={}, max_attempts=1)
    leased = queue.lease(tenant_id="lag1", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="x", requeue=True)
    fired = queue.evaluate_alerts()
    assert any(e.rule_name == "dl-lag" for e in fired)

    # Worker leased count
    queue.enqueue(tenant_id="wten", payload={})
    held = queue.lease(tenant_id="wten", worker_id="busy-w")
    assert held is not None
    queue.create_alert_rule(
        name="worker-busy",
        metric="worker_leased_count",
        threshold=0,
        comparator="gt",
        scope="worker",
        worker_id="busy-w",
        cooldown_seconds=0,
    )
    fired2 = queue.evaluate_alerts()
    assert any(e.rule_name == "worker-busy" and e.value >= 1 for e in fired2)


def test_cooldown_suppresses_repeat_fire(queue: JobQueue):
    queue.create_alert_rule(
        name="cool",
        metric="dead_count",
        threshold=-1,
        comparator="gt",
        scope="tenant",
        tenant_id="cool",
        cooldown_seconds=3600,
    )
    queue.enqueue(tenant_id="cool", payload={}, max_attempts=1)
    j = queue.lease(tenant_id="cool", worker_id="w")
    assert j is not None
    _nack(queue, j, "w", error="e", requeue=True)
    first = queue.evaluate_alerts()
    assert any(e.rule_name == "cool" for e in first)
    second = queue.evaluate_alerts()
    assert not any(e.rule_name == "cool" for e in second)


def test_alerts_http_api(postgres_url: str):
    import json
    import threading
    import urllib.request
    from http.server import ThreadingHTTPServer

    from job_queue.coordinator.app import Coordinator, _make_handler

    q = JobQueue.connect(postgres_url, min_size=1, max_size=4)
    try:
        with q._pool.connection() as conn:
            conn.execute("TRUNCATE jobs CASCADE")
            conn.execute("TRUNCATE alert_events CASCADE")
            conn.execute("TRUNCATE alert_rules CASCADE")
            conn.execute("TRUNCATE queue_metric_buckets")
            conn.commit()
        server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(Coordinator(q)))
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{port}"

        def post(path: str, body: dict) -> dict:
            req = urllib.request.Request(
                base + path,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                return json.loads(r.read().decode())

        def get(path: str) -> dict:
            with urllib.request.urlopen(base + path, timeout=5) as r:
                return json.loads(r.read().decode())

        created = post(
            "/v1/alerts/rules",
            {
                "name": "http-dead",
                "metric": "dead_count",
                "threshold": 0,
                "comparator": "gt",
                "scope": "tenant",
                "tenant_id": "http-al",
                "cooldown_seconds": 0,
            },
        )
        assert created["rule"]["name"] == "http-dead"
        rules = get("/v1/alerts/rules")
        assert rules["count"] >= 1
        q.enqueue(tenant_id="http-al", payload={}, max_attempts=1)
        j = q.lease(tenant_id="http-al", worker_id="w")
        assert j is not None
        _nack(q, j, "w", error="z", requeue=True)
        ev = post("/v1/alerts/evaluate", {})
        assert ev["count"] >= 1
        hist = get("/v1/alerts/events?tenant_id=http-al")
        assert hist["count"] >= 1
        server.shutdown()
    finally:
        q.close()
