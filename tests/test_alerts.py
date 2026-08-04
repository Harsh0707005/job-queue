"""Metric threshold alerts: rules, evaluation, history, webhooks."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from job_queue import JobQueue, JobStatus, WebhookEvent
from job_queue.queue._common import _utcnow
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
    assert hist[0].tenant_id == "al1"


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


def test_concurrent_evaluate_single_fire_under_cooldown(queue: JobQueue):
    """Multiple concurrent evaluators must not double-fire within cooldown."""
    queue.create_alert_rule(
        name="race-cool",
        metric="dead_count",
        threshold=-1,
        comparator="gt",
        scope="tenant",
        tenant_id="race",
        cooldown_seconds=3600,
    )
    queue.enqueue(tenant_id="race", payload={}, max_attempts=1)
    j = queue.lease(tenant_id="race", worker_id="w")
    assert j is not None
    _nack(queue, j, "w", error="e", requeue=True)

    # Shared timestamp so same-now claim path is exercised.
    now = _utcnow()

    def _eval() -> int:
        return sum(
            1 for e in queue.evaluate_alerts(now=now) if e.rule_name == "race-cool"
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_eval) for _ in range(8)]
        counts = [f.result() for f in as_completed(futs)]
    assert sum(counts) == 1, f"expected single fire, got per-worker {counts}"
    hist = queue.list_alert_events(tenant_id="race", limit=50)
    assert len([e for e in hist if e.rule_name == "race-cool"]) == 1


def test_list_alert_events_requires_tenant_and_isolates(queue: JobQueue):
    """History is tenant-scoped; missing tenant_id is rejected."""
    with pytest.raises(ValueError, match="tenant_id is required"):
        queue.list_alert_events(tenant_id="")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="tenant_id is required"):
        queue.list_alert_events(tenant_id=None)  # type: ignore[arg-type]

    for tid, name in (("iso-a", "iso-a-dead"), ("iso-b", "iso-b-dead")):
        queue.create_alert_rule(
            name=name,
            metric="dead_count",
            threshold=-1,
            comparator="gt",
            scope="tenant",
            tenant_id=tid,
            cooldown_seconds=0,
        )
        queue.enqueue(tenant_id=tid, payload={}, max_attempts=1)
        j = queue.lease(tenant_id=tid, worker_id="w")
        assert j is not None
        _nack(queue, j, "w", error="e", requeue=True)

    queue.create_alert_rule(
        name="iso-overall",
        metric="dead_count",
        threshold=-1,
        comparator="gt",
        scope="overall",
        cooldown_seconds=0,
    )
    queue.evaluate_alerts()

    hist_a = queue.list_alert_events(tenant_id="iso-a")
    hist_b = queue.list_alert_events(tenant_id="iso-b")
    hist_all = queue.list_alert_events(tenant_id=ALERTS_OVERALL_TENANT)
    assert all(e.tenant_id == "iso-a" for e in hist_a)
    assert all(e.tenant_id == "iso-b" for e in hist_b)
    assert any(e.rule_name == "iso-a-dead" for e in hist_a)
    assert any(e.rule_name == "iso-b-dead" for e in hist_b)
    assert not any(e.rule_name == "iso-b-dead" for e in hist_a)
    assert not any(e.rule_name == "iso-a-dead" for e in hist_b)
    assert any(e.rule_name == "iso-overall" for e in hist_all)
    assert all(e.tenant_id == ALERTS_OVERALL_TENANT for e in hist_all)


def test_mute_suppresses_fire_and_unmute_restores(queue: JobQueue):
    rule = queue.create_alert_rule(
        name="mute-me",
        metric="dead_count",
        threshold=-1,
        comparator="gt",
        scope="tenant",
        tenant_id="mute-t",
        cooldown_seconds=0,
    )
    queue.enqueue(tenant_id="mute-t", payload={}, max_attempts=1)
    j = queue.lease(tenant_id="mute-t", worker_id="w")
    assert j is not None
    _nack(queue, j, "w", error="e", requeue=True)

    muted = queue.mute_alert_rule(rule.id, duration_seconds=3600)
    assert muted.muted_until is not None
    fired_while_muted = queue.evaluate_alerts()
    assert not any(e.rule_name == "mute-me" for e in fired_while_muted)

    queue.unmute_alert_rule(rule.id)
    fired_after = queue.evaluate_alerts()
    assert any(e.rule_name == "mute-me" for e in fired_after)


def test_resolve_alert_event_in_history(queue: JobQueue):
    queue.create_alert_rule(
        name="resolve-me",
        metric="dead_count",
        threshold=-1,
        comparator="gt",
        scope="tenant",
        tenant_id="res-t",
        cooldown_seconds=0,
    )
    queue.enqueue(tenant_id="res-t", payload={}, max_attempts=1)
    j = queue.lease(tenant_id="res-t", worker_id="w")
    assert j is not None
    _nack(queue, j, "w", error="e", requeue=True)
    fired = queue.evaluate_alerts()
    ev = next(e for e in fired if e.rule_name == "resolve-me")
    assert ev.status == "open"
    assert ev.resolved_at is None

    resolved = queue.resolve_alert_event(
        ev.id, tenant_id="res-t", resolved_by="ops", note="cleared dead letters"
    )
    assert resolved.status == "resolved"
    assert resolved.resolved_at is not None
    assert resolved.resolved_by == "ops"
    assert resolved.resolution_note == "cleared dead letters"

    open_hist = queue.list_alert_events(tenant_id="res-t", status="open")
    closed = queue.list_alert_events(tenant_id="res-t", status="resolved")
    assert not any(e.id == ev.id for e in open_hist)
    assert any(e.id == ev.id for e in closed)

    # Cross-tenant resolve rejected.
    with pytest.raises(ValueError, match="not found"):
        queue.resolve_alert_event(ev.id, tenant_id="other-tenant")


def test_auto_pause_tenant_on_alert_fire(queue: JobQueue):
    with pytest.raises(ValueError, match="auto_pause_tenant"):
        queue.create_alert_rule(
            name="bad-auto",
            metric="dead_count",
            threshold=0,
            comparator="gt",
            scope="overall",
            auto_pause_tenant=True,
        )

    queue.create_alert_rule(
        name="pause-overflow",
        metric="dead_count",
        threshold=0,
        comparator="gt",
        scope="tenant",
        tenant_id="pause-t",
        cooldown_seconds=0,
        auto_pause_tenant=True,
    )
    queue.enqueue(tenant_id="pause-t", payload={}, max_attempts=1)
    j = queue.lease(tenant_id="pause-t", worker_id="w")
    assert j is not None
    _nack(queue, j, "w", error="e", requeue=True)
    assert not queue.is_tenant_paused("pause-t")

    # Extra pending work that must not be leased after auto-pause.
    queue.enqueue(tenant_id="pause-t", payload={"keep": True})
    fired = queue.evaluate_alerts()
    assert any(e.rule_name == "pause-overflow" for e in fired)
    assert queue.is_tenant_paused("pause-t")
    assert any(
        e.detail.get("auto_paused_tenant") == "pause-t"
        for e in fired
        if e.rule_name == "pause-overflow"
    )
    # Lease should refuse while paused.
    assert queue.lease(tenant_id="pause-t", worker_id="w2") is None

    queue.resume_tenant("pause-t")
    assert not queue.is_tenant_paused("pause-t")


def test_auto_resolve_after_healthy_streak_with_hysteresis(queue: JobQueue):
    """Open events close after N spaced healthy samples past recovery line."""
    from datetime import timedelta

    from job_queue.queue.alerts import AUTO_RESOLVE_BY

    # Fire when pending > 5; recover only when pending <= 2 (hysteresis).
    rule = queue.create_alert_rule(
        name="pending-hysteresis",
        metric="pending_count",
        threshold=5,
        comparator="gt",
        scope="tenant",
        tenant_id="ar-hyst",
        cooldown_seconds=0,
        recovery_threshold=2,
        recovery_evaluations=2,
        healthy_interval_seconds=5,
    )
    assert rule.recovery_threshold == 2.0
    assert rule.recovery_evaluations == 2
    assert rule.healthy_interval_seconds == 5

    t0 = _utcnow()
    # Seed 8 pending jobs → breach.
    for _ in range(8):
        queue.enqueue(tenant_id="ar-hyst", payload={})
    result = queue.evaluate_alerts(now=t0)
    assert any(e.rule_name == "pending-hysteresis" for e in result.fired)
    open1 = queue.list_alert_events(tenant_id="ar-hyst", status="open")
    assert len(open1) >= 1
    open_ids = {e.id for e in open1}

    # Drain to 4 pending: still above recovery (2) but not above fire (5).
    # Gray zone — must not auto-resolve or count as healthy.
    for _ in range(4):
        j = queue.lease(tenant_id="ar-hyst", worker_id="w")
        assert j is not None
        _ack(queue, j, "w")
    mid = queue.evaluate_alerts(now=t0 + timedelta(seconds=1))
    assert not any(e.rule_name == "pending-hysteresis" for e in mid.resolved)
    open_mid = queue.list_alert_events(tenant_id="ar-hyst", status="open")
    assert open_ids.issubset({e.id for e in open_mid})

    # Drain to 0 pending: fully healthy.
    while True:
        j = queue.lease(tenant_id="ar-hyst", worker_id="w")
        if j is None:
            break
        _ack(queue, j, "w")

    # First healthy evaluate: streak=1, not yet resolved.
    first_h = queue.evaluate_alerts(now=t0 + timedelta(seconds=2))
    assert not any(e.id in open_ids for e in first_h.resolved)
    still_open = queue.list_alert_events(tenant_id="ar-hyst", status="open")
    assert open_ids.issubset({e.id for e in still_open})

    # Same-window re-eval (before healthy_interval elapses): no second tick.
    same_window = queue.evaluate_alerts(now=t0 + timedelta(seconds=3))
    assert not any(e.id in open_ids for e in same_window.resolved)
    rule_mid = queue.get_alert_rule(rule.id)
    assert rule_mid is not None
    assert rule_mid.healthy_streak == 1

    # Second spaced healthy evaluate (after interval): auto-resolve.
    second_h = queue.evaluate_alerts(now=t0 + timedelta(seconds=2 + 5))
    assert any(e.id in open_ids for e in second_h.resolved)
    closed = queue.list_alert_events(tenant_id="ar-hyst", status="resolved")
    closed_ids = {e.id for e in closed}
    assert open_ids.issubset(closed_ids)
    for e in closed:
        if e.id in open_ids:
            assert e.resolved_by == AUTO_RESOLVE_BY
            assert e.status == "resolved"
            assert "auto-resolved" in (e.resolution_note or "")

    # recovery_threshold validation
    with pytest.raises(ValueError, match="recovery_threshold"):
        queue.create_alert_rule(
            name="bad-rec",
            metric="pending_count",
            threshold=5,
            comparator="gt",
            scope="tenant",
            tenant_id="ar-bad",
            recovery_threshold=10,  # above fire line — invalid
        )


def test_concurrent_healthy_ticks_do_not_instant_resolve(queue: JobQueue):
    """Parallel evaluates at the same timestamp only count one healthy tick."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import timedelta

    queue.create_alert_rule(
        name="race-recover",
        metric="pending_count",
        threshold=0,
        comparator="gt",
        scope="tenant",
        tenant_id="ar-race",
        cooldown_seconds=0,
        recovery_threshold=0,
        recovery_evaluations=2,
        healthy_interval_seconds=5,
    )
    # Breach then drain.
    queue.enqueue(tenant_id="ar-race", payload={})
    t0 = _utcnow()
    fired = queue.evaluate_alerts(now=t0)
    assert any(e.rule_name == "race-recover" for e in fired)
    j = queue.lease(tenant_id="ar-race", worker_id="w")
    assert j is not None
    _ack(queue, j, "w")

    t_healthy = t0 + timedelta(seconds=1)

    def _eval() -> int:
        r = queue.evaluate_alerts(now=t_healthy)
        return sum(1 for e in r.resolved if e.rule_name == "race-recover")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_eval) for _ in range(8)]
        resolved_counts = [f.result() for f in as_completed(futs)]
    assert sum(resolved_counts) == 0, (
        f"concurrent same-now ticks must not resolve; got {resolved_counts}"
    )
    open_ev = queue.list_alert_events(tenant_id="ar-race", status="open")
    assert any(e.rule_name == "race-recover" for e in open_ev)

    # After the interval, a single sequential tick may advance to resolve.
    later = queue.evaluate_alerts(now=t_healthy + timedelta(seconds=5))
    # streak was 1 from concurrent window, +1 now = 2 → resolve
    assert any(e.rule_name == "race-recover" for e in later.resolved)


def test_auto_pause_still_delivers_alert_webhook(queue: JobQueue):
    """auto_pause must not swallow the alert.fired webhook for that tenant.

    Pause happens after outbox enqueue, and claim allows alert.fired through
    the paused-tenant filter so operators still get the notification.
    """
    sink = _WebhookCapture(fail_times=0)
    try:
        queue.register_webhook(
            tenant_id="wh-pause",
            url=sink.url,
            events=[WebhookEvent.ALERT_FIRED.value],
            secret="w" * 16,
        )
        queue.create_alert_rule(
            name="pause-and-notify",
            metric="dead_count",
            threshold=0,
            comparator="gt",
            scope="tenant",
            tenant_id="wh-pause",
            cooldown_seconds=0,
            auto_pause_tenant=True,
        )
        queue.enqueue(tenant_id="wh-pause", payload={}, max_attempts=1)
        j = queue.lease(tenant_id="wh-pause", worker_id="w")
        assert j is not None
        _nack(queue, j, "w", error="boom", requeue=True)

        fired = queue.evaluate_alerts()
        assert any(e.rule_name == "pause-and-notify" for e in fired)
        assert queue.is_tenant_paused("wh-pause")

        # Outbox row must exist even though tenant is now paused.
        with queue._pool.connection() as conn:
            pending = conn.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM webhook_deliveries
                WHERE tenant_id = %s
                  AND event = %s
                  AND status IN ('pending', 'delivering', 'succeeded')
                """,
                ("wh-pause", WebhookEvent.ALERT_FIRED.value),
            ).fetchone()
        assert pending is not None and int(pending["n"]) >= 1

        # Claim/deliver must succeed while paused (alert.fired exception).
        delivered = False
        for _ in range(25):
            queue.process_webhook_deliveries(limit=20, http_timeout_seconds=2)
            if sink.received:
                delivered = True
                break
            time.sleep(0.05)
        assert delivered, "alert.fired webhook not delivered after auto-pause"
        body = sink.received[-1]["body"]
        assert body["event"] == "alert.fired"
        assert body["rule_name"] == "pause-and-notify"
        assert body.get("detail", {}).get("auto_paused_tenant") == "wh-pause"

        # Job webhooks for the same tenant must still be blocked while paused.
        job_sink = _WebhookCapture(fail_times=0)
        try:
            queue.register_webhook(
                tenant_id="wh-pause",
                url=job_sink.url,
                events=["job.completed"],
                secret="j" * 16,
            )
            queue.enqueue(tenant_id="wh-pause", payload={"blocked": True})
            # Still paused → no lease (and no job.completed webhook path).
            assert queue.lease(tenant_id="wh-pause", worker_id="w3") is None
        finally:
            job_sink.stop()

        queue.resume_tenant("wh-pause")
    finally:
        sink.stop()


def test_alerts_http_api(postgres_url: str):
    import json
    import threading
    import urllib.error
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

        def get_status(path: str) -> tuple[int, dict]:
            try:
                with urllib.request.urlopen(base + path, timeout=5) as r:
                    return r.status, json.loads(r.read().decode())
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode() if exc.fp else "{}"
                try:
                    return exc.code, json.loads(raw or "{}")
                except json.JSONDecodeError:
                    return exc.code, {"error": raw}

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
                "auto_pause_tenant": True,
            },
        )
        assert created["rule"]["name"] == "http-dead"
        assert created["rule"]["auto_pause_tenant"] is True
        rid = created["rule"]["id"]
        rules = get("/v1/alerts/rules")
        assert rules["count"] >= 1
        q.enqueue(tenant_id="http-al", payload={}, max_attempts=1)
        j = q.lease(tenant_id="http-al", worker_id="w")
        assert j is not None
        _nack(q, j, "w", error="z", requeue=True)
        ev = post("/v1/alerts/evaluate", {})
        assert ev["count"] >= 1
        assert q.is_tenant_paused("http-al")
        hist = get("/v1/alerts/events?tenant_id=http-al")
        assert hist["count"] >= 1
        eid = hist["events"][0]["id"]
        resolved = post(
            f"/v1/alerts/events/{eid}/resolve",
            {"tenant_id": "http-al", "resolved_by": "http-test", "note": "ok"},
        )
        assert resolved["event"]["status"] == "resolved"
        muted = post(f"/v1/alerts/rules/{rid}/mute", {"duration_seconds": 600})
        assert muted["rule"]["muted_until"] is not None
        unmuted = post(f"/v1/alerts/rules/{rid}/unmute", {})
        assert unmuted["rule"]["muted_until"] is None
        # Unscoped history listing must be rejected (tenant isolation).
        code, body = get_status("/v1/alerts/events?limit=10")
        assert code == 400
        assert "tenant_id" in str(body.get("error", "")).lower()
        q.resume_tenant("http-al")
        server.shutdown()
    finally:
        q.close()
