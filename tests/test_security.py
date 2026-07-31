"""Security regression tests: tenant isolation, secrets, SSRF, auth."""

from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from urllib.parse import urlencode
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.codec import job_to_dict, webhook_delivery_to_dict
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from job_queue.webhooks import validate_webhook_url


@pytest.fixture
def coordinator_url(queue):
    coord = Coordinator(queue)
    handler = _make_handler(coord)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    t.join(timeout=5)


# ---------------------------------------------------------------------------
# Lease token fencing secret — must not appear on list/get
# ---------------------------------------------------------------------------


def test_http_get_and_list_redact_lease_token(queue: JobQueue, coordinator_url):
    client = CoordinatorClient(coordinator_url)
    # Enqueue via library; lease via HTTP so we know the token was minted.
    j = queue.enqueue(tenant_id="t-sec", payload={"x": 1})
    leased = client.lease(worker_id="w1", tenant_id="t-sec")
    assert leased is not None and leased.id == j.id
    assert leased.lease_token is not None
    token_str = str(leased.lease_token)

    # Wire-level GET must not contain the token.
    q = urlencode({"tenant_id": "t-sec"})
    with urllib.request.urlopen(
        f"{coordinator_url}/v1/jobs/{j.id}?{q}", timeout=5
    ) as resp:
        raw = resp.read()
    assert token_str.encode() not in raw
    body = json.loads(raw)
    assert body["job"]["lease_token"] is None
    assert body["job"]["status"] == "leased"

    with urllib.request.urlopen(
        f"{coordinator_url}/v1/jobs?{urlencode({'tenant_id': 't-sec'})}",
        timeout=5,
    ) as resp:
        list_raw = resp.read()
    assert token_str.encode() not in list_raw
    listed = json.loads(list_raw)
    assert all(row.get("lease_token") is None for row in listed["jobs"])


def test_http_lease_returns_lease_token(queue: JobQueue, coordinator_url):
    queue.enqueue(tenant_id="t-sec", payload={"x": 1})
    client = CoordinatorClient(coordinator_url)
    job = client.lease(worker_id="w1", tenant_id="t-sec")
    assert job is not None
    assert job.lease_token is not None


def test_cross_tenant_get_is_not_found(queue: JobQueue, coordinator_url):
    j = queue.enqueue(tenant_id="alpha", payload={"secret": "payload"})
    queue.lease(tenant_id="alpha", worker_id="w")
    q = urlencode({"tenant_id": "beta"})
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(
            f"{coordinator_url}/v1/jobs/{j.id}?{q}", timeout=5
        )
    assert ei.value.code == 404
    # No payload leakage in error body.
    err = ei.value.read().decode()
    assert "payload" not in err
    assert "secret" not in err


def test_job_to_dict_default_redacts_lease_token(queue: JobQueue):
    j = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.lease_token is not None
    d = job_to_dict(leased)
    assert d["lease_token"] is None
    d2 = job_to_dict(leased, include_lease_token=True)
    assert d2["lease_token"] == str(leased.lease_token)


# ---------------------------------------------------------------------------
# Webhook SSRF
# ---------------------------------------------------------------------------


def test_validate_webhook_url_blocks_private_by_default(monkeypatch):
    monkeypatch.delenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", raising=False)
    with pytest.raises(ValueError, match="non-public"):
        validate_webhook_url("http://127.0.0.1:8090/hook", resolve_dns=False)
    with pytest.raises(ValueError, match="non-public"):
        validate_webhook_url("http://10.0.0.5/hook", resolve_dns=False)
    with pytest.raises(ValueError, match="non-public"):
        validate_webhook_url("http://169.254.169.254/latest/meta-data", resolve_dns=False)
    with pytest.raises(ValueError, match="credentials"):
        validate_webhook_url("https://user:pass@example.com/hook", resolve_dns=False)


def test_validate_webhook_url_allows_private_with_env(monkeypatch):
    monkeypatch.setenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
    assert validate_webhook_url(
        "http://127.0.0.1:8090/hook", resolve_dns=False
    ).startswith("http://127.0.0.1")


def test_register_webhook_rejects_private_url(queue: JobQueue, monkeypatch):
    monkeypatch.delenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", raising=False)
    with pytest.raises(ValueError, match="non-public|not allowed|could not"):
        queue.register_webhook(
            tenant_id="t1",
            url="http://127.0.0.1:9/hook",
            events=["job.completed"],
        )


def test_register_webhook_accepts_private_when_allowed(queue: JobQueue, monkeypatch):
    monkeypatch.setenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
    ep = queue.register_webhook(
        tenant_id="t1",
        url="http://127.0.0.1:9/hook",
        events=["job.completed"],
        secret="s",
    )
    assert ep.url.startswith("http://127.0.0.1")


# ---------------------------------------------------------------------------
# Webhook signing_secret redaction
# ---------------------------------------------------------------------------


def test_webhook_delivery_dict_redacts_signing_secret():
    from datetime import datetime, timezone
    from uuid import uuid4

    from job_queue.models import WebhookDelivery

    d = WebhookDelivery(
        id=uuid4(),
        endpoint_id=uuid4(),
        tenant_id="t1",
        job_id=uuid4(),
        event="job.completed",
        payload={},
        status="pending",
        attempts=0,
        max_attempts=5,
        backoff_base_seconds=1,
        backoff_max_seconds=60,
        next_attempt_at=datetime.now(timezone.utc),
        delivering_until=None,
        last_error=None,
        last_status_code=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        completed_at=None,
        signing_secret="frozen-secret",
    )
    out = webhook_delivery_to_dict(d)
    assert out["signing_secret"] is None
    assert out["has_signing_secret"] is True
    full = webhook_delivery_to_dict(d, include_signing_secret=True)
    assert full["signing_secret"] == "frozen-secret"


def test_claim_exposes_top_level_secret_not_nested(queue: JobQueue, coordinator_url, monkeypatch):
    monkeypatch.setenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
    client = CoordinatorClient(coordinator_url)
    client.register_webhook(
        tenant_id="t1",
        url="http://127.0.0.1:9/hook",
        events=["job.completed"],
        secret="claim-secret",
    )
    j = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    payload = client.claim_webhooks(limit=10)
    claimed = payload.get("claimed") or []
    assert len(claimed) >= 1
    item = claimed[0]
    assert item.get("secret") == "claim-secret"
    # Nested delivery must not duplicate the secret.
    assert item["delivery"].get("signing_secret") is None


# ---------------------------------------------------------------------------
# Auth hardening + 500 sanitization
# ---------------------------------------------------------------------------


def test_auth_wrong_length_token_is_401_not_500(queue: JobQueue):
    coord = Coordinator(queue, api_token="exact-token-value")
    handler = _make_handler(coord)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    url = f"http://127.0.0.1:{port}"
    try:
        req = urllib.request.Request(
            f"{url}/v1/jobs",
            data=b'{"tenant_id":"t","payload":{}}',
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer x",  # wrong length
            },
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(req, timeout=5)
        assert ei.value.code == 401
    finally:
        server.shutdown()
        t.join(timeout=5)


def test_500_response_omits_stack_trace_by_default(queue: JobQueue, monkeypatch):
    monkeypatch.delenv("JOB_QUEUE_DEBUG", raising=False)

    class BoomCoordinator(Coordinator):
        def handle(self, method, path, body, query):  # type: ignore[no-untyped-def]
            if path == "/v1/boom":
                raise RuntimeError("sensitive internal detail")
            return super().handle(method, path, body, query)

    coord = BoomCoordinator(queue)
    handler = _make_handler(coord)
    # Inject a route via monkeypatch of handle already done.
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/boom",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(req, timeout=5)
        assert ei.value.code == 500
        body = json.loads(ei.value.read().decode())
        assert body.get("code") == "internal_error"
        assert "trace" not in body
        assert "sensitive" not in body.get("error", "")
    finally:
        server.shutdown()
        t.join(timeout=5)
