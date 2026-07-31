"""Distributed fleet smoke tests (new file; does not rewrite existing suite)."""

from __future__ import annotations

import threading
import time

import pytest

from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient, RemoteWorkerAgent
from http.server import ThreadingHTTPServer


@pytest.fixture
def coordinator_url(queue, postgres_url):
    """In-process coordinator bound to an ephemeral port."""
    # queue fixture already has schema + clean tables against session postgres.
    coord = Coordinator(queue)
    handler = _make_handler(coord)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    url = f"http://127.0.0.1:{port}"
    yield url
    server.shutdown()
    t.join(timeout=5)


def test_remote_worker_fair_lease_ack_via_http(queue, coordinator_url):
    client = CoordinatorClient(coordinator_url)
    assert client.health()["ok"] is True

    queue.enqueue(tenant_id="a", payload={"n": 1})
    queue.enqueue(tenant_id="b", payload={"n": 2})

    agent = RemoteWorkerAgent(
        client,
        worker_id="test-w1",
        tenant_id=None,
        poll_interval_seconds=0.1,
        heartbeat_interval_seconds=1.0,
        visibility_timeout_seconds=30,
    )
    th = threading.Thread(target=agent.run, daemon=True)
    th.start()
    deadline = time.time() + 20
    while time.time() < deadline and agent.processed < 2:
        time.sleep(0.1)
    agent.stop()
    th.join(timeout=5)
    assert agent.processed >= 2

    backlog = client.backlog()
    assert backlog["jobs"]["completed"] >= 2
    assert backlog["jobs"]["pending"] == 0
    assert backlog["jobs"]["leased"] == 0


def test_schedule_and_webhook_via_coordinator(queue, coordinator_url, monkeypatch):
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json

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

    client = CoordinatorClient(coordinator_url)
    client.register_webhook(
        tenant_id="t1",
        url=sink_url,
        events=["job.completed"],
    )
    sched = client.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"x": 1},
        name="one-shot-ish",
    )
    assert sched["id"]

    n = client.process_due_schedules()
    assert n >= 1

    agent = RemoteWorkerAgent(client, worker_id="w-sched", poll_interval_seconds=0.1)
    th = threading.Thread(target=agent.run, daemon=True)
    th.start()
    deadline = time.time() + 20
    while time.time() < deadline and agent.processed < 1:
        time.sleep(0.1)
    agent.stop()
    th.join(timeout=5)

    # Agent claims via coordinator, POSTs itself, then finalizes.
    from job_queue.remote import WebhookNode

    wh = WebhookNode(client, poll_interval_seconds=0.1, batch_limit=10)
    for _ in range(30):
        wh.run_once()
        if received:
            break
        time.sleep(0.1)
    sink.shutdown()

    assert agent.processed >= 1
    assert len(received) == 1
    assert received[0]["tenant_id"] == "t1"
    assert received[0]["event"] == "job.completed"


def test_claim_finalize_no_coordinator_post(queue, coordinator_url):
    """Coordinator claim returns endpoint; agent POSTs; finalize persists."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import json

    posts: list[str] = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length") or "0")
            body = json.loads(self.rfile.read(n).decode("utf-8"))
            posts.append(body.get("delivery_id") or body.get("job_id"))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            return

    sink = HTTPServer(("127.0.0.1", 0), H)
    port = sink.server_address[1]
    threading.Thread(target=sink.serve_forever, daemon=True).start()

    client = CoordinatorClient(coordinator_url)
    client.register_webhook(
        tenant_id="t1",
        url=f"http://127.0.0.1:{port}/hook",
        events=["job.completed"],
    )
    queue.enqueue(tenant_id="t1", payload={"z": 1})
    job = queue.lease(tenant_id="t1", worker_id="w")
    assert job is not None
    queue.ack(
        job.id,
        tenant_id="t1",
        worker_id="w",
        lease_token=job.lease_token,
    )

    claimed = client.claim_webhooks(limit=10)
    assert claimed["count"] == 1
    item = claimed["claimed"][0]
    assert item["url"].endswith("/hook")
    assert item["delivery"]["status"] == "delivering"

    from job_queue.webhooks import post_webhook

    code, _ = post_webhook(
        item["url"],
        item["delivery"]["payload"],
        secret=item.get("secret"),
        idempotency_key=item["delivery"]["id"],
    )
    assert code == 200
    fin = client.finalize_webhook(item["delivery"]["id"], status_code=code)
    assert fin["success"] is True
    assert fin["delivery"]["status"] == "succeeded"
    assert len(posts) == 1
    sink.shutdown()


def test_offboard_via_coordinator_http(queue, coordinator_url):
    """POST /v1/tenants/{id}/offboard clears pending work; in-flight may finish."""
    client = CoordinatorClient(coordinator_url)

    queue.enqueue(tenant_id="http-gone", payload={"pending": 1})
    queue.enqueue(tenant_id="http-gone", payload={"run": 1}, priority=10)
    leased = queue.lease(tenant_id="http-gone", worker_id="local-w")
    assert leased is not None
    assert leased.payload == {"run": 1}

    queue.create_schedule(
        tenant_id="http-gone",
        interval_seconds=60,
        payload={"s": 1},
        name="http-sched",
    )
    queue.register_webhook(
        tenant_id="http-gone",
        url="http://example.test/hook",
    )

    result = client.offboard_tenant("http-gone")
    assert result["empty"] is False
    assert result["leased_remaining"] == 1
    assert result["offboarding"] is True
    body = result["result"]
    assert body["pending_cancelled"] >= 1
    assert body["schedules_deleted"] == 1
    assert body["webhooks_deleted"] == 1

    # Resume blocked.
    with pytest.raises(Exception):
        client.resume_tenant("http-gone")

    # Finish in-flight via coordinator HTTP ack.
    ack = client.ack(
        leased.id,
        tenant_id="http-gone",
        worker_id="local-w",
        lease_token=leased.lease_token,
        result={"done": True},
    )
    assert ack.status.value == "completed" or str(ack.status) == "completed"

    assert queue.tenant_is_empty("http-gone") is True


def test_coordinator_rejects_oversized_body(coordinator_url):
    """Forged Content-Length must not pin the server reading multi-GB bodies."""
    import http.client
    import json
    from urllib.parse import urlparse

    from job_queue.coordinator import _MAX_REQUEST_BODY_BYTES

    # Claim a body just over the cap without actually sending multi-MB of data:
    # the server rejects based on Content-Length before reading the full body.
    oversized = _MAX_REQUEST_BODY_BYTES + 1
    parsed = urlparse(coordinator_url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=5)
    try:
        conn.putrequest("POST", "/v1/jobs")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(oversized))
        conn.putheader("Accept", "application/json")
        conn.endheaders()
        conn.send(b"{}")
        resp = conn.getresponse()
        body = resp.read()
        assert resp.status == 413, body
        payload = json.loads(body.decode("utf-8"))
        assert "too large" in payload.get("error", "").lower()
    finally:
        conn.close()


def test_coordinator_rejects_invalid_content_length(coordinator_url):
    import http.client
    import json
    from urllib.parse import urlparse

    parsed = urlparse(coordinator_url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=5)
    try:
        payload = b'{"tenant_id":"t1","payload":{}}'
        conn.putrequest("POST", "/v1/jobs")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", "not-a-number")
        conn.putheader("Accept", "application/json")
        conn.endheaders()
        conn.send(payload)
        resp = conn.getresponse()
        body = resp.read()
        assert resp.status == 400, body
        data = json.loads(body.decode("utf-8"))
        assert "content-length" in data.get("error", "").lower()
    finally:
        conn.close()
