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

    # Deliver webhook via coordinator.
    for _ in range(20):
        client.process_webhooks()
        if received:
            break
        time.sleep(0.2)
    sink.shutdown()

    assert agent.processed >= 1
    assert len(received) == 1
    assert received[0]["tenant_id"] == "t1"
    assert received[0]["event"] == "job.completed"
