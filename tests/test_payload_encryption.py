"""Per-tenant payload encryption, lease decrypt, and live key rotation."""

from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, JobStatus, PayloadKeyRotation
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.crypto import ENVELOPE_MARKER, is_envelope
from job_queue.remote import CoordinatorClient

from tests.helpers_queue import _ack


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


def _stored_payload(queue: JobQueue, job_id) -> dict:
    with queue._pool.connection() as conn:
        row = conn.execute(
            "SELECT payload, payload_key_id FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        assert row is not None
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return {"payload": payload, "payload_key_id": row["payload_key_id"]}


def test_enqueue_stores_ciphertext_not_plaintext(queue: JobQueue):
    secret = {"email": "user@example.com", "token": "super-secret-1"}
    job = queue.enqueue(tenant_id="acme", payload=secret)
    assert job.payload == secret
    stored = _stored_payload(queue, job.id)
    assert is_envelope(stored["payload"])
    dumped = json.dumps(stored["payload"])
    assert "super-secret-1" not in dumped
    assert "user@example.com" not in dumped
    assert stored["payload_key_id"] is not None
    got = queue.get(job.id, tenant_id="acme")
    assert got is not None
    assert got.payload == secret


def test_lease_returns_plaintext_after_claim(queue: JobQueue):
    queue.enqueue(tenant_id="acme", payload={"n": 7, "kind": "email"})
    leased = queue.lease(tenant_id="acme", worker_id="w1")
    assert leased is not None
    assert leased.payload == {"n": 7, "kind": "email"}
    stored = _stored_payload(queue, leased.id)
    assert is_envelope(stored["payload"])


def test_keys_are_isolated_per_tenant(queue: JobQueue):
    a = queue.enqueue(tenant_id="alpha", payload={"who": "a"})
    b = queue.enqueue(tenant_id="beta", payload={"who": "b"})
    keys_a = queue.list_tenant_payload_keys("alpha")
    keys_b = queue.list_tenant_payload_keys("beta")
    assert len(keys_a) == 1 and keys_a[0].status == "active"
    assert len(keys_b) == 1 and keys_b[0].status == "active"
    assert keys_a[0].key_id != keys_b[0].key_id
    stored_a = _stored_payload(queue, a.id)
    stored_b = _stored_payload(queue, b.id)
    assert stored_a["payload"]["kid"] != stored_b["payload"]["kid"]


def test_live_rotation_old_pending_still_decrypts(queue: JobQueue):
    old = queue.enqueue(tenant_id="rot", payload={"v": "old-secret", "i": 1})
    rot = queue.rotate_tenant_payload_key("rot")
    assert rot.status == "reencrypting"
    assert rot.from_key_id is not None
    assert rot.to_key_id != rot.from_key_id
    fresh = queue.enqueue(tenant_id="rot", payload={"v": "new-secret", "i": 2})
    stored_old = _stored_payload(queue, old.id)
    stored_new = _stored_payload(queue, fresh.id)
    assert stored_old["payload_key_id"] == rot.from_key_id
    assert stored_new["payload_key_id"] == rot.to_key_id
    keys = {k.key_id: k for k in queue.list_tenant_payload_keys("rot")}
    assert keys[rot.from_key_id].status == "decrypt_only"
    assert keys[rot.to_key_id].status == "active"
    leased_old = queue.lease(tenant_id="rot", worker_id="w1")
    leased_new = queue.lease(tenant_id="rot", worker_id="w2")
    assert {leased_old.payload["v"], leased_new.payload["v"]} == {
        "old-secret",
        "new-secret",
    }


def test_advance_rewrites_ciphertext_and_retires_old_key(queue: JobQueue):
    jobs = [
        queue.enqueue(tenant_id="adv", payload={"secret": f"s-{i}", "i": i})
        for i in range(5)
    ]
    rot = queue.rotate_tenant_payload_key("adv")
    advanced = queue.advance_payload_key_rotation("adv", batch_size=2)
    assert advanced.id == rot.id
    assert advanced.jobs_rewrapped >= 1
    # Finish remaining batches (crash-safe resume).
    for _ in range(10):
        advanced = queue.advance_payload_key_rotation("adv", batch_size=2)
        if advanced.status == "complete":
            break
    assert advanced.status == "complete"
    keys = {k.key_id: k for k in queue.list_tenant_payload_keys("adv")}
    assert keys[rot.to_key_id].status == "active"
    assert keys[rot.from_key_id].status == "retired"
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT wrapped_dek, wrap_nonce, status
            FROM tenant_payload_keys
            WHERE tenant_id = %s AND key_id = %s
            """,
            ("adv", rot.from_key_id),
        ).fetchone()
        assert row["status"] == "retired"
        assert bytes(row["wrapped_dek"] or b"") == b""
        assert bytes(row["wrap_nonce"] or b"") == b""
    for j in jobs:
        stored = _stored_payload(queue, j.id)
        assert stored["payload_key_id"] == rot.to_key_id
        assert is_envelope(stored["payload"])
        got = queue.get(j.id, tenant_id="adv")
        assert got is not None
        assert got.payload["secret"].startswith("s-")


def test_crash_mid_rotation_leaks_no_plaintext(queue: JobQueue):
    secrets = [f"crash-secret-{i}" for i in range(6)]
    jobs = [
        queue.enqueue(tenant_id="crash", payload={"secret": s, "i": i})
        for i, s in enumerate(secrets)
    ]
    rot = queue.rotate_tenant_payload_key("crash")
    # Simulate crash after mint + one re-seal batch (some old, some new).
    queue.advance_payload_key_rotation("crash", batch_size=2)
    with queue._pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, payload::text AS txt, payload_key_id FROM jobs WHERE tenant_id = %s",
            ("crash",),
        ).fetchall()
    texts = " ".join(r["txt"] for r in rows)
    for s in secrets:
        assert s not in texts
    kids = {r["payload_key_id"] for r in rows}
    assert rot.from_key_id in kids
    assert rot.to_key_id in kids
    # Fleet keeps working: every job still leases with plaintext.
    seen: list[str] = []
    while True:
        leased = queue.lease(tenant_id="crash", worker_id="w")
        if leased is None:
            break
        seen.append(leased.payload["secret"])
        _ack(queue, leased, "w")
    assert sorted(seen) == sorted(secrets)
    # Resume rotation after "crash"; should complete and retire old key.
    done = queue.process_payload_key_rotations(tenant_id="crash", batch_size=10)
    assert done and done[0].status == "complete"
    keys = {k.key_id: k for k in queue.list_tenant_payload_keys("crash")}
    assert keys[rot.from_key_id].status == "retired"


def test_schedule_payload_encrypted_and_fires_plaintext_jobs(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="sch",
        payload={"task": "heartbeat-check", "secret": "sched-secret"},
        interval_seconds=1,
    )
    assert sched.payload["secret"] == "sched-secret"
    with queue._pool.connection() as conn:
        row = conn.execute(
            "SELECT payload::text AS txt FROM schedules WHERE id = %s",
            (sched.id,),
        ).fetchone()
    assert "sched-secret" not in row["txt"]
    fires = queue.process_due_schedules(tenant_id="sch", limit=1)
    assert len(fires) == 1
    assert fires[0].job.payload["secret"] == "sched-secret"
    stored = _stored_payload(queue, fires[0].job.id)
    assert is_envelope(stored["payload"])
    assert "sched-secret" not in json.dumps(stored["payload"])


def test_http_lease_plaintext_get_redacted(queue: JobQueue, coordinator_url: str):
    client = CoordinatorClient(coordinator_url)
    created = client.enqueue(tenant_id="http-enc", payload={"pw": "hunter2"})
    # Enqueue echo may include payload (caller submitted it); GET must not.
    got = client.get_job(created.id, tenant_id="http-enc")
    assert got is not None
    assert got.payload == {"_encrypted": True}
    leased = client.lease(worker_id="w1", tenant_id="http-enc")
    assert leased is not None
    assert leased.payload == {"pw": "hunter2"}
    listed = client.list_jobs(tenant_id="http-enc")
    assert listed and listed[0].payload == {"_encrypted": True}


def test_http_rotate_and_advance_keep_workers_moving(
    queue: JobQueue, coordinator_url: str
):
    client = CoordinatorClient(coordinator_url)
    ids = []
    for i in range(4):
        j = client.enqueue(
            tenant_id="http-rot", payload={"secret": f"h-{i}", "i": i}
        )
        ids.append(j.id)
    rot = client.rotate_tenant_payload_key("http-rot")
    assert rot["rotation"]["status"] == "reencrypting"
    listed = client.list_tenant_payload_keys("http-rot")
    statuses = {k["status"] for k in listed["keys"]}
    assert "active" in statuses and "decrypt_only" in statuses
    for _ in range(8):
        adv = client.advance_tenant_payload_key("http-rot", batch_size=2)
        if adv["rotation"]["status"] == "complete":
            break
    assert adv["rotation"]["status"] == "complete"
    seen = []
    for _ in range(4):
        leased = client.lease(worker_id="w", tenant_id="http-rot")
        assert leased is not None
        seen.append(leased.payload["secret"])
        client.ack(
            leased.id,
            tenant_id="http-rot",
            worker_id="w",
            lease_token=leased.lease_token,
        )
    assert sorted(seen) == ["h-0", "h-1", "h-2", "h-3"]


def test_merge_moves_keys_and_old_jobs_still_decrypt(queue: JobQueue):
    src = queue.enqueue(tenant_id="src", payload={"from": "source"})
    _dst = queue.enqueue(tenant_id="dst", payload={"from": "dest"})
    result = queue.merge_tenant("src", "dst")
    assert result.source_empty is True
    moved = queue.get(src.id, tenant_id="dst")
    assert moved is not None
    assert moved.payload == {"from": "source"}
    src_keys = queue.list_tenant_payload_keys("src")
    assert src_keys == []
    dst_keys = queue.list_tenant_payload_keys("dst")
    assert any(k.status == "active" for k in dst_keys)
    assert any(k.status == "decrypt_only" for k in dst_keys)


def test_offboard_purges_keys_when_empty(queue: JobQueue):
    queue.enqueue(tenant_id="gone", payload={"x": 1})
    assert queue.list_tenant_payload_keys("gone")
    result = queue.offboard_tenant("gone")
    assert result.empty is True
    assert queue.list_tenant_payload_keys("gone") == []


def test_rebuild_from_audit_keeps_ciphertext(queue: JobQueue):
    j = queue.enqueue(tenant_id="rb", payload={"k": "v", "n": 1})
    leased = queue.lease(tenant_id="rb", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w", result={"ok": True})
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (j.id,))
        conn.commit()
    rebuilt = queue.rebuild_job_from_audit(j.id, tenant_id="rb")
    assert rebuilt.payload == {"k": "v", "n": 1}
    stored = _stored_payload(queue, j.id)
    assert is_envelope(stored["payload"])
    assert stored["payload"][ENVELOPE_MARKER] == 1


def test_webhook_delivery_does_not_store_plaintext_payload(queue: JobQueue):
    """Realistic complete → outbox → POST: secrets stay sealed in Postgres."""
    from job_queue import WebhookEvent
    from tests.helpers_queue import _WebhookCapture

    secret = "wh-delivery-secret-xyz"
    result_secret = "wh-result-secret-xyz"
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="wh-enc",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook-sign",
        )
        job = queue.enqueue(
            tenant_id="wh-enc",
            payload={"token": secret, "email": "a@example.com"},
        )
        leased = queue.lease(tenant_id="wh-enc", worker_id="w1")
        assert leased is not None
        _ack(queue, leased, "w1", result={"receipt": result_secret})

        pending = queue.list_webhook_deliveries(tenant_id="wh-enc", status="pending")
        assert len(pending) == 1
        # Library inspect decrypts so operators/tests still see the POST body.
        assert pending[0].payload["payload"]["token"] == secret
        assert pending[0].payload["result"]["receipt"] == result_secret

        with queue._pool.connection() as conn:
            row = conn.execute(
                "SELECT payload::text AS t, result::text AS r FROM jobs WHERE id = %s",
                (job.id,),
            ).fetchone()
            stored = conn.execute(
                "SELECT payload::text AS t FROM webhook_deliveries WHERE tenant_id = %s",
                ("wh-enc",),
            ).fetchone()
        assert secret not in row["t"]
        assert result_secret not in (row["r"] or "")
        assert secret not in stored["t"]
        assert result_secret not in stored["t"]
        assert "_jq_enc" in stored["t"]

        results = queue.process_webhook_deliveries(limit=10)
        assert len(results) == 1 and results[0].success is True
        assert len(capture.received) == 1
        body = capture.received[0]["body"]
        assert body["payload"]["token"] == secret
        assert body["result"]["receipt"] == result_secret
        assert queue.get_result(job.id, tenant_id="wh-enc") == {
            "receipt": result_secret
        }
    finally:
        capture.stop()


def test_workflow_steps_def_not_plaintext_in_postgres(queue: JobQueue):
    secret = "wf-step-secret-abc"
    run = queue.start_workflow(
        tenant_id="wf-enc",
        name="pay",
        max_attempts=1,
        steps=[
            {
                "name": "charge",
                "payload": {"card": secret},
                "compensate": {"payload": {"undo": secret}},
            }
        ],
    )
    assert run.steps_def[0]["payload"]["card"] == secret
    with queue._pool.connection() as conn:
        txt = conn.execute(
            "SELECT steps_def::text AS t FROM workflow_runs WHERE id = %s",
            (run.id,),
        ).fetchone()["t"]
    assert secret not in txt
    assert "_jq_enc" in txt

    leased = queue.lease(tenant_id="wf-enc", worker_id="w")
    assert leased is not None
    assert leased.payload["step"]["payload"]["card"] == secret
    _ack(queue, leased, "w", result={"ok": True})
    done = queue.get_workflow(run.id, tenant_id="wf-enc")
    assert done is not None and done.status == JobStatus.COMPLETED.value


def test_http_webhook_claim_has_plaintext_list_redacts(
    queue: JobQueue, coordinator_url: str, monkeypatch
):
    monkeypatch.setenv("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
    client = CoordinatorClient(coordinator_url)
    secret = "http-wh-secret"
    client.register_webhook(
        tenant_id="http-wh",
        url="http://127.0.0.1:9/hook",
        events=["job.completed"],
        secret="s",
    )
    client.enqueue(tenant_id="http-wh", payload={"pin": secret})
    leased = client.lease(worker_id="w", tenant_id="http-wh")
    assert leased is not None
    client.ack(
        leased.id,
        tenant_id="http-wh",
        worker_id="w",
        lease_token=leased.lease_token,
        result={"pin": secret},
    )
    claimed = client.claim_webhooks(limit=10)
    rows = claimed.get("claimed") or []
    assert rows
    body = rows[0]["delivery"]["payload"]
    assert body["payload"]["pin"] == secret
    # Wire list/finalize style serialization redacts.
    from job_queue.codec import webhook_delivery_to_dict

    d = queue.list_webhook_deliveries(tenant_id="http-wh")[0]
    redacted = webhook_delivery_to_dict(d)
    dumped = json.dumps(redacted)
    assert secret not in dumped
    assert redacted["payload"] == {"_encrypted": True}


def test_rotation_type_roundtrip(queue: JobQueue):
    queue.enqueue(tenant_id="typ", payload={"a": 1})
    rot = queue.rotate_tenant_payload_key("typ")
    assert isinstance(rot, PayloadKeyRotation)
    fetched = queue.get_payload_key_rotation("typ", rotation_id=rot.id)
    assert fetched is not None
    assert fetched.id == rot.id
    assert fetched.to_key_id == rot.to_key_id
