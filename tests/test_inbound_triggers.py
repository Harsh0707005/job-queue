"""Signed inbound HTTP triggers: enqueue, DAG, workflow, signal."""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import (
    NONCE_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    JobQueue,
    JobStatus,
    sign_inbound,
)
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.models import WorkflowRunStatus
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


@pytest.fixture
def coordinator_url(queue: JobQueue):
    coord = Coordinator(queue, api_token="fleet-token")
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        t.join(timeout=5)


def _signed_post(url: str, secret: str, body: dict, *, nonce: str | None = None):
    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    nonce = nonce or uuid4().hex
    sig = sign_inbound(secret, raw, timestamp=ts, nonce=nonce)
    req = urllib.request.Request(
        url,
        data=raw,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sig,
            TIMESTAMP_HEADER: ts,
            NONCE_HEADER: nonce,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read().decode() or "{}")
        return int(exc.code), payload


def test_inbound_job_dag_workflow_signal(queue: JobQueue, coordinator_url: str):
    client = CoordinatorClient(coordinator_url, api_token="fleet-token")
    created = client.create_inbound_trigger(
        tenant_id="in-a",
        name="ops",
        secret="super-secret-trigger-1",
        kinds=["job", "dag", "workflow", "signal"],
    )
    trig = created["trigger"]
    assert trig["secret"] == "super-secret-trigger-1"
    assert trig["path"] == f"/v1/inbound/{trig['id']}"
    listed = client.list_inbound_triggers(tenant_id="in-a")
    assert listed["triggers"][0]["secret"] is None

    fire = f"{coordinator_url}{trig['path']}"
    code, job_out = _signed_post(
        fire,
        trig["secret"],
        {"action": "job", "payload": {"who": "ext", "n": 1}},
    )
    assert code == 201
    assert job_out["job"]["tenant_id"] == "in-a"
    assert job_out["job"]["payload"]["who"] == "ext"
    leased = queue.lease(tenant_id="in-a", worker_id="w")
    assert leased is not None
    assert leased.payload == {"who": "ext", "n": 1}
    _ack(queue, leased, "w")

    code, dag_out = _signed_post(
        fire,
        trig["secret"],
        {
            "action": "dag",
            "batch_key": "ext-dag",
            "jobs": [
                {"ref": "a", "payload": {"step": "a"}},
                {"ref": "b", "payload": {"step": "b"}, "depends_on": ["a"]},
            ],
        },
    )
    assert code == 201, dag_out
    assert dag_out["created"] is True
    assert len(dag_out["jobs"]) == 2
    for _ in range(2):
        node = queue.lease(tenant_id="in-a", worker_id="w")
        assert node is not None
        _ack(queue, node, "w")

    code, wf = _signed_post(
        fire,
        trig["secret"],
        {
            "action": "workflow",
            "name": "gate",
            "steps": [
                {"name": "reserve", "kind": "effect", "payload": {"op": "r"}},
                {
                    "name": "gate",
                    "kind": "wait_signal",
                    "signal": "go",
                    "timeout_seconds": 30,
                },
                {"name": "ship", "kind": "effect", "payload": {"op": "s"}},
            ],
        },
    )
    assert code == 201
    run_id = wf["run"]["id"]
    first = queue.lease(tenant_id="in-a", worker_id="w")
    assert first is not None
    _ack(queue, first, "w")
    code, sig = _signed_post(
        fire,
        trig["secret"],
        {"action": "signal", "run_id": run_id, "signal": "go", "payload": {"ok": 1}},
    )
    assert code == 201
    assert sig["created"] is True
    assert sig["unblocked"] == 1
    ship = queue.lease(tenant_id="in-a", worker_id="w")
    assert ship is not None
    _ack(queue, ship, "w")
    done = queue.get_workflow(run_id, tenant_id="in-a")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_inbound_tenant_isolation_and_auth(queue: JobQueue, coordinator_url: str):
    client = CoordinatorClient(coordinator_url, api_token="fleet-token")
    a = client.create_inbound_trigger(
        tenant_id="iso-a",
        name="a",
        secret="aaaaaaaaaaaaaaaa",
        kinds=["job", "signal"],
    )["trigger"]
    b = client.create_inbound_trigger(
        tenant_id="iso-b",
        name="b",
        secret="bbbbbbbbbbbbbbbb",
        kinds=["job"],
    )["trigger"]
    ja = queue.enqueue(tenant_id="iso-a", payload={"n": 1})
    assert ja.tenant_id == "iso-a"

    # No API token on fire; wrong secret is 401.
    code, err = _signed_post(
        f"{coordinator_url}/v1/inbound/{a['id']}",
        b["secret"],
        {"action": "job", "payload": {"x": 1}},
    )
    assert code == 401
    assert err["code"] == "bad_signature"

    # Body tenant_id cannot hijack another tenant.
    code, err = _signed_post(
        f"{coordinator_url}/v1/inbound/{a['id']}",
        a["secret"],
        {"action": "job", "tenant_id": "iso-b", "payload": {"x": 1}},
    )
    assert code == 403

    # Kind not allowed on B.
    code, err = _signed_post(
        f"{coordinator_url}/v1/inbound/{b['id']}",
        b["secret"],
        {"action": "signal", "run_id": str(uuid4()), "signal": "go"},
    )
    assert code == 403

    # Signal for the other tenant's run is not-found (no leak).
    run = queue.start_workflow(
        tenant_id="iso-b",
        steps=[{"name": "only", "kind": "effect", "payload": {"op": "x"}}],
    )
    code, err = _signed_post(
        f"{coordinator_url}/v1/inbound/{a['id']}",
        a["secret"],
        {"action": "signal", "run_id": str(run.id), "signal": "go"},
    )
    assert code == 404

    # Unsigned fire is rejected even though the path is public.
    req = urllib.request.Request(
        f"{coordinator_url}/v1/inbound/{a['id']}",
        data=b'{"action":"job","payload":{"x":1}}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=5)
    assert ei.value.code == 401

    # Management still requires the API token.
    req = urllib.request.Request(
        f"{coordinator_url}/v1/triggers?tenant_id=iso-a",
        method="GET",
    )
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=5)
    assert ei.value.code == 401


def test_inbound_replay_and_stale_and_offboard(
    queue: JobQueue, coordinator_url: str
):
    client = CoordinatorClient(coordinator_url, api_token="fleet-token")
    trig = client.create_inbound_trigger(
        tenant_id="in-r",
        name="r",
        secret="replay-secret-xxxx",
        kinds=["job"],
    )["trigger"]
    fire = f"{coordinator_url}/v1/inbound/{trig['id']}"
    nonce = "once-only"
    code, _ = _signed_post(
        fire, trig["secret"], {"action": "job", "payload": {"n": 1}}, nonce=nonce
    )
    assert code == 201
    code, err = _signed_post(
        fire, trig["secret"], {"action": "job", "payload": {"n": 2}}, nonce=nonce
    )
    assert code == 409
    assert err["code"] == "replay"

    raw = json.dumps({"action": "job", "payload": {"n": 3}}).encode()
    ts = str(int(time.time()) - 10_000)
    nonce2 = uuid4().hex
    sig = sign_inbound(trig["secret"], raw, timestamp=ts, nonce=nonce2)
    req = urllib.request.Request(
        fire,
        data=raw,
        headers={
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sig,
            TIMESTAMP_HEADER: ts,
            NONCE_HEADER: nonce2,
        },
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=5)
    assert ei.value.code == 401

    queue.offboard_tenant("in-r")
    code, err = _signed_post(
        fire, trig["secret"], {"action": "job", "payload": {"n": 9}}
    )
    assert code == 404
    # Empty offboard wipes the tenant; a new trigger can be registered.


def test_inbound_disabled_and_queue_isolation(queue: JobQueue, coordinator_url: str):
    client = CoordinatorClient(coordinator_url, api_token="fleet-token")
    trig = client.create_inbound_trigger(
        tenant_id="in-d",
        name="d",
        secret="disabled-secret-xx",
        kinds=["job"],
    )["trigger"]
    client.set_inbound_trigger_enabled(trig["id"], tenant_id="in-d", enabled=False)
    code, err = _signed_post(
        f"{coordinator_url}/v1/inbound/{trig['id']}",
        trig["secret"],
        {"action": "job", "payload": {"n": 1}},
    )
    assert code == 403
    client.set_inbound_trigger_enabled(trig["id"], tenant_id="in-d", enabled=True)
    code, out = _signed_post(
        f"{coordinator_url}/v1/inbound/{trig['id']}",
        trig["secret"],
        {"action": "job", "payload": {"n": 1}},
    )
    assert code == 201
    assert queue.count(tenant_id="keep", status=JobStatus.PENDING) == 0
    assert queue.count(tenant_id="in-d", status=JobStatus.PENDING) == 1
    listed = client.list_inbound_triggers(tenant_id="keep")
    assert listed["triggers"] == []


def test_inbound_job_callback_signed_on_success_and_dead(queue: JobQueue):
    from job_queue import WebhookEvent, verify_webhook_signature
    from tests.helpers_queue import _WebhookCapture, _nack

    cb = _WebhookCapture()
    hook = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="cb-a",
            url=hook.url,
            events=[WebhookEvent.JOB_COMPLETED.value, WebhookEvent.JOB_DEAD.value],
            secret="tenant-hook-secret",
        )
        trig = queue.create_inbound_trigger(
            tenant_id="cb-a",
            name="with-cb",
            secret="trigger-secret-xxx",
            kinds=["job"],
        )
        out = queue.fire_inbound_trigger(
            trig.id,
            {
                "action": "job",
                "payload": {"n": 1},
                "callback_url": cb.url,
            },
            nonce="cb-ok",
        )
        job = out["job"]
        leased = queue.lease(tenant_id="cb-a", worker_id="w")
        assert leased is not None and leased.id == job.id
        _ack(queue, leased, "w", result={"ok": True})
        results = queue.process_webhook_deliveries(limit=10)
        assert any(r.success for r in results)
        cb_hit = next(r for r in cb.received if r["body"].get("trigger_id") == str(trig.id))
        got = cb_hit["body"]
        assert got["event"] == "job.completed"
        assert got["job_id"] == str(job.id)
        assert got["status"] == "completed"
        assert got["result"] == {"ok": True}
        assert got["tenant_id"] == "cb-a"
        raw = json.dumps(got, default=str, separators=(",", ":")).encode()
        assert verify_webhook_signature("trigger-secret-xxx", raw, cb_hit["signature"])
        assert any(r["body"].get("event") == "job.completed" for r in hook.received)

        dead = queue.fire_inbound_trigger(
            trig.id,
            {
                "action": "job",
                "payload": {"n": 2},
                "callback_url": cb.url,
                "max_attempts": 1,
            },
            nonce="cb-dead",
        )
        inbound_dead = queue.lease(tenant_id="cb-a", worker_id="wd")
        assert inbound_dead is not None
        assert inbound_dead.id == dead["job"].id
        _nack(queue, inbound_dead, "wd", error="boom", requeue=True)
        queue.process_webhook_deliveries(limit=20)
        assert any(
            r["body"].get("event") == "job.dead"
            and r["body"].get("job_id") == str(dead["job"].id)
            for r in cb.received
        )
    finally:
        cb.stop()
        hook.stop()


def test_inbound_callback_survives_crash_and_does_not_double_deliver(
    queue: JobQueue,
):
    from tests.helpers_queue import _WebhookCapture

    cb = _WebhookCapture()
    try:
        trig = queue.create_inbound_trigger(
            tenant_id="cb-crash",
            name="crash",
            secret="crash-callback-xxx",
            kinds=["job"],
        )
        out = queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 1}, "callback_url": cb.url},
            nonce="cb-crash-1",
        )
        leased = queue.lease(tenant_id="cb-crash", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"done": True})
        pending = queue.list_webhook_deliveries(
            tenant_id="cb-crash", status="pending"
        )
        assert len(pending) == 1
        assert pending[0].endpoint_id is None

        def crash_after_post(_item):
            raise RuntimeError("coordinator died after claim, before POST")

        with pytest.raises(RuntimeError, match="died after claim"):
            queue.process_webhook_deliveries(
                limit=5, claim_seconds=0.01, on_before_post=crash_after_post
            )
        assert cb.received == []
        delivering = queue.list_webhook_deliveries(
            tenant_id="cb-crash", status="delivering"
        )
        assert len(delivering) == 1
        import time

        time.sleep(0.02)

        # Reclaim + POST. Then crash after 2xx is persisted, before terminal flip.
        real_note = queue.note_webhook_last_status

        def crash_after_2xx(delivery_id, status_code):
            real_note(delivery_id, status_code)
            raise RuntimeError("crash after POST before finalize")

        queue.note_webhook_last_status = crash_after_2xx  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="before finalize"):
            queue.process_webhook_deliveries(limit=5, claim_seconds=0.01)
        assert len(cb.received) == 1
        queue.note_webhook_last_status = real_note  # type: ignore[method-assign]

        # Reclaim must auto-succeed from the stored 2xx, not POST again.
        time.sleep(0.02)
        results = queue.process_webhook_deliveries(limit=5)
        assert any(r.success for r in results)
        assert len(cb.received) == 1
        succeeded = queue.list_webhook_deliveries(
            tenant_id="cb-crash", status="succeeded"
        )
        assert len(succeeded) == 1
        assert succeeded[0].id == pending[0].id
    finally:
        cb.stop()


def test_offboard_delivers_pending_inbound_callback_then_empties(queue: JobQueue):
    """Offboard pauses the tenant but must still claim the kept callback."""
    from job_queue import verify_webhook_signature
    from tests.helpers_queue import _WebhookCapture

    cb = _WebhookCapture()
    hook = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="cb-ob",
            url=hook.url,
            events=["job.completed"],
            secret="tenant-hook-ob",
        )
        trig = queue.create_inbound_trigger(
            tenant_id="cb-ob",
            name="ob-pending",
            secret="ob-pending-secretx",
            kinds=["job"],
        )
        out = queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 1}, "callback_url": cb.url},
            nonce="ob-pending-1",
        )
        leased = queue.lease(tenant_id="cb-ob", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"ok": True})
        pending = queue.list_webhook_deliveries(tenant_id="cb-ob", status="pending")
        assert any(d.endpoint_id is None for d in pending)
        assert any(d.endpoint_id is not None for d in pending)

        result = queue.offboard_tenant("cb-ob")
        assert result.empty is False
        assert result.offboarding is True
        assert result.webhooks_delivering_remaining >= 1
        assert queue.is_tenant_paused("cb-ob") is True

        # Tenant webhook pending rows are purged; inbound callback is kept.
        left = queue.list_webhook_deliveries(tenant_id="cb-ob")
        assert len(left) == 1
        assert left[0].endpoint_id is None
        assert left[0].status == "pending"

        results = queue.process_webhook_deliveries(limit=10)
        assert any(r.success and r.delivery.endpoint_id is None for r in results)
        assert len(cb.received) == 1
        body = cb.received[0]["body"]
        assert body["job_id"] == str(out["job"].id)
        assert body["event"] == "job.completed"
        raw = json.dumps(body, default=str, separators=(",", ":")).encode()
        assert verify_webhook_signature(
            "ob-pending-secretx", raw, cb.received[0]["signature"]
        )
        assert hook.received == []
        assert queue.tenant_is_empty("cb-ob") is True
        assert queue.is_tenant_offboarding("cb-ob") is False
    finally:
        cb.stop()
        hook.stop()


def test_offboard_delivers_callback_after_in_flight_ack(queue: JobQueue):
    """Leased inbound job acks after offboard; callback still POSTs."""
    from tests.helpers_queue import _WebhookCapture

    cb = _WebhookCapture()
    try:
        trig = queue.create_inbound_trigger(
            tenant_id="cb-ob-lease",
            name="ob-lease",
            secret="ob-lease-secretxxx",
            kinds=["job"],
        )
        out = queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 2}, "callback_url": cb.url},
            nonce="ob-lease-1",
        )
        leased = queue.lease(tenant_id="cb-ob-lease", worker_id="w")
        assert leased is not None and leased.id == out["job"].id

        result = queue.offboard_tenant("cb-ob-lease")
        assert result.offboarding is True
        assert result.leased_remaining == 1
        _ack(queue, leased, "w", result={"after": True})

        results = queue.process_webhook_deliveries(limit=10)
        assert any(r.success for r in results)
        assert len(cb.received) == 1
        assert cb.received[0]["body"]["job_id"] == str(out["job"].id)
        assert queue.tenant_is_empty("cb-ob-lease") is True
    finally:
        cb.stop()


def test_offboard_reclaims_inbound_callback_instead_of_dead_letter(queue: JobQueue):
    """Expired delivering callbacks must be re-POSTed, not reaped as dead."""
    from tests.helpers_queue import _WebhookCapture

    cb = _WebhookCapture()
    try:
        trig = queue.create_inbound_trigger(
            tenant_id="cb-ob-reap",
            name="ob-reap",
            secret="ob-reap-secretxxxxx",
            kinds=["job"],
        )
        queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 3}, "callback_url": cb.url},
            nonce="ob-reap-1",
        )
        leased = queue.lease(tenant_id="cb-ob-reap", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"ok": True})

        claimed, auto = queue.claim_webhook_deliveries(
            limit=5, claim_seconds=0.05
        )
        assert auto == []
        assert len(claimed) == 1
        assert claimed[0].delivery.endpoint_id is None

        result = queue.offboard_tenant("cb-ob-reap")
        assert result.offboarding is True
        assert result.webhooks_delivering_remaining == 1

        time.sleep(0.08)
        results = queue.process_webhook_deliveries(limit=5, claim_seconds=5.0)
        assert any(r.success for r in results)
        assert len(cb.received) == 1
        assert queue.tenant_is_empty("cb-ob-reap") is True
    finally:
        cb.stop()


def test_paused_tenant_still_delivers_inbound_callback(queue: JobQueue):
    """Regular pause blocks tenant webhooks but not inbound callbacks."""
    from tests.helpers_queue import _WebhookCapture

    cb = _WebhookCapture()
    hook = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="cb-pause",
            url=hook.url,
            events=["job.completed"],
            secret="tenant-hook-pause",
        )
        trig = queue.create_inbound_trigger(
            tenant_id="cb-pause",
            name="pause-cb",
            secret="pause-callback-xxx",
            kinds=["job"],
        )
        queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 4}, "callback_url": cb.url},
            nonce="pause-cb-1",
        )
        leased = queue.lease(tenant_id="cb-pause", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w", result={"ok": True})
        assert len(queue.list_webhook_deliveries(tenant_id="cb-pause")) == 2

        queue.pause_tenant("cb-pause")
        results = queue.process_webhook_deliveries(limit=10)
        assert any(r.success and r.delivery.endpoint_id is None for r in results)
        assert len(cb.received) == 1
        assert hook.received == []
        still = [
            d
            for d in queue.list_webhook_deliveries(tenant_id="cb-pause")
            if d.endpoint_id is not None
        ]
        assert len(still) == 1
        assert still[0].status == "pending"

        queue.resume_tenant("cb-pause")
        more = queue.process_webhook_deliveries(limit=10)
        assert any(r.success and r.delivery.endpoint_id is not None for r in more)
        assert len(hook.received) == 1
    finally:
        cb.stop()
        hook.stop()


def _nonce_rows(queue: JobQueue, trigger_id, nonce: str) -> int:
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM inbound_trigger_nonces
            WHERE trigger_id = %s AND nonce = %s
            """,
            (trigger_id, nonce),
        ).fetchone()
    return int(row["n"]) if row else 0


def test_inbound_retry_after_crash_between_nonce_and_job(
    queue: JobQueue, monkeypatch
):
    """Nonce must not stick if the coordinator dies before the job commits.

    Simulates: nonce INSERT succeeds in the fire txn, then enqueue blows up
    (process kill / exception) before COMMIT. Retry with the same nonce
    must create the job, not return replay.
    """
    trig = queue.create_inbound_trigger(
        tenant_id="in-crash",
        name="crash",
        secret="crash-secret-xxxx",
        kinds=["job"],
    )
    nonce = "crash-mid-create"
    real_enqueue = queue._enqueue_pending_on_conn

    def die_after_nonce(*args, **kwargs):
        raise RuntimeError("coordinator died after nonce, before job")

    monkeypatch.setattr(queue, "_enqueue_pending_on_conn", die_after_nonce)
    with pytest.raises(RuntimeError, match="died after nonce"):
        queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 1}},
            nonce=nonce,
        )
    assert _nonce_rows(queue, trig.id, nonce) == 0
    assert queue.count(tenant_id="in-crash", status=JobStatus.PENDING) == 0

    monkeypatch.setattr(queue, "_enqueue_pending_on_conn", real_enqueue)
    out = queue.fire_inbound_trigger(
        trig.id,
        {"action": "job", "payload": {"n": 1}},
        nonce=nonce,
    )
    assert out["action"] == "job"
    assert out["job"].payload == {"n": 1}
    assert _nonce_rows(queue, trig.id, nonce) == 1
    assert queue.count(tenant_id="in-crash", status=JobStatus.PENDING) == 1

    with pytest.raises(FileExistsError, match="replayed nonce"):
        queue.fire_inbound_trigger(
            trig.id,
            {"action": "job", "payload": {"n": 2}},
            nonce=nonce,
        )
    assert queue.count(tenant_id="in-crash", status=JobStatus.PENDING) == 1
