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
