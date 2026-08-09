"""Job TTL: unfinished work is cancelled or dead-lettered."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer

from job_queue import JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_ttl_dead_letters_unmatched_skill_job(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="ttl-d",
        payload={"need": "tpu"},
        required_skills=["tpu"],
        ttl_seconds=1,
        expiry_action="dead",
    )
    assert j.status == JobStatus.AWAITING_SKILLS
    assert j.expires_at is not None
    assert j.expiry_action == "dead"
    time.sleep(1.1)
    out = queue.process_expired_jobs(tenant_id="ttl-d")
    assert out["dead"] == 1
    got = queue.get(j.id, tenant_id="ttl-d")
    assert got is not None and got.status == JobStatus.DEAD
    assert "ttl expired" in (got.last_error or "")
    usage = queue.get_tenant_usage("ttl-d")
    assert usage.dead_letters >= 1


def test_ttl_cancel_unmatched_job(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="ttl-c",
        payload={"need": "asic"},
        required_skills=["asic"],
        ttl_seconds=1,
        expiry_action="cancel",
    )
    assert j.status == JobStatus.AWAITING_SKILLS
    time.sleep(1.1)
    out = queue.process_expired_jobs()
    assert out["cancelled"] >= 1
    got = queue.get(j.id, tenant_id="ttl-c")
    assert got is not None and got.status == JobStatus.CANCELLED


def test_ttl_does_not_touch_completed_jobs(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="ttl-ok", payload={"x": 1}, ttl_seconds=1, expiry_action="dead"
    )
    leased = queue.lease(tenant_id="ttl-ok", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    time.sleep(1.1)
    out = queue.process_expired_jobs(tenant_id="ttl-ok")
    assert out["dead"] == 0 and out["cancelled"] == 0
    got = queue.get(j.id, tenant_id="ttl-ok")
    assert got is not None and got.status == JobStatus.COMPLETED


def test_tenant_default_ttl_and_action(queue: JobQueue):
    queue.set_tenant_job_ttl("ttl-def", 1, expiry_action="cancel")
    got = queue.get_tenant_job_ttl("ttl-def")
    assert got["ttl_seconds"] == 1
    assert got["expiry_action"] == "cancel"
    j = queue.enqueue(
        tenant_id="ttl-def",
        payload={"need": "rare"},
        required_skills=["rare"],
    )
    assert j.expiry_action == "cancel"
    assert j.expires_at is not None
    time.sleep(1.1)
    queue.process_expired_jobs(tenant_id="ttl-def")
    row = queue.get(j.id, tenant_id="ttl-def")
    assert row is not None and row.status == JobStatus.CANCELLED


def test_per_job_ttl_overrides_tenant_default(queue: JobQueue):
    queue.set_tenant_job_ttl("ttl-ov", 3600, expiry_action="cancel")
    j = queue.enqueue(
        tenant_id="ttl-ov",
        payload={"need": "rare"},
        required_skills=["rare"],
        ttl_seconds=1,
        expiry_action="dead",
    )
    assert j.expiry_action == "dead"
    time.sleep(1.1)
    queue.process_expired_jobs(tenant_id="ttl-ov")
    row = queue.get(j.id, tenant_id="ttl-ov")
    assert row is not None and row.status == JobStatus.DEAD


def test_expires_at_absolute(queue: JobQueue):
    past = _utcnow() - timedelta(seconds=1)
    j = queue.enqueue(
        tenant_id="ttl-abs",
        payload={"need": "x"},
        required_skills=["x"],
        expires_at=past,
        expiry_action="dead",
    )
    out = queue.process_expired_jobs(tenant_id="ttl-abs")
    assert out["dead"] == 1
    got = queue.get(j.id, tenant_id="ttl-abs")
    assert got is not None and got.status == JobStatus.DEAD


def test_lease_does_not_claim_ttl_expired_job(queue: JobQueue):
    queue.enqueue(
        tenant_id="ttl-skip",
        payload={"plain": True},
        ttl_seconds=1,
        expiry_action="dead",
    )
    time.sleep(1.1)
    # Maintenance on lease should expire it; nothing claimable.
    leased = queue.lease(tenant_id="ttl-skip", worker_id="w")
    assert leased is None
    parked = queue.list_jobs(tenant_id="ttl-skip", status="dead")
    assert len(parked) == 1


def test_ttl_expires_leased_job(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="ttl-run",
        payload={"slow": True},
        ttl_seconds=1,
        expiry_action="cancel",
    )
    leased = queue.lease(tenant_id="ttl-run", worker_id="w")
    assert leased is not None and leased.id == j.id
    time.sleep(1.1)
    queue.process_expired_jobs(tenant_id="ttl-run")
    got = queue.get(j.id, tenant_id="ttl-run")
    assert got is not None and got.status == JobStatus.CANCELLED
    assert got.lease_token is None


def test_concurrent_ttl_expiry_no_double_terminal(queue: JobQueue):
    n = 20
    ids = []
    for i in range(n):
        j = queue.enqueue(
            tenant_id="ttl-race",
            payload={"i": i},
            required_skills=["gone"],
            ttl_seconds=1,
            expiry_action="dead",
        )
        ids.append(j.id)
    time.sleep(1.1)
    errors: list[BaseException] = []
    results: list[dict] = []

    def go() -> None:
        try:
            results.append(queue.process_expired_jobs(tenant_id="ttl-race", limit=8))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors
    dead = 0
    for jid in ids:
        got = queue.get(jid, tenant_id="ttl-race")
        assert got is not None
        assert got.status == JobStatus.DEAD
        dead += 1
    assert dead == n
    assert sum(r["dead"] for r in results) == n


def test_http_ttl_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        client.set_tenant_job_ttl("http-ttl", 1, expiry_action="dead")
        cfg = client.get_tenant_job_ttl("http-ttl")
        assert cfg["ttl_seconds"] == 1
        job = client.enqueue(
            tenant_id="http-ttl",
            payload={"need": "nope"},
            required_skills=["nope"],
        )
        assert job.expiry_action == "dead"
        time.sleep(1.1)
        out = client.process_expired_jobs(tenant_id="http-ttl")
        assert out["dead"] >= 1
        got = client.get_job(job.id, tenant_id="http-ttl")
        assert got is not None and got.status == JobStatus.DEAD
    finally:
        server.shutdown()
