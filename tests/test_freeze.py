"""Per-job freeze: hold one job, release or dead-letter, isolate tenants."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer
import pytest

from job_queue import FreezeError, JobQueue, JobStatus, LeaseError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _WebhookCapture, _ack, _hb, _nack


def test_freeze_pending_blocks_lease_sibling_still_runs(queue: JobQueue):
    held = queue.enqueue(tenant_id="a", payload={"hold": True}, priority=10)
    other = queue.enqueue(tenant_id="a", payload={"go": True}, priority=1)
    frozen = queue.freeze(held.id, tenant_id="a", reason="inspect")
    assert frozen.status == JobStatus.FROZEN
    assert frozen.is_frozen is True
    assert frozen.last_error == "inspect"
    job = queue.lease(tenant_id="a", worker_id="w")
    assert job is not None
    assert job.id == other.id
    _ack(queue, job, "w")
    assert queue.lease(tenant_id="a", worker_id="w2") is None
    still = queue.get(held.id, tenant_id="a")
    assert still is not None and still.status == JobStatus.FROZEN


def test_freeze_does_not_pause_other_tenant(queue: JobQueue):
    hold = queue.enqueue(tenant_id="bad", payload={"x": 1})
    queue.enqueue(tenant_id="good", payload={"y": 1})
    queue.freeze(hold.id, tenant_id="bad")
    job = queue.lease(worker_id="fair")
    assert job is not None
    assert job.tenant_id == "good"
    _ack(queue, job, "fair")
    assert queue.lease(tenant_id="bad", worker_id="w") is None
    assert queue.get(hold.id, tenant_id="bad").status == JobStatus.FROZEN


def test_other_tenant_cannot_freeze_or_unfreeze_or_dead(queue: JobQueue):
    job = queue.enqueue(tenant_id="owner", payload={"x": 1})
    with pytest.raises(FreezeError, match="not found"):
        queue.freeze(job.id, tenant_id="intruder")
    queue.freeze(job.id, tenant_id="owner")
    with pytest.raises(FreezeError, match="not found"):
        queue.unfreeze(job.id, tenant_id="intruder")
    with pytest.raises(FreezeError, match="not found"):
        queue.dead_letter_frozen(job.id, tenant_id="intruder")
    assert queue.get(job.id, tenant_id="owner").status == JobStatus.FROZEN
    # Intruder cannot lease it either.
    assert queue.lease(tenant_id="intruder", worker_id="w") is None


def test_freeze_leased_fences_holder(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"x": 1}, max_attempts=3)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    frozen = queue.freeze(leased.id, tenant_id="t1", reason="stop")
    assert frozen.status == JobStatus.FROZEN
    assert frozen.lease_token is None
    assert frozen.leased_by is None
    with pytest.raises(LeaseError):
        _hb(queue, leased, "w1")
    with pytest.raises(LeaseError):
        _ack(queue, leased, "w1", result={"stolen": True})
    with pytest.raises(LeaseError):
        _nack(queue, leased, "w1", error="nope")
    got = queue.get(leased.id, tenant_id="t1")
    assert got.status == JobStatus.FROZEN
    assert queue.lease(tenant_id="t1", worker_id="w2") is None


def test_unfreeze_returns_to_pending_and_can_complete(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"n": 1})
    queue.freeze(job.id, tenant_id="t1")
    assert queue.lease(tenant_id="t1", worker_id="w") is None
    thawed = queue.unfreeze(job.id, tenant_id="t1")
    assert thawed.status == JobStatus.PENDING
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    assert leased.id == job.id
    done = _ack(queue, leased, "w", result={"ok": True})
    assert done.status == JobStatus.COMPLETED


def test_dead_letter_frozen_notifies_and_stays_dead(queue: JobQueue):
    capture = _WebhookCapture()
    try:
        queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.dead"],
            backoff_base_seconds=0,
        )
        child_parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
        child = queue.enqueue(
            tenant_id="t1",
            payload={"c": 1},
            depends_on=[child_parent.id],
        )
        queue.freeze(child_parent.id, tenant_id="t1")
        dead = queue.dead_letter_frozen(
            child_parent.id, tenant_id="t1", error="bad payload"
        )
        assert dead.status == JobStatus.DEAD
        assert dead.last_error == "bad payload"
        dep = queue.get(child.id, tenant_id="t1")
        assert dep is not None and dep.status == JobStatus.CANCELLED
        assert queue.lease(tenant_id="t1", worker_id="w") is None
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        assert pending[0].payload.get("last_error") == "bad payload"
        results = queue.process_webhook_deliveries()
        assert len(results) == 1 and results[0].success
        assert capture.received[-1]["body"]["event"] == "job.dead"
    finally:
        capture.stop()


def test_freeze_completed_rejected_and_idempotent(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    with pytest.raises(FreezeError, match="cannot be frozen"):
        queue.freeze(job.id, tenant_id="t1")
    live = queue.enqueue(tenant_id="t1", payload={"a": 1})
    first = queue.freeze(live.id, tenant_id="t1")
    second = queue.freeze(live.id, tenant_id="t1")
    assert first.status == second.status == JobStatus.FROZEN
    with pytest.raises(FreezeError, match="not frozen"):
        queue.unfreeze(job.id, tenant_id="t1")


def test_freeze_vs_lease_race_consistent_state(queue: JobQueue):
    job = queue.enqueue(tenant_id="race", payload={"r": 1})
    outcomes: list[str] = []
    barrier = threading.Barrier(3)

    def leaser(wid: str) -> None:
        barrier.wait()
        leased = queue.lease(tenant_id="race", worker_id=wid)
        if leased is None:
            outcomes.append("miss")
            return
        try:
            _ack(queue, leased, wid, result={"w": wid})
            outcomes.append("acked")
        except LeaseError:
            outcomes.append("fenced")

    def freezer() -> None:
        barrier.wait()
        try:
            queue.freeze(job.id, tenant_id="race", reason="race")
            outcomes.append("frozen")
        except FreezeError:
            outcomes.append("freeze_fail")

    threads = [
        threading.Thread(target=leaser, args=("w1",)),
        threading.Thread(target=leaser, args=("w2",)),
        threading.Thread(target=freezer),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    got = queue.get(job.id, tenant_id="race")
    assert got is not None
    assert got.status in (JobStatus.FROZEN, JobStatus.COMPLETED)
    if got.status == JobStatus.FROZEN:
        assert got.lease_token is None
        assert queue.lease(tenant_id="race", worker_id="later") is None
        assert "acked" not in outcomes
    else:
        assert got.result is not None
        assert got.lease_token is None


def test_unfreeze_vs_dead_letter_one_winner(queue: JobQueue):
    job = queue.enqueue(tenant_id="race2", payload={"r": 2})
    queue.freeze(job.id, tenant_id="race2")
    hits: list[str] = []
    barrier = threading.Barrier(2)

    def thaw() -> None:
        barrier.wait()
        try:
            queue.unfreeze(job.id, tenant_id="race2")
            hits.append("unfrozen")
        except FreezeError:
            hits.append("unfreeze_fail")

    def kill() -> None:
        barrier.wait()
        try:
            queue.dead_letter_frozen(job.id, tenant_id="race2", error="gone")
            hits.append("dead")
        except FreezeError:
            hits.append("dead_fail")

    t1 = threading.Thread(target=thaw)
    t2 = threading.Thread(target=kill)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    got = queue.get(job.id, tenant_id="race2")
    assert got is not None
    assert got.status in (JobStatus.PENDING, JobStatus.DEAD)
    assert hits.count("unfrozen") + hits.count("dead") == 1
    if got.status == JobStatus.DEAD:
        assert queue.lease(tenant_id="race2", worker_id="w") is None
        assert got.last_error == "gone"
    else:
        leased = queue.lease(tenant_id="race2", worker_id="w")
        assert leased is not None
        _ack(queue, leased, "w")


def test_ttl_does_not_expire_frozen_job(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="ttl-f",
        payload={"x": 1},
        ttl_seconds=1,
        expiry_action="dead",
    )
    queue.freeze(job.id, tenant_id="ttl-f")
    time.sleep(1.1)
    out = queue.process_expired_jobs(tenant_id="ttl-f")
    assert out["dead"] == 0
    assert out["cancelled"] == 0
    assert queue.get(job.id, tenant_id="ttl-f").status == JobStatus.FROZEN


def test_dedupe_held_while_frozen(queue: JobQueue):
    a = queue.enqueue(
        tenant_id="d", payload={"n": 1}, dedupe_key="same", queue_name="default"
    )
    queue.freeze(a.id, tenant_id="d")
    b = queue.enqueue(
        tenant_id="d", payload={"n": 2}, dedupe_key="same", queue_name="default"
    )
    assert b.id == a.id
    assert b.status == JobStatus.FROZEN


def test_freeze_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        job = client.enqueue(tenant_id="http-f", payload={"n": 1})
        frozen = client.freeze_job(job.id, tenant_id="http-f", reason="hold")
        assert frozen.status == JobStatus.FROZEN
        assert client.lease(tenant_id="http-f", worker_id="w") is None
        with pytest.raises(FreezeError):
            client.freeze_job(job.id, tenant_id="other")
        other = client.enqueue(tenant_id="other", payload={"ok": True})
        leased = client.lease(tenant_id="other", worker_id="w")
        assert leased is not None
        assert leased.id == other.id
        thawed = client.unfreeze_job(job.id, tenant_id="http-f")
        assert thawed.status == JobStatus.PENDING
        again = client.enqueue(tenant_id="http-f", payload={"n": 2})
        client.freeze_job(again.id, tenant_id="http-f")
        dead = client.dead_letter_frozen(
            again.id, tenant_id="http-f", error="drop"
        )
        assert dead.status == JobStatus.DEAD
        assert dead.last_error == "drop"
    finally:
        server.shutdown()


def test_freeze_matching_by_label_leaves_unlabeled_and_other_labels(queue: JobQueue):
    red = [
        queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            labels=["hold:red", "kind:work"],
        )
        for i in range(4)
    ]
    blue = queue.enqueue(
        tenant_id="t1", payload={"blue": True}, labels=["hold:blue"]
    )
    bare = queue.enqueue(tenant_id="t1", payload={"bare": True})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None

    result = queue.freeze_matching(
        tenant_id="t1",
        labels=["hold:red"],
        reason="bulk hold",
    )
    assert result.action == "freeze"
    assert result.count == 4
    assert result.complete is True
    for j in red:
        got = queue.get(j.id, tenant_id="t1")
        assert got is not None and got.status == JobStatus.FROZEN
    # Unlabeled / other-label jobs still claimable (unless we happened to
    # lease one of the reds before freeze — then blue/bare remain).
    survivors = {blue.id, bare.id}
    still = queue.get(leased.id, tenant_id="t1")
    assert still is not None
    if still.id in survivors:
        assert still.status in (JobStatus.PENDING, JobStatus.LEASED, JobStatus.FROZEN)
        if still.status == JobStatus.LEASED:
            _ack(queue, leased, "w1")
    remaining = [
        j
        for j in (queue.get(blue.id, tenant_id="t1"), queue.get(bare.id, tenant_id="t1"))
        if j is not None and j.status != JobStatus.COMPLETED
    ]
    assert all(j.status != JobStatus.FROZEN for j in remaining)
    nxt = queue.lease(tenant_id="t1", worker_id="w2")
    if nxt is not None:
        assert nxt.id in survivors
        assert nxt.id not in {j.id for j in red}
        _ack(queue, nxt, "w2")


def test_freeze_matching_tenant_isolation(queue: JobQueue):
    for tid in ("iso-a", "iso-b"):
        for i in range(3):
            queue.enqueue(
                tenant_id=tid,
                payload={"i": i},
                labels=["batch:shared"],
            )
    res = queue.freeze_matching(tenant_id="iso-a", labels=["batch:shared"])
    assert res.count == 3
    assert all(j.tenant_id == "iso-a" for j in res.jobs)
    other = queue.list_jobs(tenant_id="iso-b", labels=["batch:shared"])
    assert len(other) == 3
    assert all(j.status == JobStatus.PENDING for j in other)
    job = queue.lease(tenant_id="iso-b", worker_id="w")
    assert job is not None
    _ack(queue, job, "w")
    assert queue.lease(tenant_id="iso-a", worker_id="w") is None


def test_unfreeze_matching_restores_only_matching_frozen(queue: JobQueue):
    hold = [
        queue.enqueue(tenant_id="t1", payload={"i": i}, labels=["hold:red"])
        for i in range(3)
    ]
    keep = queue.enqueue(tenant_id="t1", payload={"k": 1}, labels=["hold:blue"])
    queue.freeze_matching(tenant_id="t1", labels=["hold:red"])
    queue.freeze(keep.id, tenant_id="t1")
    out = queue.unfreeze_matching(tenant_id="t1", labels=["hold:red"])
    assert out.action == "unfreeze"
    assert out.count == 3
    for j in hold:
        got = queue.get(j.id, tenant_id="t1")
        assert got is not None and got.status == JobStatus.PENDING
    still = queue.get(keep.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.FROZEN
    claimed = 0
    while True:
        job = queue.lease(tenant_id="t1", worker_id="w")
        if job is None:
            break
        assert job.id != keep.id
        _ack(queue, job, "w")
        claimed += 1
    assert claimed == 3


def test_freeze_matching_queue_name_and_max_jobs(queue: JobQueue):
    for i in range(4):
        queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            labels=["q:x"],
            queue_name="alpha",
        )
    queue.enqueue(
        tenant_id="t1",
        payload={"other": True},
        labels=["q:x"],
        queue_name="beta",
    )
    partial = queue.freeze_matching(
        tenant_id="t1",
        labels=["q:x"],
        queue_name="alpha",
        max_jobs=2,
        batch_size=2,
    )
    assert partial.count == 2
    assert partial.complete is False
    assert partial.remaining_estimate == 2
    rest = queue.freeze_matching(
        tenant_id="t1", labels=["q:x"], queue_name="alpha"
    )
    assert rest.count == 2
    assert rest.complete is True
    beta = queue.list_jobs(tenant_id="t1", queue_name="beta")
    assert len(beta) == 1
    assert beta[0].status == JobStatus.PENDING


def test_freeze_matching_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        for i in range(3):
            client.enqueue(
                tenant_id="http-m",
                payload={"i": i},
                labels=["hold:red"],
            )
        client.enqueue(tenant_id="http-m", payload={"bare": True})
        client.enqueue(
            tenant_id="http-other",
            payload={"x": 1},
            labels=["hold:red"],
        )
        res = client.freeze_matching(
            tenant_id="http-m", labels=["hold:red"], reason="soak"
        )
        assert res.count == 3
        assert res.action == "freeze"
        assert client.lease(tenant_id="http-m", worker_id="w") is not None
        other = client.lease(tenant_id="http-other", worker_id="w")
        assert other is not None
        assert other.tenant_id == "http-other"
        thawed = client.unfreeze_matching(tenant_id="http-m", labels=["hold:red"])
        assert thawed.count == 3
        assert thawed.action == "unfreeze"
    finally:
        server.shutdown()
