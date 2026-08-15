"""Clone / re-run an existing job as a new pending job."""

from __future__ import annotations

import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import CloneError, JobQueue, JobStatus, QueueDrainError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _nack


def test_clone_copies_config_and_points_at_source(queue: JobQueue):
    queue.register_worker("w", skills=["gpu"])
    src = queue.enqueue(
        tenant_id="t1",
        queue_name="render",
        payload={"kind": "render", "n": 1},
        priority=7,
        max_attempts=5,
        visibility_timeout_seconds=45,
        max_runtime_seconds=90,
        retry_backoff_base_seconds=2,
        retry_backoff_max_seconds=40,
        labels=["batch", "vip"],
        required_skills=["gpu"],
        estimated_duration_seconds=12,
        preemptible=False,
        max_worker_hops=3,
        quarantine_action="release",
        prefer_last_worker=True,
        ttl_seconds=120,
        expiry_action="cancel",
    )
    leased = queue.lease(tenant_id="t1", worker_id="w", queue_name="render")
    assert leased is not None
    done = _ack(queue, leased, "w", result={"ok": True})
    assert done.status == JobStatus.COMPLETED

    clone = queue.clone_job(src.id, tenant_id="t1")
    assert clone.id != src.id
    assert clone.tenant_id == "t1"
    assert clone.status == JobStatus.PENDING
    assert clone.queue_name == "render"
    assert clone.payload == {"kind": "render", "n": 1}
    assert clone.priority == 7
    assert clone.max_attempts == 5
    assert clone.attempts == 0
    assert clone.visibility_timeout_seconds == 45
    assert clone.max_runtime_seconds == 90
    assert clone.retry_backoff_base_seconds == 2
    assert clone.retry_backoff_max_seconds == 40
    assert clone.labels == ("batch", "vip")
    assert clone.required_skills == ("gpu",)
    assert clone.estimated_duration_seconds == 12
    assert clone.preemptible is False
    assert clone.max_worker_hops == 3
    assert clone.quarantine_action == "release"
    assert clone.prefer_last_worker is True
    assert clone.payload_kind == "render"
    assert clone.cloned_from_job_id == src.id
    assert clone.dedupe_key is None
    assert clone.schedule_id is None
    assert clone.depends_on == ()
    assert clone.parent_job_id is None
    assert clone.waiting_on == ()
    assert clone.result is None
    assert clone.checkpoint is None
    assert clone.last_error is None
    assert clone.failed_workers == ()
    assert clone.leased_by is None
    assert clone.completed_at is None
    assert clone.expiry_action == "cancel"
    assert clone.expires_at is not None
    ttl = (clone.expires_at - clone.created_at).total_seconds()
    assert 118 <= ttl <= 122

    leftover = queue.get(src.id, tenant_id="t1")
    assert leftover is not None
    assert leftover.status == JobStatus.COMPLETED
    assert leftover.result == {"ok": True}
    assert leftover.cloned_from_job_id is None

    listed = queue.list_jobs(tenant_id="t1", cloned_from_job_id=src.id)
    assert [j.id for j in listed] == [clone.id]


def test_clone_payload_override_and_second_clone(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="t1", payload={"kind": "render", "n": 1}, priority=2
    )
    first = queue.clone_job(
        src.id,
        tenant_id="t1",
        payload={"kind": "other", "n": 9, "note": "override"},
    )
    assert first.payload == {"kind": "other", "n": 9, "note": "override"}
    assert first.payload_kind == "other"
    assert first.priority == 2
    assert first.cloned_from_job_id == src.id

    second = queue.clone_job(first.id, tenant_id="t1")
    assert second.cloned_from_job_id == first.id
    assert second.payload == first.payload
    assert second.id != first.id
    kids = queue.list_jobs(tenant_id="t1", cloned_from_job_id=src.id)
    assert {j.id for j in kids} == {first.id}


def test_clone_rejects_missing_and_cross_tenant(queue: JobQueue):
    src = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    with pytest.raises(CloneError, match="not found"):
        queue.clone_job(src.id, tenant_id="t2")
    with pytest.raises(CloneError, match="not found"):
        queue.clone_job(uuid4(), tenant_id="t1")


def test_clone_works_from_live_and_terminal_statuses(queue: JobQueue):
    pending = queue.enqueue(
        tenant_id="t1", queue_name="pend", payload={"kind": "render", "s": "p"}
    )
    assert queue.clone_job(pending.id, tenant_id="t1").status == JobStatus.PENDING

    live = queue.enqueue(
        tenant_id="t1", queue_name="live", payload={"kind": "render", "s": "l"}
    )
    leased = queue.lease(tenant_id="t1", worker_id="w", queue_name="live")
    assert leased is not None and leased.id == live.id
    cloned_live = queue.clone_job(live.id, tenant_id="t1")
    assert cloned_live.status == JobStatus.PENDING
    still = queue.get(live.id, tenant_id="t1")
    assert still is not None and still.status == JobStatus.LEASED

    failed = queue.enqueue(
        tenant_id="t1",
        queue_name="failq",
        payload={"kind": "fail"},
        max_attempts=1,
    )
    bad = queue.lease(tenant_id="t1", worker_id="w2", queue_name="failq")
    assert bad is not None
    dead = _nack(queue, bad, "w2", error="boom", requeue=False)
    assert dead.status == JobStatus.DEAD
    assert dead.failed_workers
    revived = queue.clone_job(dead.id, tenant_id="t1")
    assert revived.status == JobStatus.PENDING
    assert revived.failed_workers == ()
    assert revived.attempts == 0
    assert revived.last_error is None

    cancelled = queue.enqueue(
        tenant_id="t1", queue_name="cx", payload={"kind": "render", "s": "c"}
    )
    queue.cancel(cancelled.id, tenant_id="t1")
    from_cancel = queue.clone_job(cancelled.id, tenant_id="t1")
    assert from_cancel.status == JobStatus.PENDING


def test_clone_does_not_inherit_graph_or_dedupe(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"kind": "parent"})
    child = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "child"},
        depends_on=parent.id,
        dedupe_key="once",
    )
    clone = queue.clone_job(child.id, tenant_id="t1")
    assert clone.depends_on == ()
    assert clone.dedupe_key is None
    assert clone.parent_job_id is None
    # Source still holds the live dedupe key; clone would conflict if inherited.
    with pytest.raises(CloneError):
        queue.clone_job(child.id, tenant_id="t1", dedupe_key="once")
    # After the source is gone from the active set, a keyed clone is allowed.
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == parent.id
    _ack(queue, leased, "w")
    ready = queue.lease(tenant_id="t1", worker_id="w")
    assert ready is not None and ready.id == child.id
    _ack(queue, ready, "w")
    keyed = queue.clone_job(child.id, tenant_id="t1", dedupe_key="once")
    assert keyed.dedupe_key == "once"
    assert keyed.cloned_from_job_id == child.id


def test_clone_shifts_deadline_and_does_not_starve(queue: JobQueue):
    now = _utcnow()
    src = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "render"},
        deadline_at=now + timedelta(seconds=30),
        ttl_seconds=60,
    )
    clone = queue.clone_job(src.id, tenant_id="t1")
    assert clone.deadline_at is not None
    slack = (clone.deadline_at - clone.created_at).total_seconds()
    assert 28 <= slack <= 32
    assert src.deadline_at is not None
    assert clone.deadline_at != src.deadline_at

    picked = queue.lease(tenant_id="t1", worker_id="w")
    assert picked is not None
    _ack(queue, picked, "w")
    leftover = queue.lease(tenant_id="t1", worker_id="w")
    assert leftover is not None
    assert {picked.id, leftover.id} == {src.id, clone.id}
    _ack(queue, leftover, "w")


def test_clone_blocked_by_drain_other_queue_ok(queue: JobQueue):
    email = queue.enqueue(
        tenant_id="t1", queue_name="email", payload={"kind": "mail"}
    )
    sms = queue.enqueue(tenant_id="t1", queue_name="sms", payload={"kind": "sms"})
    queue.drain_tenant_queue("t1", "email")
    with pytest.raises(QueueDrainError, match="draining"):
        queue.clone_job(email.id, tenant_id="t1")
    clone = queue.clone_job(sms.id, tenant_id="t1")
    assert clone.queue_name == "sms"
    queue.undrain_tenant_queue("t1", "email")
    after = queue.clone_job(email.id, tenant_id="t1")
    assert after.queue_name == "email"


def test_clone_audit_and_delay(queue: JobQueue):
    src = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    clone = queue.clone_job(src.id, tenant_id="t1", delay_seconds=30)
    assert clone.available_at > clone.created_at
    src_events = queue.list_job_audit(src.id, tenant_id="t1")
    assert any(e.event_type == "cloned" for e in src_events)
    clone_events = queue.list_job_audit(clone.id, tenant_id="t1")
    assert clone_events[0].event_type == "enqueued"
    assert clone_events[0].detail.get("cloned_from_job_id") == str(src.id)


def test_clone_http_and_list_filter(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="http-clone",
        payload={"kind": "render", "n": 1},
        priority=4,
        labels=["hot"],
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        clone = client.clone_job(
            src.id,
            tenant_id="http-clone",
            payload={"kind": "other", "n": 2},
        )
        assert clone.cloned_from_job_id == src.id
        assert clone.payload == {"kind": "other", "n": 2}
        assert clone.priority == 4
        assert clone.labels == ("hot",)
        kids = client.list_jobs(
            tenant_id="http-clone", cloned_from_job_id=src.id
        )
        assert [j.id for j in kids] == [clone.id]
        with pytest.raises(CloneError):
            client.clone_job(src.id, tenant_id="other")
    finally:
        server.shutdown()


def test_clone_rolls_back_when_source_audit_fails(queue: JobQueue):
    src = queue.enqueue(tenant_id="t1", payload={"kind": "render"})
    orig = queue._append_job_audit

    def boom(*args: object, **kwargs: object):
        if kwargs.get("event_type") == "cloned":
            raise RuntimeError("audit boom")
        return orig(*args, **kwargs)

    queue._append_job_audit = boom  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="audit boom"):
            queue.clone_job(src.id, tenant_id="t1")
    finally:
        queue._append_job_audit = orig  # type: ignore[method-assign]
    assert queue.list_jobs(tenant_id="t1", cloned_from_job_id=src.id) == []
    leftover = [j for j in queue.list_jobs(tenant_id="t1") if j.id != src.id]
    assert leftover == []
    src_events = queue.list_job_audit(src.id, tenant_id="t1")
    assert [e.event_type for e in src_events] == ["enqueued"]


def test_concurrent_clones_same_source_are_atomic(queue: JobQueue):
    src = queue.enqueue(tenant_id="t1", payload={"kind": "render", "n": 1})
    n = 16
    barrier = threading.Barrier(n)
    ok: list = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def go() -> None:
        barrier.wait()
        try:
            job = queue.clone_job(src.id, tenant_id="t1")
            with lock:
                ok.append(job)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(ok) == n
    ids = {j.id for j in ok}
    assert len(ids) == n
    kids = queue.list_jobs(tenant_id="t1", cloned_from_job_id=src.id, limit=500)
    assert {j.id for j in kids} == ids
    cloned = [
        e
        for e in queue.list_job_audit(src.id, tenant_id="t1")
        if e.event_type == "cloned"
    ]
    assert len(cloned) == n
    seqs = [e.seq for e in cloned]
    assert seqs == list(range(min(seqs), min(seqs) + n))
    clone_ids = {e.detail.get("clone_job_id") for e in cloned}
    assert clone_ids == {str(i) for i in ids}


def test_concurrent_clones_two_queues_same_source(
    queue: JobQueue, postgres_url: str
):
    other = JobQueue.connect(
        postgres_url,
        min_size=2,
        max_size=8,
        require_worker_registration=False,
    )
    try:
        src = queue.enqueue(
            tenant_id="dual-clone", payload={"kind": "render"}
        )
        n = 8
        barrier = threading.Barrier(n * 2)
        ok: list = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def go(q: JobQueue) -> None:
            barrier.wait()
            try:
                job = q.clone_job(src.id, tenant_id="dual-clone")
                with lock:
                    ok.append(job)
            except BaseException as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=go, args=(queue,)) for _ in range(n)]
        threads += [threading.Thread(target=go, args=(other,)) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(ok) == n * 2
        kids = queue.list_jobs(
            tenant_id="dual-clone", cloned_from_job_id=src.id, limit=500
        )
        assert len(kids) == n * 2
        cloned = [
            e
            for e in queue.list_job_audit(src.id, tenant_id="dual-clone")
            if e.event_type == "cloned"
        ]
        assert len(cloned) == n * 2
        seqs = [e.seq for e in cloned]
        assert len(set(seqs)) == n * 2
    finally:
        other.close()


def test_http_concurrent_clones_same_source(queue: JobQueue):
    src = queue.enqueue(
        tenant_id="http-race", payload={"kind": "render", "n": 1}
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    n = 20
    barrier = threading.Barrier(n)
    ok: list = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def go() -> None:
        client = CoordinatorClient(f"http://{host}:{port}")
        barrier.wait()
        last: BaseException | None = None
        for _ in range(2):
            try:
                job = client.clone_job(src.id, tenant_id="http-race")
                with lock:
                    ok.append(job)
                return
            except BaseException as exc:  # noqa: BLE001
                last = exc
        with lock:
            errors.append(last or RuntimeError("clone failed"))

    try:
        threads = [threading.Thread(target=go) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(ok) == n
        kids = queue.list_jobs(
            tenant_id="http-race", cloned_from_job_id=src.id, limit=500
        )
        assert len(kids) == n
        cloned = [
            e
            for e in queue.list_job_audit(src.id, tenant_id="http-race")
            if e.event_type == "cloned"
        ]
        assert len(cloned) == n
    finally:
        server.shutdown()
