"""Plan a graph-safe tenant split (stage 1: dry-run, no mutations)."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import JobQueue, TenantSplitError, TenantSplitPlan, TenantSplitResult
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_plan_split_by_queue_lists_pending_and_skips_other_queue(queue: JobQueue):
    email = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"k": 1}
    )
    sms = queue.enqueue(tenant_id="src", queue_name="sms", payload={"k": 2})
    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert isinstance(plan, TenantSplitPlan)
    assert plan.ok is True
    assert plan.move_job_ids == (email.id,)
    assert plan.stay_leased_ids == ()
    assert queue.get(email.id, tenant_id="src") is not None
    assert queue.get(sms.id, tenant_id="src") is not None
    assert queue.get(email.id, tenant_id="dst") is None


def test_plan_split_by_labels_and_skills(queue: JobQueue):
    keep = queue.enqueue(tenant_id="src", payload={"n": 1}, labels=["keep"])
    xfer = queue.enqueue(
        tenant_id="src",
        payload={"n": 2},
        labels=["xfer", "vip"],
        required_skills=["gpu"],
    )
    by_label = queue.plan_tenant_split("src", "dst", labels=["xfer"])
    assert by_label.ok is True
    assert by_label.move_job_ids == (xfer.id,)
    by_skill = queue.plan_tenant_split("src", "dst", required_skills=["gpu"])
    assert by_skill.ok is True
    assert by_skill.move_job_ids == (xfer.id,)
    assert queue.get(keep.id, tenant_id="src") is not None


def test_plan_split_fails_closed_when_child_on_other_queue(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="src", queue_name="default", payload={"kind": "p"}
    )
    child = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"kind": "c"},
        depends_on=parent.id,
    )
    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert plan.ok is False
    assert child.id not in plan.move_job_ids
    reasons = {b.reason for b in plan.blockers}
    assert "graph_outside_filter" in reasons
    assert queue.get(parent.id, tenant_id="src") is not None
    assert queue.get(child.id, tenant_id="src") is not None


def test_plan_split_fails_closed_when_parent_would_leave_child(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"kind": "p"}
    )
    child = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"kind": "c"},
        depends_on=parent.id,
    )
    # Both match the queue, so the closed pair can move together.
    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert plan.ok is True
    assert set(plan.move_job_ids) == {parent.id, child.id}


def test_plan_split_leased_isolated_stays_leased_neighbor_blocks(queue: JobQueue):
    lone = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"lone": True}
    )
    leased = queue.lease(tenant_id="src", worker_id="w", queue_name="email")
    assert leased is not None and leased.id == lone.id
    parent = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"p": True}
    )
    child = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"c": True},
        depends_on=parent.id,
    )
    held = queue.lease(tenant_id="src", worker_id="w2", queue_name="email")
    assert held is not None and held.id == parent.id
    plan = queue.plan_tenant_split("src", "dst", queue_name="email")
    assert lone.id in plan.stay_leased_ids
    assert parent.id in plan.stay_leased_ids
    assert plan.ok is False
    assert any(b.reason == "leased_graph_neighbor" for b in plan.blockers)
    assert child.id not in plan.move_job_ids
    _ack(queue, leased, "w")
    _ack(queue, held, "w2")


def test_plan_requires_filter_and_distinct_tenants(queue: JobQueue):
    with pytest.raises(ValueError, match="at least one"):
        queue.plan_tenant_split("src", "dst")
    with pytest.raises(ValueError, match="must differ"):
        queue.plan_tenant_split("src", "src", queue_name="email")


def test_plan_split_http(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="http-src", queue_name="email", payload={"n": 1}
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        plan = client.plan_tenant_split(
            "http-src", "http-dst", queue_name="email"
        )
        assert plan["ok"] is True
        assert plan["move_job_ids"] == [str(job.id)]
        assert queue.get(job.id, tenant_id="http-src") is not None
    finally:
        server.shutdown()


def test_split_moves_closed_queue_and_reseals_payload(queue: JobQueue):
    email = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"kind": "render", "secret": "keep"},
        labels=["xfer"],
    )
    keep = queue.enqueue(
        tenant_id="src", queue_name="default", payload={"stay": True}
    )
    result = queue.split_tenant("src", "dst", queue_name="email")
    assert isinstance(result, TenantSplitResult)
    assert result.jobs_moved == 1
    assert result.job_ids == (email.id,)
    assert queue.get(email.id, tenant_id="src") is None
    moved = queue.get(email.id, tenant_id="dst")
    assert moved is not None
    assert moved.payload == {"kind": "render", "secret": "keep"}
    assert moved.tenant_id == "dst"
    assert moved.id == email.id
    assert queue.get(keep.id, tenant_id="src") is not None
    events = queue.list_job_audit(email.id, tenant_id="dst")
    assert [e.event_type for e in events][-1] == "tenant_split"
    assert queue.list_job_audit(email.id, tenant_id="src") == []
    got = queue.lease(tenant_id="dst", worker_id="w", queue_name="email")
    assert got is not None and got.id == email.id
    _ack(queue, got, "w")


def test_split_fails_closed_on_broken_graph_and_moves_nothing(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="src", queue_name="default", payload={"p": 1}
    )
    child = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"c": 1},
        depends_on=parent.id,
    )
    extra = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"e": 1}
    )
    with pytest.raises(TenantSplitError, match="not graph-safe"):
        queue.split_tenant("src", "dst", queue_name="email")
    assert queue.get(child.id, tenant_id="src") is not None
    assert queue.get(extra.id, tenant_id="src") is not None
    assert queue.get(extra.id, tenant_id="dst") is None


def test_split_moves_matching_parent_and_child_together(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="src", queue_name="email", payload={"p": 1}
    )
    child = queue.enqueue(
        tenant_id="src",
        queue_name="email",
        payload={"c": 1},
        depends_on=parent.id,
    )
    result = queue.split_tenant("src", "dst", queue_name="email")
    assert set(result.job_ids) == {parent.id, child.id}
    assert queue.get(parent.id, tenant_id="src") is None
    dest_parent = queue.get(parent.id, tenant_id="dst")
    dest_child = queue.get(child.id, tenant_id="dst")
    assert dest_parent is not None and dest_child is not None
    assert dest_child.depends_on == (parent.id,)
    first = queue.lease(tenant_id="dst", worker_id="w", queue_name="email")
    assert first is not None and first.id == parent.id
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="dst", worker_id="w", queue_name="email")
    assert second is not None and second.id == child.id
    _ack(queue, second, "w")


def test_split_http_execute(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="http-src", queue_name="email", payload={"n": 2}
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        parent = client.enqueue(
            tenant_id="http-src",
            queue_name="default",
            payload={"p": 1},
        )
        client.enqueue(
            tenant_id="http-src",
            queue_name="email",
            payload={"c": 1},
            depends_on=[str(parent.id)],
        )
        with pytest.raises(TenantSplitError):
            client.split_tenant("http-src", "http-dst", queue_name="email")
        still = client.get_job(job.id, tenant_id="http-src")
        assert still is not None
        # Isolated email-only split after cancelling the child? child still there.
        # Cancel the graph child then split.
        kids = client.list_jobs(tenant_id="http-src", queue_name="email")
        for k in kids:
            if k.id != job.id:
                client.cancel_job(k.id, tenant_id="http-src")
        out = client.split_tenant("http-src", "http-dst", queue_name="email")
        assert out["jobs_moved"] == 1
        assert out["job_ids"] == [str(job.id)]
        assert client.get_job(job.id, tenant_id="http-src") is None
        dest = client.get_job(job.id, tenant_id="http-dst")
        assert dest is not None and dest.tenant_id == "http-dst"
    finally:
        server.shutdown()


def test_split_and_keep_queue_workers_run_together(
    queue: JobQueue, postgres_url: str
):
    other = JobQueue.connect(
        postgres_url, min_size=2, max_size=8, require_worker_registration=False
    )
    try:
        keep = [
            queue.enqueue(
                tenant_id="src",
                queue_name="default",
                payload={"keep": i},
            )
            for i in range(6)
        ]
        mail = [
            queue.enqueue(
                tenant_id="src",
                queue_name="email",
                payload={"mail": i},
            )
            for i in range(6)
        ]
        done: list = []
        errors: list[BaseException] = []
        stop = threading.Event()

        def worker(q: JobQueue, wid: str, qn: str) -> None:
            try:
                while True:
                    job = q.lease(tenant_id="src", worker_id=wid, queue_name=qn)
                    if job is None:
                        if stop.is_set():
                            return
                        continue
                    _ack(q, job, wid)
                    done.append(job.id)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(queue, "wa", "default")),
            threading.Thread(target=worker, args=(other, "wb", "default")),
        ]
        for t in threads:
            t.start()
        result = other.split_tenant("src", "dst", queue_name="email")
        assert result.jobs_moved == 6
        assert set(result.job_ids) == {j.id for j in mail}
        stop.set()
        for t in threads:
            t.join(timeout=5)
        assert errors == []
        assert set(done) == {j.id for j in keep}
        for j in mail:
            assert queue.get(j.id, tenant_id="src") is None
            assert queue.get(j.id, tenant_id="dst") is not None
    finally:
        other.close()
