"""Live coalesce / fold of pending jobs that share a key."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import CoalesceError, CoalesceResult, JobQueue, JobStatus
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def test_coalesce_absorbs_pending_extras_into_oldest(queue: JobQueue):
    first = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, coalesce_key="invoice-42"
    )
    extras = [
        queue.enqueue(
            tenant_id="t1", payload={"n": i}, coalesce_key="invoice-42"
        )
        for i in range(2, 5)
    ]
    keep = queue.enqueue(tenant_id="t1", payload={"stay": True})
    out = queue.coalesce_jobs("t1", "invoice-42")
    assert isinstance(out, CoalesceResult)
    assert out.survivor_job_id == first.id
    assert out.jobs_folded == 3
    assert set(out.folded_job_ids) == {j.id for j in extras}
    assert queue.get(first.id, tenant_id="t1") is not None
    assert queue.get(first.id, tenant_id="t1").status == JobStatus.PENDING
    for extra in extras:
        got = queue.get(extra.id, tenant_id="t1")
        assert got is not None and got.status == JobStatus.CANCELLED
    assert queue.get(keep.id, tenant_id="t1").status == JobStatus.PENDING
    events = queue.list_job_audit(first.id, tenant_id="t1")
    assert [e.event_type for e in events][-1] == "coalesce_absorbed"


def test_coalesce_is_tenant_and_queue_scoped(queue: JobQueue):
    a = queue.enqueue(
        tenant_id="alpha", payload={}, coalesce_key="same"
    )
    queue.enqueue(tenant_id="alpha", payload={}, coalesce_key="same")
    b = queue.enqueue(
        tenant_id="beta", payload={}, coalesce_key="same"
    )
    queue.enqueue(tenant_id="beta", payload={}, coalesce_key="same")
    c = queue.enqueue(
        tenant_id="alpha",
        payload={},
        coalesce_key="same",
        queue_name="other",
    )
    queue.enqueue(
        tenant_id="alpha",
        payload={},
        coalesce_key="same",
        queue_name="other",
    )
    out_a = queue.coalesce_jobs("alpha", "same")
    assert out_a.survivor_job_id == a.id
    assert out_a.jobs_folded == 1
    assert queue.get(b.id, tenant_id="beta").status == JobStatus.PENDING
    out_c = queue.coalesce_jobs("alpha", "same", queue_name="other")
    assert out_c.survivor_job_id == c.id
    assert out_c.jobs_folded == 1
    assert queue.coalesce_jobs("beta", "same").jobs_folded == 1


def test_coalesce_remaps_dependents_and_does_not_cascade(queue: JobQueue):
    extra = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, coalesce_key="k"
    )
    survivor = queue.enqueue(
        tenant_id="t1", payload={"n": 2}, coalesce_key="k"
    )
    child = queue.enqueue(
        tenant_id="t1", payload={"c": 1}, depends_on=extra.id
    )
    out = queue.coalesce_jobs("t1", "k", keep_job_id=survivor.id)
    assert extra.id in out.folded_job_ids
    assert queue.get(extra.id, tenant_id="t1").status == JobStatus.CANCELLED
    live = queue.get(child.id, tenant_id="t1")
    assert live is not None
    assert live.status == JobStatus.PENDING
    assert live.depends_on == (survivor.id,)
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None and first.id == survivor.id
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="t1", worker_id="w")
    assert second is not None and second.id == child.id
    _ack(queue, second, "w")


def test_coalesce_unions_depends_on_onto_survivor(queue: JobQueue):
    parent_a = queue.enqueue(tenant_id="t1", payload={"pa": 1})
    parent_b = queue.enqueue(tenant_id="t1", payload={"pb": 1})
    left = queue.enqueue(
        tenant_id="t1",
        payload={"l": 1},
        coalesce_key="fold",
        depends_on=parent_a.id,
    )
    right = queue.enqueue(
        tenant_id="t1",
        payload={"r": 1},
        coalesce_key="fold",
        depends_on=parent_b.id,
    )
    out = queue.coalesce_jobs("t1", "fold", keep_job_id=left.id)
    assert out.jobs_folded == 1
    assert out.folded_job_ids == (right.id,)
    surv = queue.get(left.id, tenant_id="t1")
    assert set(surv.depends_on) == {parent_a.id, parent_b.id}
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id in {parent_a.id, parent_b.id}


def test_coalesce_leased_is_survivor_and_pending_does_not_run(queue: JobQueue):
    extra = queue.enqueue(
        tenant_id="t1", payload={"n": 1}, coalesce_key="live"
    )
    hold = queue.enqueue(
        tenant_id="t1", payload={"n": 2}, coalesce_key="live"
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == extra.id
    out = queue.coalesce_jobs("t1", "live")
    assert out.survivor_job_id == extra.id
    assert hold.id in out.folded_job_ids
    assert queue.get(hold.id, tenant_id="t1").status == JobStatus.CANCELLED
    assert queue.lease(tenant_id="t1", worker_id="w2") is None
    _ack(queue, leased, "w", result={"ok": True})
    assert queue.get_result(extra.id, tenant_id="t1") == {"ok": True}


def test_coalesce_refuses_two_leased(queue: JobQueue):
    a = queue.enqueue(tenant_id="t1", payload={"n": 1}, coalesce_key="race")
    b = queue.enqueue(tenant_id="t1", payload={"n": 2}, coalesce_key="race")
    first = queue.lease(tenant_id="t1", worker_id="w1")
    second = queue.lease(tenant_id="t1", worker_id="w2")
    assert {first.id, second.id} == {a.id, b.id}
    with pytest.raises(CoalesceError, match="leased"):
        queue.coalesce_jobs("t1", "race")
    assert queue.get(a.id, tenant_id="t1").status == JobStatus.LEASED
    assert queue.get(b.id, tenant_id="t1").status == JobStatus.LEASED
    _ack(queue, first, "w1")
    _ack(queue, second, "w2")


def test_coalesce_noop_when_one_or_none(queue: JobQueue):
    empty = queue.coalesce_jobs("t1", "missing")
    assert empty.jobs_folded == 0
    assert empty.survivor_job_id is None
    only = queue.enqueue(tenant_id="t1", payload={}, coalesce_key="solo")
    again = queue.coalesce_jobs("t1", "solo")
    assert again.jobs_folded == 0
    assert again.survivor_job_id == only.id
    assert queue.get(only.id, tenant_id="t1").status == JobStatus.PENDING


def test_coalesce_requires_key_and_keep_in_set(queue: JobQueue):
    with pytest.raises(ValueError, match="coalesce_key"):
        queue.coalesce_jobs("t1", "")
    job = queue.enqueue(tenant_id="t1", payload={}, coalesce_key="k")
    queue.enqueue(tenant_id="t1", payload={}, coalesce_key="k")
    other = queue.enqueue(tenant_id="t1", payload={}, coalesce_key="other")
    with pytest.raises(CoalesceError, match="not in the coalesce set"):
        queue.coalesce_jobs("t1", "k", keep_job_id=other.id)
    assert queue.get(job.id, tenant_id="t1").status == JobStatus.PENDING


def test_coalesce_http_roundtrip(queue: JobQueue):
    first = queue.enqueue(
        tenant_id="http-c", payload={"n": 1}, coalesce_key="inv"
    )
    extra = queue.enqueue(
        tenant_id="http-c", payload={"n": 2}, coalesce_key="inv"
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        out = client.coalesce_jobs("http-c", "inv")
        assert out["jobs_folded"] == 1
        assert out["survivor_job_id"] == str(first.id)
        assert client.get_job(extra.id, tenant_id="http-c").status == (
            JobStatus.CANCELLED
        )
        still = client.get_job(first.id, tenant_id="http-c")
        assert still is not None and still.status == JobStatus.PENDING
    finally:
        server.shutdown()
