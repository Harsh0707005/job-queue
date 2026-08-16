"""Graph-closed dead-letter replay: deps and joins come back together."""

from __future__ import annotations

import threading
from http.server import ThreadingHTTPServer

import pytest

from job_queue import (
    JobQueue,
    JobStatus,
    ReplayError,
    ReplayGraphPlan,
    ReplayGraphResult,
)
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _nack


def test_plan_and_replay_parent_dead_brings_cancelled_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1}, max_attempts=1)
    child_a = queue.enqueue(
        tenant_id="t1", payload={"c": "a"}, depends_on=parent.id, max_attempts=1
    )
    child_b = queue.enqueue(
        tenant_id="t1", payload={"c": "b"}, depends_on=parent.id, max_attempts=1
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == parent.id
    dead_parent = _nack(queue, leased, "w", error="boom", requeue=True)
    assert dead_parent.status == JobStatus.DEAD
    assert queue.get(child_a.id, tenant_id="t1").status == JobStatus.CANCELLED
    assert queue.get(child_b.id, tenant_id="t1").status == JobStatus.CANCELLED

    plan = queue.plan_replay_dead_letter_graph("t1", job_ids=[parent.id])
    assert isinstance(plan, ReplayGraphPlan)
    assert plan.ok is True
    assert set(plan.replay_job_ids) == {parent.id, child_a.id, child_b.id}

    out = queue.replay_dead_letter_graph("t1", job_ids=[parent.id])
    assert isinstance(out, ReplayGraphResult)
    assert out.jobs_replayed == 3
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.get(child_a.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.get(child_b.id, tenant_id="t1").depends_on == (parent.id,)

    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None and first.id == parent.id
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="t1", worker_id="w")
    third = queue.lease(tenant_id="t1", worker_id="w2")
    got = {j.id for j in (second, third) if j is not None}
    assert got == {child_a.id, child_b.id}


def test_replay_from_child_pulls_dead_parent(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1}, max_attempts=1)
    child = queue.enqueue(
        tenant_id="t1", payload={"c": 1}, depends_on=parent.id, max_attempts=1
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == parent.id
    _nack(queue, leased, "w", error="p-fail", requeue=True)
    assert queue.get(child.id, tenant_id="t1").status == JobStatus.CANCELLED

    out = queue.replay_dead_letter_graph("t1", job_ids=[child.id])
    assert set(out.job_ids) == {parent.id, child.id}
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.PENDING


def test_replay_child_only_when_parent_already_completed(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    child = queue.enqueue(
        tenant_id="t1", payload={"c": 1}, depends_on=parent.id, max_attempts=1
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == parent.id
    _ack(queue, leased, "w")
    kid = queue.lease(tenant_id="t1", worker_id="w")
    assert kid is not None and kid.id == child.id
    dead = _nack(queue, kid, "w", error="c-fail", requeue=True)
    assert dead.status == JobStatus.DEAD

    plan = queue.plan_replay_dead_letter_graph("t1", job_ids=[child.id])
    assert plan.ok is True
    assert plan.replay_job_ids == (child.id,)
    assert parent.id in plan.stay_job_ids
    out = queue.replay_dead_letter_graph("t1", job_ids=[child.id])
    assert out.job_ids == (child.id,)
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.COMPLETED


def test_replay_spawn_join_restores_parent_awaiting_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"role": "p"})
    held = queue.lease(tenant_id="t1", worker_id="w")
    assert held is not None
    spawned = queue.spawn_children(
        held.id,
        tenant_id="t1",
        worker_id="w",
        lease_token=held.lease_token,
        children=[
            {"payload": {"c": 1}, "max_attempts": 1},
            {"payload": {"c": 2}, "max_attempts": 1},
        ],
    )
    assert spawned.parent.status == JobStatus.AWAITING_CHILDREN
    parked = queue.get(parent.id, tenant_id="t1")
    assert parked is not None and parked.status == JobStatus.AWAITING_CHILDREN
    kids = list(parked.waiting_on)
    assert len(kids) == 2
    first = queue.lease(tenant_id="t1", worker_id="w")
    assert first is not None and first.id in kids
    _nack(queue, first, "w", error="kid-dead", requeue=True)
    # First child dead → parent join-fails and sibling is cancelled.
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.CANCELLED

    out = queue.replay_dead_letter_graph("t1", job_ids=[parent.id])
    assert parent.id in out.rejoin_job_ids
    assert set(out.job_ids) >= {parent.id, *kids}
    again = queue.get(parent.id, tenant_id="t1")
    assert again is not None
    assert again.status == JobStatus.AWAITING_CHILDREN
    assert set(again.waiting_on) == set(kids)
    for kid in kids:
        assert queue.get(kid, tenant_id="t1").status == JobStatus.PENDING

    for kid in kids:
        leased = queue.lease(tenant_id="t1", worker_id="w")
        assert leased is not None and leased.id in kids
        _ack(queue, leased, "w")
    joined = queue.get(parent.id, tenant_id="t1")
    assert joined is not None and joined.status == JobStatus.PENDING
    last = queue.lease(tenant_id="t1", worker_id="w")
    assert last is not None and last.id == parent.id
    _ack(queue, last, "w")


def test_replay_graph_dedupe_conflict_moves_nothing(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="t1", payload={"p": 1}, max_attempts=1, dedupe_key="p-key"
    )
    child = queue.enqueue(
        tenant_id="t1",
        payload={"c": 1},
        depends_on=parent.id,
        max_attempts=1,
        dedupe_key="c-key",
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None and leased.id == parent.id
    _nack(queue, leased, "w", error="boom", requeue=True)
    live = queue.enqueue(tenant_id="t1", payload={"x": 1}, dedupe_key="p-key")
    with pytest.raises(ReplayError, match="dedupe_key"):
        queue.replay_dead_letter_graph("t1", job_ids=[parent.id])
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.DEAD
    assert queue.get(child.id, tenant_id="t1").status == JobStatus.CANCELLED
    assert queue.get(live.id, tenant_id="t1").status == JobStatus.PENDING


def test_replay_graph_frozen_parent_fails_closed(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    child = queue.enqueue(
        tenant_id="t1", payload={"c": 1}, depends_on=parent.id, max_attempts=1
    )
    queue.freeze(parent.id, tenant_id="t1")
    # Child cannot lease; freeze then dead-letter the child directly.
    queue.freeze(child.id, tenant_id="t1")
    queue.dead_letter_frozen(child.id, tenant_id="t1")
    plan = queue.plan_replay_dead_letter_graph("t1", job_ids=[child.id])
    assert plan.ok is False
    assert any(b.reason == "frozen_neighbor" for b in plan.blockers)
    with pytest.raises(ReplayError, match="not closed"):
        queue.replay_dead_letter_graph("t1", job_ids=[child.id])
    assert queue.get(child.id, tenant_id="t1").status == JobStatus.DEAD


def test_replay_graph_idempotent_and_concurrent(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1}, max_attempts=1)
    child = queue.enqueue(
        tenant_id="t1", payload={"c": 1}, depends_on=parent.id, max_attempts=1
    )
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="x", requeue=True)

    results: list[int] = []
    errors: list[BaseException] = []

    def go() -> None:
        try:
            out = queue.replay_dead_letter_graph("t1", job_ids=[parent.id])
            results.append(out.jobs_replayed)
        except ReplayError:
            results.append(0)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive()
    assert errors == []
    assert sum(results) == 2
    assert queue.get(parent.id, tenant_id="t1").status == JobStatus.PENDING
    assert queue.get(child.id, tenant_id="t1").status == JobStatus.PENDING


def test_replay_graph_other_tenant_keeps_working(queue: JobQueue, postgres_url: str):
    other = JobQueue.connect(
        postgres_url, min_size=2, max_size=8, require_worker_registration=False
    )
    try:
        live = [
            other.enqueue(tenant_id="live", payload={"n": i}) for i in range(6)
        ]
        parent = queue.enqueue(tenant_id="dead", payload={"p": 1}, max_attempts=1)
        child = queue.enqueue(
            tenant_id="dead",
            payload={"c": 1},
            depends_on=parent.id,
            max_attempts=1,
        )
        leased = queue.lease(tenant_id="dead", worker_id="w")
        assert leased is not None
        _nack(queue, leased, "w", error="x", requeue=True)

        done: list = []

        def worker() -> None:
            while True:
                job = other.lease(tenant_id="live", worker_id="lw")
                if job is None:
                    if len(done) >= 6:
                        return
                    continue
                _ack(other, job, "lw")
                done.append(job.id)

        t = threading.Thread(target=worker)
        t.start()
        out = queue.replay_dead_letter_graph("dead", job_ids=[parent.id])
        assert set(out.job_ids) == {parent.id, child.id}
        t.join(timeout=10)
        assert set(done) == {j.id for j in live}
        assert queue.get(parent.id, tenant_id="dead").status == JobStatus.PENDING
    finally:
        other.close()


def test_replay_graph_http(queue: JobQueue):
    parent = queue.enqueue(tenant_id="http-d", payload={"p": 1}, max_attempts=1)
    child = queue.enqueue(
        tenant_id="http-d",
        payload={"c": 1},
        depends_on=parent.id,
        max_attempts=1,
    )
    leased = queue.lease(tenant_id="http-d", worker_id="w")
    assert leased is not None
    _nack(queue, leased, "w", error="x", requeue=True)
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    client = CoordinatorClient(f"http://{host}:{port}")
    try:
        plan = client.plan_replay_dead_letter_graph(
            tenant_id="http-d", job_ids=[parent.id]
        )
        assert plan["ok"] is True
        assert set(plan["replay_job_ids"]) == {str(parent.id), str(child.id)}
        out = client.replay_dead_letter_graph(
            tenant_id="http-d", job_ids=[parent.id]
        )
        assert out["jobs_replayed"] == 2
        assert client.get_job(parent.id, tenant_id="http-d").status == (
            JobStatus.PENDING
        )
        assert client.get_job(child.id, tenant_id="http-d").status == (
            JobStatus.PENDING
        )
    finally:
        server.shutdown()
