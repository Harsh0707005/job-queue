"""Atomic DAG enqueue: all-or-nothing, idempotent retry, no races."""

from __future__ import annotations

import threading
import time
from uuid import uuid4

import pytest
from psycopg.errors import UniqueViolation

from job_queue import CancelError, DependencyError, JobQueue, JobStatus, RateLimitError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack


def _diamond(prefix: str = "n") -> list[dict]:
    return [
        {"ref": f"{prefix}-a", "payload": {"step": "a", "p": prefix}},
        {
            "ref": f"{prefix}-b",
            "payload": {"step": "b", "p": prefix},
            "depends_on": [f"{prefix}-a"],
        },
        {
            "ref": f"{prefix}-c",
            "payload": {"step": "c", "p": prefix},
            "depends_on": [f"{prefix}-a"],
        },
        {
            "ref": f"{prefix}-d",
            "payload": {"step": "d", "p": prefix},
            "depends_on": [f"{prefix}-b", f"{prefix}-c"],
        },
    ]


def test_enqueue_dag_linear_runs_in_order(queue: JobQueue):
    out = queue.enqueue_dag(
        tenant_id="dag-lin",
        batch_key="lin-1",
        jobs=[
            {"ref": "a", "payload": {"n": 1}},
            {"ref": "b", "payload": {"n": 2}, "depends_on": ["a"]},
            {"ref": "c", "payload": {"n": 3}, "depends_on": ["b"]},
        ],
    )
    assert out.created is True
    assert [j.payload["n"] for j in out.jobs] == [1, 2, 3]
    assert out.by_ref["b"].depends_on == (out.by_ref["a"].id,)
    first = queue.lease(tenant_id="dag-lin", worker_id="w")
    assert first is not None and first.payload["n"] == 1
    assert queue.lease(tenant_id="dag-lin", worker_id="w2") is None
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="dag-lin", worker_id="w")
    assert second is not None and second.payload["n"] == 2
    _ack(queue, second, "w")
    third = queue.lease(tenant_id="dag-lin", worker_id="w")
    assert third is not None and third.payload["n"] == 3
    _ack(queue, third, "w")


def test_enqueue_dag_diamond(queue: JobQueue):
    out = queue.enqueue_dag(
        tenant_id="dag-dia", batch_key="dia-1", jobs=_diamond()
    )
    a, b, c, d = out.jobs
    assert set(d.depends_on) == {b.id, c.id}
    la = queue.lease(tenant_id="dag-dia", worker_id="w")
    assert la is not None and la.id == a.id
    _ack(queue, la, "w")
    mid = {queue.lease(tenant_id="dag-dia", worker_id="w1").id for _ in range(2)}
    assert mid == {b.id, c.id}
    for jid, wid in zip(mid, ("w1", "w2"), strict=True):
        job = queue.get(jid, tenant_id="dag-dia")
        assert job is not None and job.lease_token is not None
        queue.ack(
            job.id,
            tenant_id="dag-dia",
            worker_id=job.leased_by,
            lease_token=job.lease_token,
        )
    last = queue.lease(tenant_id="dag-dia", worker_id="w")
    assert last is not None and last.id == d.id


def test_enqueue_dag_cycle_rejected(queue: JobQueue):
    with pytest.raises(DependencyError, match="cycle"):
        queue.enqueue_dag(
            tenant_id="dag-cyc",
            batch_key="cyc",
            jobs=[
                {"ref": "a", "payload": {"n": 1}, "depends_on": ["b"]},
                {"ref": "b", "payload": {"n": 2}, "depends_on": ["a"]},
            ],
        )
    assert queue.list_jobs(tenant_id="dag-cyc") == []


def test_enqueue_dag_cycle_via_explicit_ids_rejected(queue: JobQueue):
    id_a, id_b = uuid4(), uuid4()
    with pytest.raises(DependencyError, match="cycle"):
        queue.enqueue_dag(
            tenant_id="dag-cyc-id",
            batch_key="cyc-id",
            jobs=[
                {
                    "ref": "a",
                    "job_id": str(id_a),
                    "payload": {"n": 1},
                    "depends_on": [str(id_b)],
                },
                {
                    "ref": "b",
                    "job_id": str(id_b),
                    "payload": {"n": 2},
                    "depends_on": [str(id_a)],
                },
            ],
        )
    assert queue.list_jobs(tenant_id="dag-cyc-id") == []


def test_enqueue_dag_duplicate_job_id_rejected(queue: JobQueue):
    shared = uuid4()
    with pytest.raises(ValueError, match="duplicate job_id"):
        queue.enqueue_dag(
            tenant_id="dag-dup-id",
            batch_key="dup-id",
            jobs=[
                {"ref": "a", "job_id": str(shared), "payload": {"n": 1}},
                {"ref": "b", "job_id": str(shared), "payload": {"n": 2}},
            ],
        )
    assert queue.list_jobs(tenant_id="dag-dup-id") == []


def test_enqueue_dag_depends_on_sibling_uuid(queue: JobQueue):
    id_a = uuid4()
    out = queue.enqueue_dag(
        tenant_id="dag-sid",
        batch_key="sid",
        jobs=[
            {"ref": "a", "job_id": id_a, "payload": {"n": 1}},
            {"ref": "b", "payload": {"n": 2}, "depends_on": [str(id_a)]},
        ],
    )
    assert out.by_ref["b"].depends_on == (id_a,)
    first = queue.lease(tenant_id="dag-sid", worker_id="w")
    assert first is not None and first.id == id_a
    assert queue.lease(tenant_id="dag-sid", worker_id="w2") is None
    _ack(queue, first, "w")
    second = queue.lease(tenant_id="dag-sid", worker_id="w")
    assert second is not None and second.payload["n"] == 2


def test_enqueue_dag_merge_renames_colliding_batch_key(queue: JobQueue):
    src = queue.enqueue_dag(
        tenant_id="dag-ms",
        batch_key="same",
        jobs=[{"ref": "a", "payload": {"n": 1}}],
    )
    dst = queue.enqueue_dag(
        tenant_id="dag-md",
        batch_key="same",
        jobs=[{"ref": "a", "payload": {"n": 2}}],
    )
    result = queue.merge_tenant("dag-ms", "dag-md")
    assert result.source_empty is True
    jobs = queue.list_jobs(tenant_id="dag-md")
    assert {j.id for j in jobs} == {src.jobs[0].id, dst.jobs[0].id}
    # Retrying either original key still returns a committed graph.
    again_dst = queue.enqueue_dag(
        tenant_id="dag-md",
        batch_key="same",
        jobs=[{"ref": "a", "payload": {"n": 2}}],
    )
    assert again_dst.created is False
    assert again_dst.jobs[0].id == dst.jobs[0].id


def test_enqueue_dag_unknown_ref_rejected(queue: JobQueue):
    with pytest.raises(DependencyError, match="unknown"):
        queue.enqueue_dag(
            tenant_id="dag-miss",
            batch_key="miss",
            jobs=[
                {"ref": "a", "payload": {"n": 1}, "depends_on": ["nope"]},
            ],
        )
    assert queue.list_jobs(tenant_id="dag-miss") == []


def test_enqueue_dag_atomic_on_pending_cap(queue: JobQueue):
    queue.set_pending_limit("dag-cap", 1)
    queue.enqueue(tenant_id="dag-cap", payload={"keep": True})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue_dag(
            tenant_id="dag-cap",
            batch_key="too-big",
            jobs=_diamond("x"),
        )
    jobs = queue.list_jobs(tenant_id="dag-cap")
    assert len(jobs) == 1
    assert jobs[0].payload.get("keep") is True


def test_enqueue_dag_parent_cancel_during_insert_cascades(queue: JobQueue):
    """Cancel of an external parent overlapping enqueue must not orphan children.

    The enqueue txn locks the parent after validating it. Cancel blocks on
    that row, then cascades once the DAG commits — children must end
    cancelled, never pending forever.
    """
    parent = queue.enqueue(tenant_id="dag-race", payload={"p": 1})
    orig = queue._validate_dependencies
    cancel_thread: threading.Thread | None = None

    def validate_then_cancel(conn, *, tenant_id, depends_on):
        nonlocal cancel_thread
        orig(conn, tenant_id=tenant_id, depends_on=depends_on)
        started = threading.Event()

        def cancel_parent() -> None:
            started.set()
            queue.cancel(parent.id, tenant_id=tenant_id)

        cancel_thread = threading.Thread(target=cancel_parent, name="cancel-parent")
        cancel_thread.start()
        assert started.wait(timeout=5)
        # Cancel must stay blocked on the parent row held by this txn.
        # On the old unlocked SELECT it would have committed already and
        # cascade would miss the children inserted below.
        time.sleep(0.15)
        assert cancel_thread is not None and cancel_thread.is_alive(), (
            "parent cancel finished while enqueue still held the row; "
            "children would have been invisible to cascade"
        )

    queue._validate_dependencies = validate_then_cancel  # type: ignore[method-assign]
    try:
        out = queue.enqueue_dag(
            tenant_id="dag-race",
            batch_key="race-1",
            jobs=[
                {
                    "ref": "c",
                    "payload": {"c": 1},
                    "depends_on": [str(parent.id)],
                },
                {"ref": "d", "payload": {"d": 1}, "depends_on": ["c"]},
            ],
        )
    finally:
        queue._validate_dependencies = orig  # type: ignore[method-assign]

    assert cancel_thread is not None
    cancel_thread.join(timeout=10)
    assert not cancel_thread.is_alive()

    parent_after = queue.get(parent.id, tenant_id="dag-race")
    assert parent_after is not None
    assert parent_after.status == JobStatus.CANCELLED
    for job in out.jobs:
        got = queue.get(job.id, tenant_id="dag-race")
        assert got is not None
        assert got.status == JobStatus.CANCELLED
    assert queue.lease(tenant_id="dag-race", worker_id="w") is None


def test_enqueue_dag_parent_cancel_race_no_orphan(queue: JobQueue):
    """Hammer enqueue_dag vs cancel: never leave a child pending on a dead parent."""
    for i in range(16):
        tid = f"dag-rr-{i}"
        parent = queue.enqueue(tenant_id=tid, payload={"p": i})
        created: list = []
        rejected: list[BaseException] = []

        def enq(parent_id=parent.id, tenant=tid) -> None:
            try:
                created.append(
                    queue.enqueue_dag(
                        tenant_id=tenant,
                        batch_key="k",
                        jobs=[
                            {
                                "ref": "c",
                                "payload": {"c": 1},
                                "depends_on": [str(parent_id)],
                            },
                            {
                                "ref": "d",
                                "payload": {"d": 1},
                                "depends_on": ["c"],
                            },
                        ],
                    )
                )
            except DependencyError as exc:
                rejected.append(exc)

        def cancel(parent_id=parent.id, tenant=tid) -> None:
            try:
                queue.cancel(parent_id, tenant_id=tenant)
            except CancelError:
                pass

        workers = [threading.Thread(target=enq), threading.Thread(target=cancel)]
        if i % 2:
            workers.reverse()
        for t in workers:
            t.start()
        for t in workers:
            t.join(timeout=15)
            assert not t.is_alive()

        parent_after = queue.get(parent.id, tenant_id=tid)
        assert parent_after is not None
        kids = [j for j in queue.list_jobs(tenant_id=tid) if j.id != parent.id]
        terminal = {
            JobStatus.CANCELLED,
            JobStatus.DEAD,
            JobStatus.FAILED,
        }
        if rejected:
            assert created == []
            assert kids == []
        else:
            assert len(created) == 1
            assert len(kids) == 2
        if parent_after.status in terminal:
            assert all(k.status == JobStatus.CANCELLED for k in kids)
        assert all(
            not (
                k.status == JobStatus.PENDING and parent_after.status in terminal
            )
            for k in kids
        )


def test_enqueue_dag_failed_parent_rolls_back(queue: JobQueue):
    dead = queue.enqueue(
        tenant_id="dag-dead", payload={"x": 1}, max_attempts=1
    )
    leased = queue.lease(tenant_id="dag-dead", worker_id="w")
    assert leased is not None
    from tests.helpers_queue import _nack

    _nack(queue, leased, "w", error="boom", requeue=True)
    with pytest.raises(DependencyError, match="already dead"):
        queue.enqueue_dag(
            tenant_id="dag-dead",
            batch_key="child",
            jobs=[
                {"ref": "c", "payload": {"c": 1}, "depends_on": [str(dead.id)]},
                {"ref": "d", "payload": {"d": 1}, "depends_on": ["c"]},
            ],
        )
    leftover = [
        j
        for j in queue.list_jobs(tenant_id="dag-dead")
        if j.id != dead.id
    ]
    assert leftover == []


def test_enqueue_dag_idempotent_retry(queue: JobQueue):
    first = queue.enqueue_dag(
        tenant_id="dag-id", batch_key="same", jobs=_diamond()
    )
    second = queue.enqueue_dag(
        tenant_id="dag-id", batch_key="same", jobs=_diamond()
    )
    assert first.created is True
    assert second.created is False
    assert [j.id for j in first.jobs] == [j.id for j in second.jobs]
    assert len(queue.list_jobs(tenant_id="dag-id")) == 4
    # Crash-retry must not double-bill enqueue meters.
    assert queue.get_tenant_usage("dag-id").enqueues == 4


def test_enqueue_dag_pk_conflict_rolls_back_whole_graph(queue: JobQueue):
    keep = queue.enqueue(tenant_id="dag-pk", payload={"keep": True})
    with pytest.raises(UniqueViolation):
        queue.enqueue_dag(
            tenant_id="dag-pk",
            batch_key="pk-boom",
            jobs=[
                {"ref": "a", "payload": {"n": 1}},
                {
                    "ref": "b",
                    "payload": {"n": 2},
                    "depends_on": ["a"],
                    "job_id": str(keep.id),
                },
                {
                    "ref": "c",
                    "payload": {"n": 3},
                    "depends_on": ["b"],
                },
            ],
        )
    leftover = queue.list_jobs(tenant_id="dag-pk")
    assert len(leftover) == 1
    assert leftover[0].id == keep.id
    assert leftover[0].payload.get("keep") is True
    assert queue.get_tenant_usage("dag-pk").enqueues == 1


def test_enqueue_dag_rate_limit_rolls_back_whole_graph(queue: JobQueue):
    queue.set_enqueue_rate_limit("dag-rate", 1.0, burst=2)
    with pytest.raises(RateLimitError, match="enqueue rate limit"):
        queue.enqueue_dag(
            tenant_id="dag-rate",
            batch_key="rate-boom",
            jobs=_diamond("r"),
        )
    assert queue.list_jobs(tenant_id="dag-rate") == []
    assert queue.get_tenant_usage("dag-rate").enqueues == 0


def test_enqueue_dag_idempotent_mismatch_rejected(queue: JobQueue):
    queue.enqueue_dag(
        tenant_id="dag-mm",
        batch_key="mm",
        jobs=[{"ref": "a", "payload": {"n": 1}}],
    )
    with pytest.raises(ValueError, match="different refs"):
        queue.enqueue_dag(
            tenant_id="dag-mm",
            batch_key="mm",
            jobs=[
                {"ref": "a", "payload": {"n": 1}},
                {"ref": "b", "payload": {"n": 2}, "depends_on": ["a"]},
            ],
        )


def test_enqueue_dag_txn_crash_leaves_nothing(queue: JobQueue):
    try:
        with queue._pool.connection() as conn:
            with conn.transaction():
                queue._lock_dag_batch_key(
                    conn,
                    tenant_id="dag-cr",
                    queue_name="default",
                    batch_key="crash",
                )
                queue._record_usage(
                    conn, tenant_id="dag-cr", now=__import__(
                        "job_queue.queue._common", fromlist=["_utcnow"]
                    )._utcnow(),
                    enqueues=3,
                )
                raise RuntimeError("simulated crash")
    except RuntimeError:
        pass
    assert queue.list_jobs(tenant_id="dag-cr") == []
    assert queue.get_tenant_usage("dag-cr").enqueues == 0
    out = queue.enqueue_dag(
        tenant_id="dag-cr", batch_key="crash", jobs=_diamond("cr")
    )
    assert out.created is True
    assert len(out.jobs) == 4


def test_enqueue_dag_concurrent_same_key(queue: JobQueue):
    results: list = []
    errors: list[BaseException] = []

    def go() -> None:
        try:
            results.append(
                queue.enqueue_dag(
                    tenant_id="dag-cc",
                    batch_key="shared",
                    jobs=_diamond("cc"),
                )
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()
    assert not errors
    assert len(results) == 8
    ids = [tuple(j.id for j in r.jobs) for r in results]
    assert len(set(ids)) == 1
    assert sum(1 for r in results if r.created) == 1
    assert len(queue.list_jobs(tenant_id="dag-cc")) == 4


def test_enqueue_dag_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    from http.server import ThreadingHTTPServer

    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        first = client.enqueue_dag(
            tenant_id="http-dag",
            batch_key="http-1",
            jobs=_diamond("h"),
        )
        assert first.created is True
        again = client.enqueue_dag(
            tenant_id="http-dag",
            batch_key="http-1",
            jobs=_diamond("h"),
        )
        assert again.created is False
        assert [j.id for j in first.jobs] == [j.id for j in again.jobs]
    finally:
        server.shutdown()
