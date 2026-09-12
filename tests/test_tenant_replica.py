"""Live shadow replica: dest catches up while source keeps serving."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from job_queue import JobQueue, JobStatus, ReplicaError
from job_queue.blob_store import FilesystemBlobStore
from tests.helpers_queue import _ack


def _dest_dsn(postgres_url: str) -> str:
    info = conninfo_to_dict(postgres_url)
    dest_db = "job_queue_replica_dest"
    with psycopg.connect(postgres_url, autocommit=True) as conn:
        row = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (dest_db,)
        ).fetchone()
        if row is None:
            conn.execute(f"CREATE DATABASE {dest_db}")
    info["dbname"] = dest_db
    return make_conninfo(**info)


def _wipe(q: JobQueue) -> None:
    with q._pool.connection() as conn:
        conn.execute("TRUNCATE jobs CASCADE")
        conn.execute("TRUNCATE job_payload_blobs")
        conn.execute("TRUNCATE job_dag_batches")
        conn.execute("TRUNCATE job_audit_events")
        conn.execute("TRUNCATE tenant_settings")
        conn.execute("TRUNCATE tenant_fairness")
        conn.execute("TRUNCATE duration_estimates")
        conn.execute("TRUNCATE job_worker_affinity")
        conn.execute("TRUNCATE queue_metric_buckets")
        conn.execute("TRUNCATE queue_metric_snapshots")
        conn.execute("TRUNCATE tenant_usage_buckets")
        conn.execute("TRUNCATE schedule_generations, schedule_blackouts, schedules")
        conn.execute("TRUNCATE webhook_deliveries")
        conn.execute("TRUNCATE webhook_endpoints CASCADE")
        conn.execute("TRUNCATE tenant_payload_keys")
        conn.execute("TRUNCATE tenant_replicas")
        conn.execute("TRUNCATE tenant_relocations")
        conn.execute("TRUNCATE tenant_restores CASCADE")
        conn.execute("TRUNCATE tenant_snapshots CASCADE")
        conn.execute("TRUNCATE inbound_trigger_nonces, inbound_job_callbacks, inbound_triggers")
        conn.commit()


@pytest.fixture
def dest_queue(postgres_url: str, tmp_path: Path):
    dsn = _dest_dsn(postgres_url)
    q = JobQueue.connect(
        dsn, min_size=1, max_size=8, require_worker_registration=False
    )
    q._blob_store = FilesystemBlobStore(str(tmp_path / "replica-dest-blobs"))
    _wipe(q)
    yield q
    q.close()


def _replicate(src: JobQueue, dest: JobQueue, tenant_id: str, **kw):
    return src.replicate_tenant(
        tenant_id,
        dest_dsn=dest.dsn,
        dest_blob_dir=str(dest._blob_store.root),
        **kw,
    )


def test_replica_source_keeps_serving_until_cutover(
    queue: JobQueue, dest_queue: JobQueue
):
    keep_src = queue.enqueue(tenant_id="keep-src", payload={"stay": "src"})
    keep_dst = dest_queue.enqueue(tenant_id="keep-dst", payload={"stay": "dst"})
    first = queue.enqueue(
        tenant_id="shadow", payload={"n": 1}, queue_name="default"
    )
    sched = queue.create_schedule(
        tenant_id="shadow",
        name="tick",
        payload={"from": "sched"},
        interval_seconds=60,
    )

    rel = _replicate(queue, dest_queue, "shadow", batch_size=20)
    assert rel.status == "running"
    assert rel.stage in ("bootstrapping", "catching_up")

    late = queue.enqueue(
        tenant_id="shadow", payload={"n": 2, "after": True}, queue_name="default"
    )
    assert dest_queue.lease(
        tenant_id="shadow", worker_id="too-soon", queue_name="default"
    ) is None

    src_keep = queue.lease(tenant_id="keep-src", worker_id="ks", queue_name="default")
    assert src_keep is not None and src_keep.id == keep_src.id
    _ack(queue, src_keep, "ks")

    done = _replicate(
        queue, dest_queue, "shadow", batch_size=20, run_until_complete=True
    )
    assert done.status == "complete"
    assert done.stage == "complete"

    dest_first = dest_queue.get(first.id, tenant_id="shadow")
    dest_late = dest_queue.get(late.id, tenant_id="shadow")
    assert dest_first is not None and dest_first.payload == {"n": 1}
    assert dest_late is not None and dest_late.payload == {"n": 2, "after": True}
    dest_scheds = dest_queue.list_schedules(tenant_id="shadow")
    assert len(dest_scheds) == 1
    assert dest_scheds[0].id == sched.id

    claimed = dest_queue.lease(
        tenant_id="shadow", worker_id="dest-w", queue_name="default"
    )
    assert claimed is not None
    _ack(dest_queue, claimed, "dest-w", result={"moved": True})

    with pytest.raises(ValueError, match="relocated"):
        queue.enqueue(tenant_id="shadow", payload={"no": 1})
    assert queue.get(late.id, tenant_id="shadow") is None

    assert dest_queue.get(keep_dst.id, tenant_id="keep-dst") is not None
    dst_keep = dest_queue.lease(
        tenant_id="keep-dst", worker_id="kd", queue_name="default"
    )
    assert dst_keep is not None and dst_keep.id == keep_dst.id
    _ack(dest_queue, dst_keep, "kd")


def test_replica_cutover_is_one_live_side(queue: JobQueue, dest_queue: JobQueue):
    queue.enqueue(tenant_id="one-live", payload={"a": 1})
    rel = _replicate(queue, dest_queue, "one-live", batch_size=10)
    for _ in range(40):
        rel = queue.advance_tenant_replica("one-live", batch_size=10)
        if rel.stage == "catching_up":
            break
    assert rel.stage == "catching_up"
    assert dest_queue.lease(tenant_id="one-live", worker_id="early") is None
    extra = queue.enqueue(tenant_id="one-live", payload={"b": 2})
    assert extra is not None

    done = queue.cutover_tenant_replica("one-live", batch_size=20)
    assert done.status == "complete"
    assert dest_queue.get(extra.id, tenant_id="one-live") is not None
    with pytest.raises(ValueError, match="relocated"):
        queue.enqueue(tenant_id="one-live", payload={"c": 3})
    dest_queue.enqueue(tenant_id="one-live", payload={"c": 3})


def test_replica_abort_leaves_source_serving(queue: JobQueue, dest_queue: JobQueue):
    job = queue.enqueue(tenant_id="abort-rep", payload={"x": 1})
    rel = _replicate(queue, dest_queue, "abort-rep", batch_size=5)
    assert rel.status == "running"
    aborted = queue.abort_tenant_replica("abort-rep")
    assert aborted.status == "aborted"
    extra = queue.enqueue(tenant_id="abort-rep", payload={"after": True})
    assert extra is not None
    assert queue.get(job.id, tenant_id="abort-rep") is not None
    assert dest_queue.get(job.id, tenant_id="abort-rep") is None


def test_replica_resumes_after_source_reconnect(
    queue: JobQueue, dest_queue: JobQueue, postgres_url: str
):
    jobs = [
        queue.enqueue(tenant_id="crash-rep", payload={"i": i})
        for i in range(6)
    ]
    first = _replicate(queue, dest_queue, "crash-rep", batch_size=1)
    assert first.status == "running"
    for _ in range(6):
        first = queue.advance_tenant_replica("crash-rep", batch_size=1)
        if first.status == "complete":
            break
    assert first.status == "running"
    queue.close()

    src2 = JobQueue.connect(
        postgres_url, min_size=1, max_size=8, require_worker_registration=False
    )
    try:
        done = src2.replicate_tenant(
            "crash-rep",
            dest_dsn=dest_queue.dsn,
            dest_blob_dir=str(dest_queue._blob_store.root),
            batch_size=8,
            run_until_complete=True,
        )
        assert done.status == "complete"
        for job in jobs:
            got = dest_queue.get(job.id, tenant_id="crash-rep")
            assert got is not None
            assert got.payload == {"i": job.payload["i"]}
        with pytest.raises(ValueError, match="relocated"):
            src2.enqueue(tenant_id="crash-rep", payload={"late": 1})
    finally:
        src2.close()


def test_replica_refuses_occupied_dest_without_pausing_source(
    queue: JobQueue, dest_queue: JobQueue
):
    queue.enqueue(tenant_id="rep-busy", payload={"a": 1})
    dest_queue.enqueue(tenant_id="rep-busy", payload={"already": True})
    with pytest.raises(ReplicaError, match="not empty"):
        _replicate(
            queue, dest_queue, "rep-busy", batch_size=20, run_until_complete=True
        )
    assert queue.is_tenant_paused("rep-busy") is False
    assert queue.get_tenant_replica("rep-busy") is None
    extra = queue.enqueue(tenant_id="rep-busy", payload={"still": "live"})
    assert extra is not None


def test_replica_catchup_picks_up_source_ack(queue: JobQueue, dest_queue: JobQueue):
    job = queue.enqueue(tenant_id="ack-rep", payload={"n": 1}, queue_name="default")
    rel = _replicate(queue, dest_queue, "ack-rep", batch_size=20)
    leased = queue.lease(tenant_id="ack-rep", worker_id="w", queue_name="default")
    assert leased is not None
    _ack(queue, leased, "w", result={"ok": True})
    done = queue.cutover_tenant_replica("ack-rep", batch_size=20)
    assert done.status == "complete"
    dest = dest_queue.get(job.id, tenant_id="ack-rep")
    assert dest is not None
    assert dest.status == JobStatus.COMPLETED
    assert dest.result == {"ok": True}
