"""Live tenant relocate across two Postgres databases."""

from __future__ import annotations

import base64
import os
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from job_queue import (
    CancelError,
    FreezeError,
    JobQueue,
    JobStatus,
    PostponeError,
    PriorityError,
    RelocateError,
    SpendCapError,
)
from job_queue.blob_store import FilesystemBlobStore
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.crypto import PayloadCrypto
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _ack, _nack


def _dest_dsn(postgres_url: str) -> str:
    info = conninfo_to_dict(postgres_url)
    dest_db = "job_queue_relocate_dest"
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
        conn.execute("TRUNCATE schedule_generations, schedule_blackouts, schedules")
        conn.execute("TRUNCATE webhook_deliveries")
        conn.execute("TRUNCATE webhook_endpoints CASCADE")
        conn.execute("TRUNCATE queue_metric_buckets")
        conn.execute("TRUNCATE queue_metric_snapshots")
        conn.execute("TRUNCATE tenant_usage_buckets")
        conn.execute("TRUNCATE duration_estimates")
        conn.execute("TRUNCATE job_worker_affinity")
        conn.execute("TRUNCATE alert_events CASCADE")
        conn.execute("TRUNCATE alert_rules CASCADE")
        conn.execute("TRUNCATE payload_key_rotations")
        conn.execute("TRUNCATE tenant_payload_keys")
        conn.execute("TRUNCATE workflow_signals CASCADE")
        conn.execute("TRUNCATE workflow_step_runs CASCADE")
        conn.execute("TRUNCATE workflow_runs CASCADE")
        conn.execute("TRUNCATE tenant_relocations")
        conn.execute("TRUNCATE tenant_restores CASCADE")
        conn.execute("TRUNCATE tenant_snapshots CASCADE")
        conn.execute(
            "TRUNCATE inbound_trigger_nonces, inbound_job_callbacks, inbound_triggers"
        )
        conn.commit()


@pytest.fixture
def dest_queue(postgres_url: str, tmp_path: Path):
    dsn = _dest_dsn(postgres_url)
    q = JobQueue.connect(
        dsn, min_size=1, max_size=8, require_worker_registration=False
    )
    q._blob_store = FilesystemBlobStore(str(tmp_path / "dest-blobs"))
    _wipe(q)
    yield q
    q.close()


def _relocate(src: JobQueue, dest: JobQueue, tenant_id: str, **kw):
    return src.relocate_tenant(
        tenant_id,
        dest_dsn=dest.dsn,
        dest_blob_dir=str(dest._blob_store.root),
        **kw,
    )


def test_relocate_moves_jobs_schedule_webhook_history(
    queue: JobQueue, dest_queue: JobQueue
):
    keep_src = queue.enqueue(tenant_id="keep-src", payload={"stay": "src"})
    keep_dst = dest_queue.enqueue(tenant_id="keep-dst", payload={"stay": "dst"})
    pending = queue.enqueue(
        tenant_id="move",
        payload={"secret": "relocate-me", "n": 1},
        queue_name="default",
        labels=["xfer"],
    )
    done = queue.enqueue(
        tenant_id="move",
        payload={"secret": "done", "n": 2},
        queue_name="default",
    )
    leased = queue.lease(tenant_id="move", worker_id="w", queue_name="default")
    assert leased is not None
    finished_id = leased.id
    leftover_id = pending.id if leased.id == done.id else done.id
    _ack(queue, leased, "w", result={"ok": True})
    sched = queue.create_schedule(
        tenant_id="move",
        name="tick",
        payload={"from": "sched", "secret": "sched-secret"},
        interval_seconds=60,
    )
    hook = queue.register_webhook(
        tenant_id="move",
        url="http://127.0.0.1:9/hook",
        events=["job.completed"],
        secret="relocate-webhook-secret",
    )
    events_before = queue.list_tenant_audit("move", limit=500)
    assert any(e.event_type == "enqueued" for e in events_before)
    assert any(e.event_type == "completed" for e in events_before)

    rel = _relocate(queue, dest_queue, "move", batch_size=20, run_until_complete=True)
    assert rel.status == "complete"
    assert rel.stage == "complete"

    dest_pending = dest_queue.get(leftover_id, tenant_id="move")
    assert dest_pending is not None
    dest_done = dest_queue.get(finished_id, tenant_id="move")
    assert dest_done is not None
    assert dest_done.status == JobStatus.COMPLETED
    assert dest_done.result == {"ok": True}

    dest_scheds = dest_queue.list_schedules(tenant_id="move")
    assert len(dest_scheds) == 1
    assert dest_scheds[0].id == sched.id
    assert dest_scheds[0].payload["secret"] == "sched-secret"

    dest_hooks = dest_queue.list_webhooks(tenant_id="move")
    assert len(dest_hooks) == 1
    assert dest_hooks[0].id == hook.id

    dest_events = dest_queue.list_tenant_audit("move", limit=500)
    types = {e.event_type for e in dest_events}
    assert "enqueued" in types
    assert "completed" in types
    assert all(e.tenant_id == "move" for e in dest_events)

    claimed = dest_queue.lease(tenant_id="move", worker_id="dest-w", queue_name="default")
    assert claimed is not None and claimed.id == leftover_id
    _ack(dest_queue, claimed, "dest-w", result={"moved": True})

    assert queue.get(keep_src.id, tenant_id="keep-src") is not None
    src_keep = queue.lease(tenant_id="keep-src", worker_id="ks", queue_name="default")
    assert src_keep is not None and src_keep.id == keep_src.id
    _ack(queue, src_keep, "ks")

    assert dest_queue.get(keep_dst.id, tenant_id="keep-dst") is not None
    dst_keep = dest_queue.lease(tenant_id="keep-dst", worker_id="kd", queue_name="default")
    assert dst_keep is not None and dst_keep.id == keep_dst.id
    _ack(dest_queue, dst_keep, "kd")

    assert queue.get(leftover_id, tenant_id="move") is None
    with pytest.raises(ValueError, match="relocated"):
        queue.enqueue(tenant_id="move", payload={"no": 1})
    with queue._pool.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE tenant_id = %s", ("move",)
        ).fetchone()["n"]
        assert int(n) == 0
        tomb = conn.execute(
            "SELECT relocated, paused FROM tenant_settings WHERE tenant_id = %s",
            ("move",),
        ).fetchone()
        assert tomb is not None and tomb["relocated"] and tomb["paused"]


def test_relocate_refuses_occupied_dest(queue: JobQueue, dest_queue: JobQueue):
    queue.enqueue(tenant_id="move-busy", payload={"a": 1})
    dest_queue.enqueue(tenant_id="move-busy", payload={"already": True})
    with pytest.raises(RelocateError, match="not empty"):
        _relocate(
            queue, dest_queue, "move-busy", batch_size=50, run_until_complete=True
        )


def test_relocate_resumes_after_source_reconnect(
    queue: JobQueue, dest_queue: JobQueue, postgres_url: str
):
    jobs = [
        queue.enqueue(tenant_id="crash-move", payload={"i": i})
        for i in range(8)
    ]
    first = queue.relocate_tenant(
        "crash-move",
        dest_dsn=dest_queue.dsn,
        dest_blob_dir=str(dest_queue._blob_store.root),
        batch_size=1,
        run_until_complete=False,
    )
    assert first.status == "running"
    for _ in range(4):
        first = queue.advance_tenant_relocate("crash-move", batch_size=1)
        if first.status == "complete":
            break
    assert first.status == "running"
    queue.close()

    src2 = JobQueue.connect(
        postgres_url, min_size=1, max_size=8, require_worker_registration=False
    )
    try:
        done = src2.relocate_tenant(
            "crash-move",
            dest_dsn=dest_queue.dsn,
            dest_blob_dir=str(dest_queue._blob_store.root),
            batch_size=3,
            run_until_complete=True,
        )
        assert done.status == "complete"
        for job in jobs:
            got = dest_queue.get(job.id, tenant_id="crash-move")
            assert got is not None
            assert got.payload == {"i": job.payload["i"]}
        with pytest.raises(ValueError, match="relocated"):
            src2.enqueue(tenant_id="crash-move", payload={"late": 1})
    finally:
        src2.close()


def test_relocate_keeps_dest_paused_until_cutover(
    queue: JobQueue, dest_queue: JobQueue
):
    queue.enqueue(tenant_id="half-move", payload={"x": 1})
    rel = _relocate(queue, dest_queue, "half-move", batch_size=1)
    assert rel.status == "running"
    assert rel.stage != "complete"
    with dest_queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT paused, restoring FROM tenant_settings
            WHERE tenant_id = %s
            """,
            ("half-move",),
        ).fetchone()
    if row is not None:
        assert row["paused"] is True
        if rel.stage in ("applying", "verifying"):
            assert row["restoring"] is True
    dest_lease = dest_queue.lease(
        tenant_id="half-move", worker_id="too-soon", queue_name="default"
    )
    assert dest_lease is None
    done = _relocate(
        queue, dest_queue, "half-move", batch_size=50, run_until_complete=True
    )
    assert done.status == "complete"
    claimed = dest_queue.lease(
        tenant_id="half-move", worker_id="after", queue_name="default"
    )
    assert claimed is not None


def test_relocate_moves_inbound_nonces(queue: JobQueue, dest_queue: JobQueue):
    trig = queue.create_inbound_trigger(
        tenant_id="nonce-move",
        name="hook",
        secret="relocate-nonce-secret",
        kinds=["job"],
    )
    queue.fire_inbound_trigger(
        trig.id, {"action": "job", "payload": {"n": 1}}, nonce="once-only"
    )
    with pytest.raises(FileExistsError, match="replayed nonce"):
        queue.fire_inbound_trigger(
            trig.id, {"action": "job", "payload": {"n": 2}}, nonce="once-only"
        )

    rel = _relocate(
        queue, dest_queue, "nonce-move", batch_size=50, run_until_complete=True
    )
    assert rel.status == "complete"
    assert int(rel.counts.get("inbound_trigger_nonces") or 0) >= 1

    with pytest.raises(FileExistsError, match="replayed nonce"):
        dest_queue.fire_inbound_trigger(
            trig.id, {"action": "job", "payload": {"n": 3}}, nonce="once-only"
        )
    dest_queue.fire_inbound_trigger(
        trig.id, {"action": "job", "payload": {"n": 4}}, nonce="fresh-after-move"
    )
    dest_jobs = dest_queue.list_jobs(tenant_id="nonce-move")
    assert len(dest_jobs) == 2
    payloads = sorted(j.payload.get("n") for j in dest_jobs)
    assert payloads == [1, 4]


def test_relocate_preserves_spend_and_breaker_counters(
    queue: JobQueue, dest_queue: JobQueue
):
    tid = "cap-move"
    queue.set_tenant_spend_cap(
        tid, window_seconds=3600, max_enqueues=3, max_runtime_ms=10_000
    )
    queue.enqueue(tenant_id=tid, payload={"i": 1})
    queue.enqueue(tenant_id=tid, payload={"i": 2})
    queue.enqueue(tenant_id=tid, payload={"i": 3})
    src_cap = queue.get_tenant_spend_cap(tid)
    assert src_cap.enqueues == 3
    assert src_cap.enqueue_remaining == 0
    assert src_cap.window_start is not None

    rel = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert rel.status == "complete"

    dest_cap = dest_queue.get_tenant_spend_cap(tid)
    assert dest_cap.max_enqueues == 3
    assert dest_cap.max_runtime_ms == 10_000
    assert dest_cap.window_seconds == 3600
    assert dest_cap.enqueues == 3
    assert dest_cap.runtime_ms == src_cap.runtime_ms
    assert dest_cap.enqueue_remaining == 0
    assert dest_cap.exhausted is True
    assert dest_cap.window_start is not None
    with pytest.raises(SpendCapError, match="enqueue cap"):
        dest_queue.enqueue(tenant_id=tid, payload={"late": True})


def test_relocate_preserves_open_breaker(queue: JobQueue, dest_queue: JobQueue):
    tid = "brk-move"
    queue.set_tenant_breaker(
        tid, failure_threshold=2, window_seconds=60, cooldown_seconds=600
    )
    for i in range(2):
        job = queue.enqueue(
            tenant_id=tid,
            payload={"i": i},
            max_attempts=1,
            retry_backoff_base_seconds=0,
        )
        leased = queue.lease(tenant_id=tid, worker_id=f"w-{i}")
        assert leased is not None
        assert _nack(queue, leased, f"w-{i}", error="boom", requeue=True).status == (
            JobStatus.DEAD
        )
    src_br = queue.get_tenant_breaker(tid)
    assert src_br.open is True
    assert src_br.opened_at is not None

    waiting = queue.enqueue(tenant_id=tid, payload={"wait": True})
    assert queue.lease(tenant_id=tid, worker_id="blocked") is None

    rel = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert rel.status == "complete"
    dest_br = dest_queue.get_tenant_breaker(tid)
    assert dest_br.open is True
    assert dest_br.failures == src_br.failures
    assert dest_br.opened_at is not None
    assert dest_queue.lease(tenant_id=tid, worker_id="dest-blocked") is None
    dest_queue.reset_tenant_breaker(tid)
    claimed = dest_queue.lease(tenant_id=tid, worker_id="after-reset")
    assert claimed is not None and claimed.id == waiting.id


def test_relocate_rewraps_keys_for_dest_kek(queue: JobQueue, dest_queue: JobQueue):
    dest_raw = b"D" * 32
    dest_kek = base64.b64encode(dest_raw).decode()
    job = queue.enqueue(
        tenant_id="kek-move",
        payload={"secret": "keep-me"},
        queue_name="default",
    )
    rel = _relocate(
        queue,
        dest_queue,
        "kek-move",
        dest_kek=dest_kek,
        dest_kek_id="dest",
        batch_size=50,
        run_until_complete=True,
    )
    assert rel.status == "complete"
    dest_queue._crypto = PayloadCrypto({"dest": dest_raw}, "dest")
    dest_queue._crypto.drop_tenant("kek-move")
    got = dest_queue.get(job.id, tenant_id="kek-move")
    assert got is not None
    assert got.payload == {"secret": "keep-me"}
    leased = dest_queue.lease(
        tenant_id="kek-move", worker_id="dest-w", queue_name="default"
    )
    assert leased is not None
    assert leased.payload == {"secret": "keep-me"}
    with dest_queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT kek_id FROM tenant_payload_keys
            WHERE tenant_id = %s
            """,
            ("kek-move",),
        ).fetchone()
    assert row is not None
    assert row["kek_id"] == "dest"


def test_relocate_fails_if_source_dek_cannot_unwrap(
    queue: JobQueue, dest_queue: JobQueue
):
    job = queue.enqueue(
        tenant_id="kek-bad",
        payload={"secret": "stuck"},
        queue_name="default",
    )
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE tenant_payload_keys
            SET kek_id = 'gone'
            WHERE tenant_id = %s
            """,
            ("kek-bad",),
        )
        conn.commit()
    queue._crypto.drop_tenant("kek-bad")
    with pytest.raises(RelocateError, match="unwrap"):
        _relocate(
            queue, dest_queue, "kek-bad", batch_size=50, run_until_complete=True
        )
    assert dest_queue.get(job.id, tenant_id="kek-bad") is None
    with queue._pool.connection() as conn:
        still = conn.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
            (job.id, "kek-bad"),
        ).fetchone()
    assert still is not None


def test_relocate_preserves_source_paused(queue: JobQueue, dest_queue: JobQueue):
    job = queue.enqueue(tenant_id="paused-move", payload={"stay": "paused"})
    queue.pause_tenant("paused-move")
    assert queue.is_tenant_paused("paused-move")

    rel = _relocate(
        queue, dest_queue, "paused-move", batch_size=50, run_until_complete=True
    )
    assert rel.status == "complete"
    assert rel.source_paused is True
    assert dest_queue.is_tenant_paused("paused-move")
    assert dest_queue.lease(
        tenant_id="paused-move", worker_id="too-soon", queue_name="default"
    ) is None
    dest_queue.resume_tenant("paused-move")
    claimed = dest_queue.lease(
        tenant_id="paused-move", worker_id="after", queue_name="default"
    )
    assert claimed is not None and claimed.id == job.id


def _advance_past_export(src: JobQueue, dest: JobQueue, tenant_id: str):
    rel = _relocate(src, dest, tenant_id, batch_size=2)
    for _ in range(80):
        if rel.status == "complete" or rel.stage in (
            "applying",
            "verifying",
            "cutover",
            "purging",
            "complete",
        ):
            return rel
        rel = src.advance_tenant_relocate(tenant_id, batch_size=2)
    raise AssertionError(f"export never finished: {rel}")


def test_relocate_rejects_source_mutations_after_export(
    queue: JobQueue, dest_queue: JobQueue
):
    pending = queue.enqueue(
        tenant_id="mut-move",
        payload={"keep": True},
        queue_name="default",
    )
    other = queue.enqueue(
        tenant_id="mut-keep",
        payload={"other": True},
        queue_name="default",
    )
    rel = _advance_past_export(queue, dest_queue, "mut-move")
    assert rel.stage in ("applying", "verifying", "cutover", "purging", "complete")

    with pytest.raises(CancelError, match="relocating"):
        queue.cancel(pending.id, tenant_id="mut-move")
    with pytest.raises(PriorityError, match="relocating"):
        queue.set_job_priority(pending.id, tenant_id="mut-move", priority=9)
    with pytest.raises(FreezeError, match="relocating"):
        queue.freeze(pending.id, tenant_id="mut-move")
    with pytest.raises(PostponeError, match="relocating"):
        queue.postpone_job(pending.id, tenant_id="mut-move", delay_seconds=30)
    with pytest.raises(ValueError, match="relocating"):
        queue.cancel_matching(tenant_id="mut-move")

    src = queue.get(pending.id, tenant_id="mut-move")
    assert src is not None
    assert src.status == JobStatus.PENDING
    assert src.priority == pending.priority

    # Other tenants can still cancel.
    cancelled = queue.cancel(other.id, tenant_id="mut-keep")
    assert cancelled.status == JobStatus.CANCELLED

    done = _relocate(
        queue, dest_queue, "mut-move", batch_size=50, run_until_complete=True
    )
    assert done.status == "complete"
    dest = dest_queue.get(pending.id, tenant_id="mut-move")
    assert dest is not None
    assert dest.status == JobStatus.PENDING
    claimed = dest_queue.lease(
        tenant_id="mut-move", worker_id="dest-w", queue_name="default"
    )
    assert claimed is not None and claimed.id == pending.id


def test_relocate_skips_ttl_expire_after_export(
    queue: JobQueue, dest_queue: JobQueue
):
    from datetime import timedelta

    from job_queue.queue._common import _utcnow

    job = queue.enqueue(
        tenant_id="ttl-move",
        payload={"ttl": True},
        queue_name="default",
        expires_at=_utcnow() + timedelta(seconds=1),
        expiry_action="cancel",
    )
    rel = _advance_past_export(queue, dest_queue, "ttl-move")
    assert rel.status == "running" or rel.stage != "complete"
    import time

    time.sleep(1.1)
    expired = queue.process_expired_jobs(tenant_id="ttl-move")
    assert expired["cancelled"] == 0
    still = queue.get(job.id, tenant_id="ttl-move")
    assert still is not None and still.status == JobStatus.PENDING

    done = _relocate(
        queue, dest_queue, "ttl-move", batch_size=50, run_until_complete=True
    )
    assert done.status == "complete"
    dest = dest_queue.get(job.id, tenant_id="ttl-move")
    assert dest is not None
    assert dest.status == JobStatus.PENDING


def test_http_relocate_roundtrip(queue: JobQueue, dest_queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{port}")
        enq = client.enqueue(tenant_id="http-move", payload={"k": "v"})
        job_id = enq.id
        rel = client.relocate_tenant(
            "http-move",
            dest_dsn=dest_queue.dsn,
            dest_blob_dir=str(dest_queue._blob_store.root),
            batch_size=50,
            run_until_complete=True,
        )
        assert rel["relocate"]["status"] == "complete"
        got = dest_queue.get(job_id, tenant_id="http-move")
        assert got is not None
        assert got.payload == {"k": "v"}
    finally:
        server.shutdown()
        t.join(timeout=5)
