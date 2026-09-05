"""Live tenant relocate across two Postgres databases."""

from __future__ import annotations

import base64
import os
import threading
import time
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
    LeaseError,
    PostponeError,
    PriorityError,
    RateLimitError,
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


def _activity_steps(*, timeout: float = 30.0, heartbeat: float = 10.0):
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        {
            "name": "charge",
            "kind": "activity",
            "activity": "charge",
            "timeout_seconds": timeout,
            "heartbeat_timeout_seconds": heartbeat,
            "payload": {"op": "charge"},
            "compensate": {"kind": "effect", "payload": {"op": "undo_charge"}},
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def _drive_effect(queue: JobQueue, tenant_id: str):
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})
    return j


def test_relocate_waits_for_running_activity(queue: JobQueue, dest_queue: JobQueue):
    tid = "act-move"
    queue.start_workflow(
        tenant_id=tid, max_attempts=1, steps=_activity_steps()
    )
    _drive_effect(queue, tid)
    job = queue.lease(tenant_id=tid, worker_id="w")
    assert job is not None
    started = queue.start_workflow_activity(
        job.id, tenant_id=tid, worker_id="w", lease_token=job.lease_token
    )
    assert started.status == "running"

    rel = _relocate(queue, dest_queue, tid, batch_size=50)
    for _ in range(6):
        assert rel.status == "running"
        assert rel.stage == "draining"
        hb = queue.heartbeat_workflow_activity(
            job.id, tenant_id=tid, worker_id="w", lease_token=job.lease_token
        )
        assert hb.status == "running"
        with dest_queue._pool.connection() as conn:
            n = conn.execute(
                """
                SELECT COUNT(*) AS n FROM workflow_activities
                WHERE tenant_id = %s
                """,
                (tid,),
            ).fetchone()["n"]
        assert int(n) == 0
        rel = queue.advance_tenant_relocate(tid, batch_size=50)

    done_act = queue.complete_workflow_activity(
        job.id,
        tenant_id=tid,
        worker_id="w",
        lease_token=job.lease_token,
        result={"charged": True},
    )
    assert done_act.status == "completed"
    _ack(queue, job, "w", result=done_act.result)

    fin = _relocate(
        queue, dest_queue, tid, batch_size=50, run_until_complete=True
    )
    assert fin.status == "complete"
    dest_act = dest_queue.get_workflow_activity(job.id, tenant_id=tid)
    assert dest_act is not None
    assert dest_act.status == "completed"
    assert dest_act.result == {"charged": True}
    with dest_queue._pool.connection() as conn:
        running = conn.execute(
            """
            SELECT COUNT(*) AS n FROM workflow_activities
            WHERE tenant_id = %s AND status = 'running'
            """,
            (tid,),
        ).fetchone()["n"]
    assert int(running) == 0


def test_relocate_rejects_new_activity_start(queue: JobQueue, dest_queue: JobQueue):
    tid = "act-start-block"
    queue.start_workflow(
        tenant_id=tid, max_attempts=1, steps=_activity_steps()
    )
    _drive_effect(queue, tid)
    job = queue.lease(tenant_id=tid, worker_id="w")
    assert job is not None
    rel = _relocate(queue, dest_queue, tid, batch_size=50)
    assert rel.status == "running"
    with pytest.raises(LeaseError, match="relocating"):
        queue.start_workflow_activity(
            job.id, tenant_id=tid, worker_id="w", lease_token=job.lease_token
        )
    aborted = queue.abort_tenant_relocate(tid)
    assert aborted.status == "aborted"
    started = queue.start_workflow_activity(
        job.id, tenant_id=tid, worker_id="w", lease_token=job.lease_token
    )
    assert started.status == "running"


def test_relocate_refuses_occupied_dest(queue: JobQueue, dest_queue: JobQueue):
    queue.enqueue(tenant_id="move-busy", payload={"a": 1})
    dest_queue.enqueue(tenant_id="move-busy", payload={"already": True})
    with pytest.raises(RelocateError, match="not empty"):
        _relocate(
            queue, dest_queue, "move-busy", batch_size=50, run_until_complete=True
        )
    assert queue.is_tenant_paused("move-busy") is False
    assert queue.get_tenant_relocate("move-busy") is None
    extra = queue.enqueue(tenant_id="move-busy", payload={"still": "live"})
    assert extra is not None


def test_relocate_refuses_dest_that_only_has_settings(
    queue: JobQueue, dest_queue: JobQueue
):
    queue.enqueue(tenant_id="set-busy", payload={"src": True})
    dest_queue.set_tenant_spend_cap(
        "set-busy", window_seconds=3600, max_enqueues=5
    )
    dest_queue.pause_tenant("set-busy")
    before = dest_queue.get_tenant_spend_cap("set-busy")
    assert before.max_enqueues == 5
    assert dest_queue.is_tenant_paused("set-busy") is True

    with pytest.raises(RelocateError, match="tenant_settings"):
        _relocate(
            queue, dest_queue, "set-busy", batch_size=50, run_until_complete=True
        )

    assert queue.is_tenant_paused("set-busy") is False
    assert dest_queue.is_tenant_paused("set-busy") is True
    after = dest_queue.get_tenant_spend_cap("set-busy")
    assert after.max_enqueues == 5
    assert after.enqueues == 0
    extra = queue.enqueue(tenant_id="set-busy", payload={"still": "src"})
    assert extra is not None


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


def _rate_token_state(q: JobQueue, tenant_id: str):
    with q._pool.connection() as conn:
        return conn.execute(
            """
            SELECT enqueue_rate_per_sec, enqueue_burst,
                   enqueue_tokens, enqueue_tokens_at,
                   lease_rate_per_sec, lease_burst,
                   lease_tokens, lease_tokens_at
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()


def test_relocate_preserves_rate_limit_tokens(queue: JobQueue, dest_queue: JobQueue):
    tid = "rate-move"
    # Refill is slow enough that relocate elapsed time cannot refill a token.
    queue.set_enqueue_rate_limit(tid, 0.0001, burst=2)
    queue.set_lease_rate_limit(tid, 0.0001, burst=1)
    first = queue.enqueue(tenant_id=tid, payload={"i": 1})
    second = queue.enqueue(tenant_id=tid, payload={"i": 2})
    with pytest.raises(RateLimitError, match="enqueue rate limit"):
        queue.enqueue(tenant_id=tid, payload={"i": 3})
    leased = queue.lease(tenant_id=tid, worker_id="w")
    assert leased is not None
    leftover_id = second.id if leased.id == first.id else first.id
    assert queue.lease(tenant_id=tid, worker_id="blocked") is None
    # Drain the inflight lease so relocate can cut over; tokens stay spent.
    _ack(queue, leased, "w", result={"ok": True})
    src = _rate_token_state(queue, tid)
    assert src is not None
    assert src["enqueue_tokens"] is not None and float(src["enqueue_tokens"]) < 1
    assert src["lease_tokens"] is not None and float(src["lease_tokens"]) < 1
    assert src["enqueue_tokens_at"] is not None
    assert src["lease_tokens_at"] is not None

    rel = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert rel.status == "complete"

    dest_rate, dest_burst = dest_queue.get_enqueue_rate_limit(tid)
    assert dest_rate == 0.0001 and dest_burst == 2
    dest_lease_rate, dest_lease_burst = dest_queue.get_lease_rate_limit(tid)
    assert dest_lease_rate == 0.0001 and dest_lease_burst == 1
    dest = _rate_token_state(dest_queue, tid)
    assert dest is not None
    assert dest["enqueue_tokens"] is not None
    assert float(dest["enqueue_tokens"]) < 1
    assert dest["lease_tokens"] is not None
    assert float(dest["lease_tokens"]) < 1
    assert dest["enqueue_tokens_at"] is not None
    assert dest["lease_tokens_at"] is not None
    with pytest.raises(RateLimitError, match="enqueue rate limit"):
        dest_queue.enqueue(tenant_id=tid, payload={"late": True})
    assert dest_queue.lease(tenant_id=tid, worker_id="dest-blocked") is None
    waiting = dest_queue.get(leftover_id, tenant_id=tid)
    assert waiting is not None and waiting.status == JobStatus.PENDING


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
    rel = queue.get_tenant_relocate("kek-bad")
    assert rel is not None
    assert rel.status == "aborted"
    assert queue.is_tenant_paused("kek-bad") is False
    with queue._pool.connection() as conn:
        flags = conn.execute(
            """
            SELECT relocating, relocated FROM tenant_settings
            WHERE tenant_id = %s
            """,
            ("kek-bad",),
        ).fetchone()
    assert flags is None or (
        not flags["relocating"] and not flags["relocated"]
    )
    assert dest_queue.get(job.id, tenant_id="kek-bad") is None
    with queue._pool.connection() as conn:
        still = conn.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
            (job.id, "kek-bad"),
        ).fetchone()
    assert still is not None


def test_abort_relocate_unpauses_source(queue: JobQueue, dest_queue: JobQueue):
    job = queue.enqueue(tenant_id="abort-move", payload={"keep": True})
    rel = _relocate(queue, dest_queue, "abort-move", batch_size=1)
    assert rel.status == "running"
    assert queue.is_tenant_paused("abort-move") is True
    aborted = queue.abort_tenant_relocate("abort-move")
    assert aborted.status == "aborted"
    assert queue.is_tenant_paused("abort-move") is False
    extra = queue.enqueue(tenant_id="abort-move", payload={"after": True})
    assert extra is not None
    src = queue.get(job.id, tenant_id="abort-move")
    assert src is not None
    assert dest_queue.get(job.id, tenant_id="abort-move") is None


def test_abort_relocate_keeps_prior_pause(queue: JobQueue, dest_queue: JobQueue):
    queue.enqueue(tenant_id="abort-paused", payload={"p": 1})
    queue.pause_tenant("abort-paused")
    rel = _relocate(queue, dest_queue, "abort-paused", batch_size=1)
    assert rel.status == "running"
    aborted = queue.abort_tenant_relocate("abort-paused")
    assert aborted.status == "aborted"
    assert queue.is_tenant_paused("abort-paused") is True
    assert queue.lease(tenant_id="abort-paused", worker_id="w") is None


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


def _drive_to_exporting(src: JobQueue, dest: JobQueue, tenant_id: str):
    rel = _relocate(src, dest, tenant_id, batch_size=50)
    for _ in range(8):
        if rel.stage == "exporting":
            return rel
        if rel.status != "running":
            raise AssertionError(f"stopped before export: {rel}")
        rel = src.advance_tenant_relocate(tenant_id, batch_size=50)
    raise AssertionError(f"never reached exporting: {rel}")


def _snapshot_count(q: JobQueue, tenant_id: str) -> int:
    with q._pool.connection() as conn:
        return int(
            conn.execute(
                """
                SELECT COUNT(*) AS n FROM tenant_snapshots
                WHERE source_tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()["n"]
        )


def test_concurrent_advance_exports_once(queue: JobQueue, dest_queue: JobQueue):
    """API advance + cleaner must not both snapshot the same cursor."""
    tid = "race-export"
    queue.enqueue(tenant_id=tid, payload={"i": 1})
    rel = _drive_to_exporting(queue, dest_queue, tid)
    assert rel.snapshot_id is None

    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []
    orig = queue.export_tenant

    def slow_export(tenant_id: str):
        calls.append(tenant_id)
        started.set()
        assert release.wait(10)
        return orig(tenant_id)

    errors: list[BaseException] = []
    results: list = []

    def api_advance() -> None:
        try:
            results.append(queue.advance_tenant_relocate(tid, batch_size=50))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def cleaner_advance() -> None:
        try:
            results.extend(
                queue.process_tenant_relocations(limit=10, batch_size=50)
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    queue.export_tenant = slow_export  # type: ignore[method-assign]
    try:
        t_api = threading.Thread(target=api_advance)
        t_api.start()
        assert started.wait(10)
        t_clean = threading.Thread(target=cleaner_advance)
        t_clean.start()
        # Old bug: cleaner passed the released xact lock and exported again.
        time.sleep(0.3)
        release.set()
        t_api.join(15)
        t_clean.join(15)
        assert not t_api.is_alive() and not t_clean.is_alive()
    finally:
        queue.export_tenant = orig  # type: ignore[method-assign]

    assert errors == []
    assert calls == [tid]
    assert _snapshot_count(queue, tid) == 1
    after = queue.get_tenant_relocate(tid)
    assert after is not None and after.snapshot_id is not None
    done = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert done.status == "complete"
    got = dest_queue.list_jobs(tenant_id=tid)
    assert len(got) == 1


def test_concurrent_api_advances_serialize_export(
    queue: JobQueue, dest_queue: JobQueue
):
    tid = "race-api"
    queue.enqueue(tenant_id=tid, payload={"i": 1})
    rel = _drive_to_exporting(queue, dest_queue, tid)
    assert rel.stage == "exporting"

    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []
    orig = queue.export_tenant

    def slow_export(tenant_id: str):
        calls.append(tenant_id)
        started.set()
        assert release.wait(10)
        return orig(tenant_id)

    errors: list[BaseException] = []
    results: list = []

    def run() -> None:
        try:
            results.append(queue.advance_tenant_relocate(tid, batch_size=50))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    queue.export_tenant = slow_export  # type: ignore[method-assign]
    try:
        t1 = threading.Thread(target=run)
        t1.start()
        assert started.wait(10)
        t2 = threading.Thread(target=run)
        t2.start()
        time.sleep(0.3)
        release.set()
        t1.join(15)
        t2.join(15)
        assert not t1.is_alive() and not t2.is_alive()
    finally:
        queue.export_tenant = orig  # type: ignore[method-assign]

    assert errors == []
    assert calls == [tid]
    assert _snapshot_count(queue, tid) == 1
    assert len(results) == 2
    ids = {r.snapshot_id for r in results}
    assert None not in ids and len(ids) == 1
    done = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert done.status == "complete"


def test_concurrent_apply_holds_stage_lock(
    queue: JobQueue, dest_queue: JobQueue
):
    tid = "race-apply"
    for i in range(4):
        queue.enqueue(tenant_id=tid, payload={"i": i})
    rel = _advance_past_export(queue, dest_queue, tid)
    assert rel.stage in ("applying", "verifying", "cutover", "purging", "complete")
    if rel.status == "complete":
        return

    in_apply = 0
    max_in = 0
    gate = threading.Lock()
    started = threading.Event()
    release = threading.Event()
    orig = queue._relocate_apply

    def slow_apply(row, *, batch_size, now):
        nonlocal in_apply, max_in
        with gate:
            in_apply += 1
            max_in = max(max_in, in_apply)
        started.set()
        release.wait(10)
        try:
            return orig(row, batch_size=batch_size, now=now)
        finally:
            with gate:
                in_apply -= 1

    errors: list[BaseException] = []

    def run() -> None:
        try:
            queue.advance_tenant_relocate(tid, batch_size=2)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    queue._relocate_apply = slow_apply  # type: ignore[method-assign]
    try:
        t1 = threading.Thread(target=run)
        t1.start()
        assert started.wait(10)
        t2 = threading.Thread(target=run)
        t2.start()
        time.sleep(0.3)
        release.set()
        t1.join(15)
        t2.join(15)
        assert not t1.is_alive() and not t2.is_alive()
    finally:
        queue._relocate_apply = orig  # type: ignore[method-assign]

    assert errors == []
    assert max_in == 1
    done = _relocate(queue, dest_queue, tid, batch_size=50, run_until_complete=True)
    assert done.status == "complete"
    assert len(dest_queue.list_jobs(tenant_id=tid)) == 4


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
