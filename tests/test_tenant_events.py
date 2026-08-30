"""Durable per-tenant sealed job event history + cursor follow."""

from __future__ import annotations

from job_queue import JobQueue, JobStatus
from job_queue.crypto import is_envelope
from job_queue.db import transaction

from tests.helpers_queue import _ack, _nack


def _stored_detail(queue: JobQueue, event_id) -> dict:
    with queue._pool.connection() as conn:
        row = conn.execute(
            "SELECT detail FROM job_audit_events WHERE event_id = %s",
            (event_id,),
        ).fetchone()
    assert row is not None
    return row["detail"]


def test_events_cover_enqueue_lease_ack_and_cursor(queue: JobQueue):
    job = queue.enqueue(tenant_id="ev-a", payload={"secret": "alpha-body"})
    leased = queue.lease(tenant_id="ev-a", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w", result={"out": "done"})

    first = queue.list_tenant_events("ev-a", after_seq=0, limit=2)
    assert [e.event_type for e in first] == ["enqueued", "leased"]
    assert [e.tenant_seq for e in first] == [1, 2]
    rest = queue.list_tenant_events("ev-a", after_seq=first[-1].tenant_seq)
    assert [e.event_type for e in rest] == ["completed"]
    assert rest[0].tenant_seq == 3
    assert rest[0].detail.get("job", {}).get("payload")
    stored = _stored_detail(queue, first[0].event_id)
    assert "alpha-body" not in str(stored)
    assert is_envelope(stored)
    assert queue.list_tenant_events("ev-a", after_seq=3) == []


def test_events_do_not_leak_across_tenants(queue: JobQueue):
    a = queue.enqueue(tenant_id="ev-iso-a", payload={"t": "a"})
    b = queue.enqueue(tenant_id="ev-iso-b", payload={"t": "b"})
    ev_a = queue.list_tenant_events("ev-iso-a")
    ev_b = queue.list_tenant_events("ev-iso-b")
    assert {e.job_id for e in ev_a} == {a.id}
    assert {e.job_id for e in ev_b} == {b.id}
    assert all(e.tenant_id == "ev-iso-a" for e in ev_a)
    assert all(e.tenant_id == "ev-iso-b" for e in ev_b)
    assert queue.list_job_audit(a.id, tenant_id="ev-iso-b") == []


def test_events_nack_cancel_checkpoint_and_replay(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="ev-flow",
        payload={"x": 1},
        max_attempts=1,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="ev-flow", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="ev-flow",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    _nack(queue, leased, "w1", error="boom", requeue=True)
    types = [e.event_type for e in queue.list_tenant_events("ev-flow")]
    assert types == ["enqueued", "leased", "checkpointed", "dead_lettered"]
    seqs = [e.tenant_seq for e in queue.list_tenant_events("ev-flow")]
    assert seqs == list(range(1, len(seqs) + 1))
    dead = queue.get(job.id, tenant_id="ev-flow")
    assert dead is not None
    if dead.status == JobStatus.DEAD:
        queue.replay_dead_letter(job.id, tenant_id="ev-flow")
        more = queue.list_tenant_events("ev-flow")
        assert more[-1].event_type == "replayed"
        assert more[-1].tenant_seq == len(more)


def test_event_append_rolls_back_with_job(queue: JobQueue):
    job = queue.enqueue(tenant_id="ev-crash", payload={"n": 1})
    before = queue.list_tenant_events("ev-crash")
    assert len(before) == 1
    try:
        with transaction(queue._pool) as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE id = %s FOR UPDATE",
                (job.id,),
            ).fetchone()
            queue._append_job_audit(
                conn,
                job_row=dict(row),
                event_type="leased",
                from_status="pending",
                actor="w",
            )
            raise RuntimeError("boom mid-event")
    except RuntimeError:
        pass
    after = queue.list_tenant_events("ev-crash")
    assert [e.event_type for e in after] == ["enqueued"]
    assert after[0].tenant_seq == 1


def test_offboard_wipes_tenant_events(queue: JobQueue):
    queue.enqueue(tenant_id="ev-gone", payload={"n": 1})
    assert queue.list_tenant_events("ev-gone")
    queue.offboard_tenant("ev-gone")
    queue.process_offboard_finalizes(limit=5)
    assert queue.list_tenant_events("ev-gone") == []
    assert queue.tenant_is_empty("ev-gone")


def test_export_restore_keeps_sealed_events(queue: JobQueue):
    job = queue.enqueue(tenant_id="ev-src", payload={"secret": "hist"})
    leased = queue.lease(tenant_id="ev-src", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w", result={"ok": True})
    snap = queue.export_tenant("ev-src")
    rst = queue.restore_tenant(
        "ev-dst", snapshot_id=snap.id, run_until_complete=True
    )
    assert rst.status == "complete"
    dest = queue.list_tenant_events("ev-dst")
    assert [e.event_type for e in dest] == ["enqueued", "leased", "completed"]
    assert dest[0].tenant_seq == 1
    assert dest[-1].detail.get("job", {}).get("status") == "completed"
