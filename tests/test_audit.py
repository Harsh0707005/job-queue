"""Immutable job audit stream."""

from __future__ import annotations

from job_queue import JobQueue, JobStatus

# ---------------------------------------------------------------------------
# Immutable job audit stream
# ---------------------------------------------------------------------------


def test_audit_enqueued_leased_completed(queue: JobQueue):
    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert len(events) == 1
    assert events[0].event_type == "enqueued"
    assert events[0].seq == 1
    assert events[0].to_status == "pending"

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued", "leased"]
    assert events[-1].to_status == "leased"
    assert events[-1].seq == 2

    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    events = queue.list_job_audit(job.id, tenant_id="t1")
    assert [e.event_type for e in events] == ["enqueued", "leased", "completed"]
    assert events[-1].to_status == "completed"
    report = queue.verify_job_audit(job.id, tenant_id="t1")
    assert report.ok is True
    assert report.event_count == 3
    assert report.job_status == "completed"
    assert report.audit_final_status == "completed"


def test_audit_matches_after_nack_cancel_checkpoint(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    queue.nack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        error="boom",
        requeue=True,
    )
    types = [e.event_type for e in queue.list_job_audit(job.id, tenant_id="t1")]
    assert types == ["enqueued", "leased", "checkpointed", "requeued"]
    assert queue.verify_job_audit(job.id, tenant_id="t1").ok

    leased2 = queue.lease(tenant_id="t1", worker_id="w2")
    assert leased2 is not None
    queue.cancel(leased2.id, tenant_id="t1")
    assert queue.verify_job_audit(job.id, tenant_id="t1").ok
    assert queue.get(job.id, tenant_id="t1").status == JobStatus.CANCELLED
    final = queue.list_job_audit(job.id, tenant_id="t1")[-1]
    assert final.event_type == "cancelled"
    assert final.to_status == "cancelled"


def test_audit_tenant_scoped_and_after_seq(queue: JobQueue):
    j = queue.enqueue(tenant_id="alpha", payload={})
    queue.enqueue(tenant_id="beta", payload={})
    events = queue.list_job_audit(j.id, tenant_id="alpha")
    assert len(events) == 1
    assert queue.list_job_audit(j.id, tenant_id="beta") == []
    more = queue.list_job_audit(j.id, tenant_id="alpha", after_seq=1)
    assert more == []
    stream = queue.list_tenant_audit("alpha")
    assert any(e.job_id == j.id for e in stream)


def test_verify_tenant_audit_all_ok(queue: JobQueue):
    ids = []
    for i in range(5):
        j = queue.enqueue(tenant_id="t1", payload={"i": i})
        ids.append(j.id)
        leased = queue.lease(tenant_id="t1", worker_id="w1")
        assert leased is not None
        queue.ack(
            leased.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=leased.lease_token,
        )
    reports = queue.verify_tenant_audit("t1")
    assert len(reports) == 5
    assert all(r.ok for r in reports)


def test_audit_history_survives_tenant_merge(queue: JobQueue):
    """After merge, target owner sees full seq including pre-merge events."""
    job = queue.enqueue(tenant_id="alpha", payload={"m": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    pre = queue.list_job_audit(job.id, tenant_id="alpha")
    assert [e.event_type for e in pre] == ["enqueued", "leased", "checkpointed"]
    assert all(e.tenant_id == "alpha" for e in pre)
    assert [e.seq for e in pre] == [1, 2, 3]

    queue.merge_tenant("alpha", "beta")
    assert queue.get(job.id, tenant_id="alpha") is None
    moved = queue.get(job.id, tenant_id="beta")
    assert moved is not None
    assert moved.status == JobStatus.LEASED

    # Target sees full history; pre-merge rows still stamped alpha.
    full = queue.list_job_audit(job.id, tenant_id="beta")
    types = [e.event_type for e in full]
    assert types[:3] == ["enqueued", "leased", "checkpointed"]
    assert "tenant_merged" in types
    assert [e.seq for e in full] == list(range(1, len(full) + 1))
    assert full[0].tenant_id == "alpha"
    merge_ev = next(e for e in full if e.event_type == "tenant_merged")
    assert merge_ev.tenant_id == "beta"
    assert merge_ev.detail.get("from_tenant") == "alpha"
    assert merge_ev.detail.get("to_tenant") == "beta"

    # Source no longer owns the job → no access via list_job_audit.
    assert queue.list_job_audit(job.id, tenant_id="alpha") == []

    # Unrelated tenant blocked.
    assert queue.list_job_audit(job.id, tenant_id="gamma") == []

    report = queue.verify_job_audit(job.id, tenant_id="beta")
    assert report.ok is True
    assert report.event_count >= 4
    assert report.job_status == "leased"
    assert report.audit_final_status == "leased"

    # Tenant-wide catch-up for beta includes pre-merge events for owned jobs.
    stream = queue.list_tenant_audit("beta", limit=100)
    job_events = [e for e in stream if e.job_id == job.id]
    assert len(job_events) >= 4
    assert any(e.tenant_id == "alpha" for e in job_events)

    # Finish under beta; history remains contiguous.
    queue.ack(
        moved.id,
        tenant_id="beta",
        worker_id="w1",
        lease_token=moved.lease_token,
        result={"ok": True},
    )
    final = queue.list_job_audit(job.id, tenant_id="beta")
    assert final[-1].event_type == "completed"
    assert final[-1].seq == len(final)
    assert queue.verify_job_audit(job.id, tenant_id="beta").ok
