"""Rebuild job from audit after row loss."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus

# ---------------------------------------------------------------------------
# Rebuild job from audit after row loss
# ---------------------------------------------------------------------------


def test_rebuild_job_from_audit_after_delete(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"k": "v", "n": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 2, "total": 5},
    )
    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    jid = job.id
    # Simulate partial write / bad migrate: drop job row, keep audit.
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    assert queue.get(jid, tenant_id="t1") is None
    orphans = queue.list_orphaned_audit_jobs(tenant_id="t1")
    assert jid in orphans

    rebuilt = queue.rebuild_job_from_audit(jid, tenant_id="t1")
    assert rebuilt.id == jid
    assert rebuilt.tenant_id == "t1"
    assert rebuilt.status == JobStatus.COMPLETED
    assert rebuilt.payload == {"k": "v", "n": 1}
    assert rebuilt.checkpoint == {"step": 2, "total": 5}
    assert rebuilt.result == {"ok": True}
    assert queue.verify_job_audit(jid, tenant_id="t1").ok

    # Refuse overwrite if row exists.
    with pytest.raises(RebuildError, match="already exists"):
        queue.rebuild_job_from_audit(jid, tenant_id="t1")


def test_rebuild_after_merge_uses_target_tenant(queue: JobQueue):
    job = queue.enqueue(tenant_id="alpha", payload={"x": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    queue.checkpoint(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        progress={"step": 1},
    )
    queue.merge_tenant("alpha", "beta")
    jid = job.id
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()

    with pytest.raises(Exception) as ei:
        queue.rebuild_job_from_audit(jid, tenant_id="alpha")
    assert "rebuild owner" in str(ei.value).lower() or "not" in str(ei.value).lower()

    rebuilt = queue.rebuild_job_from_audit(jid, tenant_id="beta")
    assert rebuilt.tenant_id == "beta"
    assert rebuilt.checkpoint == {"step": 1}
    assert rebuilt.status == JobStatus.LEASED
    events = queue.list_job_audit(jid, tenant_id="beta")
    assert events[0].tenant_id == "alpha"
    assert any(e.event_type == "tenant_merged" for e in events)
    assert events[-1].event_type == "rebuilt"
    assert queue.verify_job_audit(jid, tenant_id="beta").ok


def test_rebuild_rejects_corrupt_audit(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    jid = job.id
    with queue._pool.connection() as conn:
        # Corrupt: wipe snapshot from last event.
        conn.execute(
            """
            UPDATE job_audit_events
            SET detail = '{}'::jsonb
            WHERE job_id = %s AND seq = 1
            """,
            (jid,),
        )
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    with pytest.raises(RebuildError, match="missing detail.job"):
        queue.rebuild_job_from_audit(jid)


def test_rebuild_rejects_seq_gap(queue: JobQueue):
    from job_queue import RebuildError

    job = queue.enqueue(tenant_id="t1", payload={"a": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    queue.ack(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    jid = job.id
    with queue._pool.connection() as conn:
        conn.execute(
            "DELETE FROM job_audit_events WHERE job_id = %s AND seq = 2",
            (jid,),
        )
        conn.execute("DELETE FROM jobs WHERE id = %s", (jid,))
        conn.commit()
    with pytest.raises(RebuildError, match="seq gap"):
        queue.rebuild_job_from_audit(jid)
