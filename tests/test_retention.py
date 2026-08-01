"""Terminal job retention + cleaner."""

from __future__ import annotations

import time

from job_queue import JobQueue, JobStatus

from tests.helpers_queue import (
    _ack,
    _nack,
)

# ---------------------------------------------------------------------------
# Terminal job retention + cleaner
# ---------------------------------------------------------------------------


def test_retention_default_keeps_forever(queue: JobQueue):
    settings = queue.get_job_retention()
    assert settings.completed_retention_seconds is None
    assert settings.dead_retention_seconds is None
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")
    purged = queue.purge_expired_jobs(limit=100)
    assert purged.total_deleted == 0
    assert queue.get(job.id, tenant_id="t1") is not None


def test_purge_completed_after_retention(queue: JobQueue):
    queue.set_job_retention(completed_seconds=1, dead_seconds=None)
    job = queue.enqueue(tenant_id="t1", payload={"x": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1", result={"ok": True})
    # Still within window.
    assert queue.purge_expired_jobs().completed_deleted == 0
    assert queue.get(job.id, tenant_id="t1") is not None
    time.sleep(1.2)
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 1
    assert result.dead_deleted == 0
    assert queue.get(job.id, tenant_id="t1") is None


def test_purge_dead_after_retention(queue: JobQueue):
    queue.set_job_retention(completed_seconds=None, dead_seconds=1)
    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        max_attempts=1,
    )
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    dead = _nack(queue, leased, "w1", error="boom", requeue=True)
    assert dead.status == JobStatus.DEAD
    assert dead.completed_at is not None
    time.sleep(1.2)
    result = queue.purge_expired_jobs()
    assert result.dead_deleted == 1
    assert queue.get(job.id, tenant_id="t1") is None


def test_tenant_retention_override(queue: JobQueue):
    queue.set_job_retention(completed_seconds=3600, dead_seconds=3600)
    queue.set_tenant_job_retention("fast", completed_seconds=1, dead_seconds=1)
    # Other tenant uses global (long) retention.
    a = queue.enqueue(tenant_id="fast", payload={})
    b = queue.enqueue(tenant_id="slow", payload={})
    for tid, jid in (("fast", a.id), ("slow", b.id)):
        leased = queue.lease(tenant_id=tid, worker_id="w1")
        assert leased is not None and leased.id == jid
        _ack(queue, leased, "w1")
    time.sleep(1.2)
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 1
    assert queue.get(a.id, tenant_id="fast") is None
    assert queue.get(b.id, tenant_id="slow") is not None


def test_tenant_retention_none_means_forever_not_global(queue: JobQueue):
    """Bugfix: tenant None must not COALESCE into short global retention."""
    queue.set_job_retention(completed_seconds=1, dead_seconds=1)
    # Explicit forever for this tenant — ignore aggressive global.
    got = queue.set_tenant_job_retention(
        "keep", completed_seconds=None, dead_seconds=None
    )
    assert got.completed_inherits is False
    assert got.dead_inherits is False
    assert got.completed_retention_seconds is None
    assert got.dead_retention_seconds is None
    read = queue.get_tenant_job_retention("keep")
    assert read.completed_inherits is False
    assert read.dead_inherits is False

    job = queue.enqueue(tenant_id="keep", payload={"x": 1})
    leased = queue.lease(tenant_id="keep", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")
    time.sleep(1.3)
    # Global would have purged; tenant forever must retain.
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 0
    assert result.dead_deleted == 0
    assert queue.get(job.id, tenant_id="keep") is not None


def test_tenant_retention_none_dead_forever_not_global(queue: JobQueue):
    queue.set_job_retention(completed_seconds=1, dead_seconds=1)
    queue.set_tenant_job_retention("keep-dead", completed_seconds=1, dead_seconds=None)
    ov = queue.get_tenant_job_retention("keep-dead")
    assert ov.completed_inherits is False
    assert ov.completed_retention_seconds == 1
    assert ov.dead_inherits is False
    assert ov.dead_retention_seconds is None  # forever

    job = queue.enqueue(tenant_id="keep-dead", payload={}, max_attempts=1)
    leased = queue.lease(tenant_id="keep-dead", worker_id="w1")
    assert leased is not None
    dead = _nack(queue, leased, "w1", error="boom", requeue=True)
    assert dead.status == JobStatus.DEAD
    time.sleep(1.3)
    result = queue.purge_expired_jobs()
    # completed window is short but this job is dead with forever override.
    assert result.dead_deleted == 0
    assert queue.get(job.id, tenant_id="keep-dead") is not None


def test_tenant_retention_clear_inherits_global(queue: JobQueue):
    queue.set_job_retention(completed_seconds=1, dead_seconds=1)
    queue.set_tenant_job_retention("t1", completed_seconds=None, dead_seconds=None)
    queue.set_tenant_job_retention("t1", clear=True)
    cleared = queue.get_tenant_job_retention("t1")
    assert cleared.completed_inherits is True
    assert cleared.dead_inherits is True

    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")
    time.sleep(1.3)
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 1
    assert queue.get(job.id, tenant_id="t1") is None


def test_purge_completed_and_dead_independently(queue: JobQueue):
    queue.set_job_retention(completed_seconds=1, dead_seconds=3600)
    c = queue.enqueue(tenant_id="t1", payload={"k": "c"})
    d = queue.enqueue(tenant_id="t1", payload={"k": "d"}, max_attempts=1)
    lc = queue.lease(tenant_id="t1", worker_id="w1")
    assert lc is not None and lc.id == c.id
    _ack(queue, lc, "w1")
    ld = queue.lease(tenant_id="t1", worker_id="w1")
    assert ld is not None and ld.id == d.id
    _nack(queue, ld, "w1", error="x", requeue=True)
    time.sleep(1.3)
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 1
    assert result.dead_deleted == 0
    assert queue.get(c.id, tenant_id="t1") is None
    assert queue.get(d.id, tenant_id="t1") is not None


def test_purge_deletes_side_effects(queue: JobQueue):
    queue.set_job_retention(completed_seconds=1, dead_seconds=1)
    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.complete_side_effect(
        leased.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        effect_key="charge",
        result={"ok": True},
    )
    _ack(queue, leased, "w1")
    time.sleep(1.2)
    result = queue.purge_expired_jobs()
    assert result.completed_deleted == 1
    assert result.side_effects_deleted >= 1
    assert queue.list_side_effects(job.id, tenant_id="t1") == []


def test_retention_cleaner_run_once(queue: JobQueue):
    from job_queue import RetentionCleaner

    queue.set_job_retention(completed_seconds=0, dead_seconds=0)
    for i in range(5):
        queue.enqueue(tenant_id="t1", payload={"i": i})
        leased = queue.lease(tenant_id="t1", worker_id="w1")
        assert leased is not None
        _ack(queue, leased, "w1")
    cleaner = RetentionCleaner(queue, batch_limit=2)
    total = 0
    for _ in range(10):
        r = cleaner.run_once()
        total += r.total_deleted
        if r.total_deleted == 0:
            break
    assert total == 5
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 0
