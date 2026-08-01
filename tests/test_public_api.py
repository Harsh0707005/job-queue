"""Public API coverage (previously untested entry points)."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Public API coverage (previously untested entry points)
# ---------------------------------------------------------------------------

def test_delete_webhook_and_set_enabled(queue: JobQueue):
    from job_queue import WebhookEvent

    ep = queue.register_webhook(
        tenant_id="t1",
        url="http://127.0.0.1:9/hook",
        events=[WebhookEvent.JOB_COMPLETED.value],
        secret="s1",
    )
    assert ep.enabled is True
    disabled = queue.set_webhook_enabled(ep.id, tenant_id="t1", enabled=False)
    assert disabled.enabled is False
    listed = queue.list_webhooks(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].enabled is False
    assert queue.delete_webhook(ep.id, tenant_id="t1") is True
    assert queue.list_webhooks(tenant_id="t1") == []
    assert queue.delete_webhook(ep.id, tenant_id="t1") is False

def test_get_lease_rate_limit_roundtrip(queue: JobQueue):
    assert queue.get_lease_rate_limit("t1") == (None, None)
    queue.set_lease_rate_limit("t1", 2.5, burst=5)
    rate, burst = queue.get_lease_rate_limit("t1")
    assert rate == 2.5
    assert burst == 5
    queue.set_lease_rate_limit("t1", None)
    assert queue.get_lease_rate_limit("t1") == (None, None)

def test_list_awaiting_children(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None and leased.id == parent.id
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=leased.lease_token,
        children=[{"payload": {"c": 1}}, {"payload": {"c": 2}}],
        wait=True,
    )
    assert result.parent.status == JobStatus.AWAITING_CHILDREN
    waiting = queue.list_awaiting_children(tenant_id="t1")
    assert len(waiting) == 1
    assert waiting[0].id == parent.id

def test_rebuild_jobs_from_audit_batch(queue: JobQueue):
    j1 = queue.enqueue(tenant_id="t1", payload={"a": 1})
    j2 = queue.enqueue(tenant_id="t1", payload={"a": 2})
    for jid, wid in ((j1.id, "w1"), (j2.id, "w2")):
        leased = queue.lease(tenant_id="t1", worker_id=wid)
        assert leased is not None and leased.id == jid
        _ack(queue, leased, wid, result={"ok": True})
    with queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = ANY(%s)", ([j1.id, j2.id],))
        conn.commit()
    rebuilt = queue.rebuild_jobs_from_audit([j1.id, j2.id], tenant_id="t1")
    assert {r.id for r in rebuilt} == {j1.id, j2.id}
    assert all(r.status == JobStatus.COMPLETED for r in rebuilt)

