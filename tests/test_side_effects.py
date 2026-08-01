"""Once-only side effects across reclaim."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

# ---------------------------------------------------------------------------
# Once-only side effects across reclaim
# ---------------------------------------------------------------------------

def test_side_effect_survives_reclaim_and_skips_second_run(queue: JobQueue):
    job = queue.enqueue(
        tenant_id="t1",
        payload={"kind": "side_effects", "effects": ["charge", "email"]},
        visibility_timeout_seconds=1,
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    j1 = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=1,
    )
    assert j1 is not None and j1.lease_token is not None
    assert queue.side_effect_done(j1.id, tenant_id="t1", effect_key="charge") is False
    begun = queue.begin_side_effect(
        j1.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=j1.lease_token,
        effect_key="charge",
    )
    assert begun.status == "started"
    assert begun.idempotency_key == f"{j1.id}:charge"
    eff = queue.complete_side_effect(
        j1.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=j1.lease_token,
        effect_key="charge",
        result={"amount": 10},
    )
    assert eff.status == "completed"
    assert eff.result == {"amount": 10}
    # Idempotent complete
    eff2 = queue.complete_side_effect(
        j1.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=j1.lease_token,
        effect_key="charge",
        result={"amount": 99},
    )
    assert eff2.result == {"amount": 10}

    # Expire lease; second worker reclaims.
    time.sleep(1.2)
    j2 = queue.lease(
        tenant_id="t1",
        worker_id="w2",
        visibility_timeout_seconds=30,
    )
    assert j2 is not None and j2.id == job.id
    assert j2.leased_by == "w2"
    assert queue.side_effect_done(j2.id, tenant_id="t1", effect_key="charge") is True
    assert queue.side_effect_done(j2.id, tenant_id="t1", effect_key="email") is False
    queue.complete_side_effect(
        j2.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=j2.lease_token,
        effect_key="email",
        result={"to": "a@b.c"},
    )
    keys = {e.effect_key for e in queue.list_side_effects(j2.id, tenant_id="t1")}
    assert keys == {"charge", "email"}
    # Old worker cannot complete after reclaim.
    with pytest.raises(LeaseError):
        queue.complete_side_effect(
            j1.id,
            tenant_id="t1",
            worker_id="w1",
            lease_token=j1.lease_token,
            effect_key="refund",
        )

def test_side_effect_crash_after_call_before_complete_reuses_idempotency_key(
    queue: JobQueue,
):
    """Die after external call / before complete → reclaim keeps same key.

    External systems that dedupe on the idempotency key will not double-apply.
    """
    job = queue.enqueue(
        tenant_id="t1",
        payload={},
        visibility_timeout_seconds=1,
        max_attempts=5,
        retry_backoff_base_seconds=0,
    )
    j1 = queue.lease(
        tenant_id="t1",
        worker_id="w1",
        visibility_timeout_seconds=1,
    )
    assert j1 is not None and j1.lease_token is not None

    begun = queue.begin_side_effect(
        j1.id,
        tenant_id="t1",
        worker_id="w1",
        lease_token=j1.lease_token,
        effect_key="charge_card",
    )
    assert begun.status == "started"
    idem = begun.idempotency_key
    # Simulate successful external call, then worker death before complete.
    external = {"applied": 0, "by_key": {}}

    def apply_charge(key: str) -> dict:
        external["applied"] += 1
        if key in external["by_key"]:
            return external["by_key"][key]  # idempotent external
        out = {"charge_id": "ch_1", "attempts": external["applied"]}
        external["by_key"][key] = out
        return out

    first = apply_charge(idem)
    assert first["charge_id"] == "ch_1"
    assert queue.side_effect_done(j1.id, tenant_id="t1", effect_key="charge_card") is False
    mid = queue.get_side_effect(j1.id, tenant_id="t1", effect_key="charge_card")
    assert mid is not None and mid.status == "started"

    time.sleep(1.2)
    j2 = queue.lease(
        tenant_id="t1",
        worker_id="w2",
        visibility_timeout_seconds=30,
    )
    assert j2 is not None and j2.id == job.id
    reclaim_begin = queue.begin_side_effect(
        j2.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=j2.lease_token,
        effect_key="charge_card",
    )
    assert reclaim_begin.status == "started"
    assert reclaim_begin.idempotency_key == idem  # same key, not a new one
    second = apply_charge(reclaim_begin.idempotency_key)
    assert second == first  # external did not double-charge
    assert external["applied"] == 2  # two attempts, one logical charge
    assert len(external["by_key"]) == 1

    done = queue.complete_side_effect(
        j2.id,
        tenant_id="t1",
        worker_id="w2",
        lease_token=j2.lease_token,
        effect_key="charge_card",
        result=second,
    )
    assert done.status == "completed"
    assert done.result == first
    assert queue.side_effect_done(j2.id, tenant_id="t1", effect_key="charge_card") is True

def test_lease_context_run_side_effect_once(queue: JobQueue):
    from job_queue.worker import LeaseContext

    job = queue.enqueue(tenant_id="t1", payload={})
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    ctx = LeaseContext(
        job=leased,
        queue=queue,
        worker_id="w1",
        lease_lost=threading.Event(),
    )
    calls = {"n": 0}
    keys: list[str] = []

    def charge(idempotency_key: str) -> dict:
        calls["n"] += 1
        keys.append(idempotency_key)
        return {"ok": True, "n": calls["n"], "idem": idempotency_key}

    r1 = ctx.run_side_effect_once("charge_card", charge)
    r2 = ctx.run_side_effect_once("charge_card", charge)
    assert r1["ok"] is True and r1["n"] == 1
    assert r2["ok"] is True and r2["n"] == 1
    assert calls["n"] == 1
    assert len(keys) == 1
    assert keys[0] == f"{leased.id}:charge_card"

def test_side_effect_cross_tenant_rejected(queue: JobQueue):
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
    assert queue.side_effect_done(job.id, tenant_id="t1", effect_key="charge") is True
    assert queue.side_effect_done(job.id, tenant_id="other", effect_key="charge") is False
    assert queue.get_side_effect(job.id, tenant_id="other", effect_key="charge") is None
    assert queue.list_side_effects(job.id, tenant_id="other") == []
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.complete_side_effect(
            job.id,
            tenant_id="other",
            worker_id="w1",
            lease_token=leased.lease_token,
            effect_key="email",
        )

def test_side_effect_merge_tenant_preserves_markers(queue: JobQueue):
    job = queue.enqueue(tenant_id="src", payload={})
    leased = queue.lease(tenant_id="src", worker_id="w1")
    assert leased is not None and leased.lease_token is not None
    queue.complete_side_effect(
        leased.id,
        tenant_id="src",
        worker_id="w1",
        lease_token=leased.lease_token,
        effect_key="charge",
        result={"cents": 100},
    )
    queue.ack(
        leased.id,
        tenant_id="src",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    queue.merge_tenant("src", "dst")
    assert queue.side_effect_done(job.id, tenant_id="dst", effect_key="charge") is True
    eff = queue.get_side_effect(job.id, tenant_id="dst", effect_key="charge")
    assert eff is not None
    assert eff.tenant_id == "dst"
    assert eff.result == {"cents": 100}
    keys = {e.effect_key for e in queue.list_side_effects(job.id, tenant_id="dst")}
    assert keys == {"charge"}

