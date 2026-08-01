"""Tenant merge (ownership transfer A → B)."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus, LeaseError

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Tenant merge (ownership transfer A → B)
# ---------------------------------------------------------------------------


def test_merge_moves_pending_jobs_and_empties_source(queue: JobQueue):
    from job_queue import TenantMergeResult

    a1 = queue.enqueue(tenant_id="alpha", payload={"i": 1})
    a2 = queue.enqueue(tenant_id="alpha", payload={"i": 2})
    b1 = queue.enqueue(tenant_id="beta", payload={"i": 10})

    result = queue.merge_tenant("alpha", "beta")
    assert isinstance(result, TenantMergeResult)
    assert result.source_tenant_id == "alpha"
    assert result.target_tenant_id == "beta"
    assert result.jobs_moved == 2
    assert result.jobs_leased_moved == 0
    assert result.source_empty is True
    assert queue.tenant_is_empty("alpha") is True

    assert queue.count(tenant_id="alpha") == 0
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 3
    assert queue.get(a1.id, tenant_id="alpha") is None
    assert queue.get(a1.id, tenant_id="beta") is not None
    assert queue.get(a2.id, tenant_id="beta") is not None
    assert queue.get(b1.id, tenant_id="beta") is not None


def test_merge_moves_running_job_without_stopping_worker(queue: JobQueue):
    """Worker still heartbeats/acks with pre-merge tenant via lease_tenant_id."""
    queue.enqueue(tenant_id="alpha", payload={"run": True})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    assert leased.tenant_id == "alpha"
    assert leased.lease_tenant_id == "alpha"
    assert leased.lease_token is not None

    result = queue.merge_tenant("alpha", "beta")
    assert result.jobs_moved == 1
    assert result.jobs_leased_moved == 1
    assert queue.tenant_is_empty("alpha") is True

    # Row now lives under beta; claim tenant remains alpha.
    moved = queue.get(leased.id, tenant_id="beta")
    assert moved is not None
    assert moved.status == JobStatus.LEASED
    assert moved.tenant_id == "beta"
    assert moved.lease_tenant_id == "alpha"
    assert moved.lease_token == leased.lease_token
    assert moved.leased_by == "w1"

    # Pre-merge tenant still works (lease_tenant_id).
    hb = queue.heartbeat(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert hb.tenant_id == "beta"
    assert hb.lease_tenant_id == "alpha"

    # Current owner also works.
    hb2 = queue.heartbeat(
        leased.id,
        tenant_id="beta",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert hb2.tenant_id == "beta"

    # Unrelated third tenant still rejected even with valid token.
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.heartbeat(
            leased.id,
            tenant_id="gamma",
            worker_id="w1",
            lease_token=leased.lease_token,
        )

    held = queue.ensure_lease(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
    )
    assert held.tenant_id == "beta"
    assert held.lease_tenant_id == "alpha"

    acked = queue.ack(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    assert acked.status == JobStatus.COMPLETED
    assert acked.tenant_id == "beta"
    assert acked.lease_tenant_id is None
    assert queue.get_result(acked.id, tenant_id="beta") == {"ok": True}


def test_merge_moves_schedules_and_webhooks(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="alpha",
        name="nightly",
        interval_seconds=60,
        payload={"s": 1},
    )
    ep = queue.register_webhook(
        tenant_id="alpha",
        url="http://example.test/a",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="alpha", payload={"n": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    deliveries = queue.list_webhook_deliveries(tenant_id="alpha")
    assert len(deliveries) >= 1

    result = queue.merge_tenant("alpha", "beta")
    assert result.schedules_moved == 1
    assert result.webhook_endpoints_moved == 1
    assert result.webhook_deliveries_moved >= 1
    assert queue.tenant_is_empty("alpha") is True

    assert queue.get_schedule(sched.id, tenant_id="alpha") is None
    moved_sched = queue.get_schedule(sched.id, tenant_id="beta")
    assert moved_sched is not None
    assert moved_sched.name == "nightly"
    assert moved_sched.tenant_id == "beta"

    eps = queue.list_webhooks(tenant_id="beta")
    assert any(e.id == ep.id for e in eps)
    assert queue.list_webhooks(tenant_id="alpha") == []

    beta_dels = queue.list_webhook_deliveries(tenant_id="beta")
    assert any(d.job_id == job.id for d in beta_dels)


def test_merge_renames_conflicting_schedule_names(queue: JobQueue):
    queue.create_schedule(
        tenant_id="alpha",
        name="shared",
        interval_seconds=60,
        payload={"from": "a"},
    )
    queue.create_schedule(
        tenant_id="beta",
        name="shared",
        interval_seconds=60,
        payload={"from": "b"},
    )
    result = queue.merge_tenant("alpha", "beta")
    assert result.schedule_names_renamed == 1
    names = {s.name for s in queue.list_schedules(tenant_id="beta")}
    assert "shared" in names
    assert "shared__from_alpha" in names


def test_merge_clears_conflicting_active_dedupe_keys(queue: JobQueue):
    ja = queue.enqueue(tenant_id="alpha", payload={"a": 1}, dedupe_key="k")
    jb = queue.enqueue(tenant_id="beta", payload={"b": 1}, dedupe_key="k")
    result = queue.merge_tenant("alpha", "beta")
    assert result.dedupe_keys_cleared == 1
    # Both jobs still present under beta; source key cleared.
    ma = queue.get(ja.id, tenant_id="beta")
    mb = queue.get(jb.id, tenant_id="beta")
    assert ma is not None and mb is not None
    assert mb.dedupe_key == "k"
    assert ma.dedupe_key is None


def test_merge_rejects_same_tenant_and_offboarding_target(queue: JobQueue):
    from job_queue import MergeError

    with pytest.raises(ValueError, match="must differ"):
        queue.merge_tenant("a", "a")

    queue.enqueue(tenant_id="src", payload={"x": 1})
    queue.offboard_tenant(
        "tgt"
    )  # empty offboard still marks? empty becomes empty without settings
    # Force target into offboarding with residual leased work.
    queue.enqueue(tenant_id="tgt", payload={"y": 1})
    leased = queue.lease(tenant_id="tgt", worker_id="w1")
    assert leased is not None
    queue.offboard_tenant("tgt")
    assert queue.is_tenant_offboarding("tgt") is True

    with pytest.raises(MergeError, match="offboarding"):
        queue.merge_tenant("src", "tgt")


def test_merge_rejects_when_target_pending_cap_would_exceed(queue: JobQueue):
    from job_queue import MergeError

    queue.set_pending_limit("beta", 2)
    queue.enqueue(tenant_id="beta", payload={"b": 1})
    queue.enqueue(tenant_id="alpha", payload={"a": 1})
    queue.enqueue(tenant_id="alpha", payload={"a": 2})

    with pytest.raises(MergeError, match="pending job cap"):
        queue.merge_tenant("alpha", "beta")

    # Source still intact (transaction rolled back).
    assert queue.count(tenant_id="alpha", status=JobStatus.PENDING) == 2
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 1


def test_merge_blocks_source_enqueue_after(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={"a": 1})
    queue.merge_tenant("alpha", "beta")
    # Source has no residual state; new enqueue under alpha is a fresh tenant.
    j = queue.enqueue(tenant_id="alpha", payload={"fresh": True})
    assert j.tenant_id == "alpha"
    assert queue.count(tenant_id="alpha") == 1


def test_merge_claimed_webhook_finalize_after_transfer(queue: JobQueue):
    queue.register_webhook(
        tenant_id="alpha",
        url="http://example.test/hook",
        events=["job.completed"],
    )
    job = queue.enqueue(tenant_id="alpha", payload={"n": 1})
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=60.0)
    assert len(claimed) == 1
    did = claimed[0].delivery.id
    assert claimed[0].delivery.tenant_id == "alpha"

    queue.merge_tenant("alpha", "beta")
    assert queue.tenant_is_empty("alpha") is True

    fin = queue.finalize_webhook_delivery(did, status_code=200)
    assert fin is not None and fin.success
    d = queue.get_webhook_delivery(did, tenant_id="beta")
    assert d is not None
    assert d.status == "succeeded"
    assert d.job_id == job.id


def test_merge_idempotent_empty_source(queue: JobQueue):
    result = queue.merge_tenant("never-had-work", "beta")
    assert result.jobs_moved == 0
    assert result.source_empty is True
    assert queue.tenant_is_empty("never-had-work") is True
