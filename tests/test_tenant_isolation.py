"""Multi-tenant isolation."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus, LeaseError

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Multi-tenant isolation
# ---------------------------------------------------------------------------


def test_tenants_are_isolated(queue: JobQueue):
    a = queue.enqueue(tenant_id="alpha", payload={"who": "a"})
    b = queue.enqueue(tenant_id="beta", payload={"who": "b"})

    leased_a = queue.lease(tenant_id="alpha", worker_id="w1")
    leased_b = queue.lease(tenant_id="beta", worker_id="w1")

    assert leased_a is not None and leased_a.id == a.id
    assert leased_b is not None and leased_b.id == b.id
    assert queue.lease(tenant_id="alpha", worker_id="w2") is None


def test_cross_tenant_get_returns_none(queue: JobQueue):
    job = queue.enqueue(tenant_id="alpha", payload={"secret": True})
    assert queue.get(job.id, tenant_id="alpha") is not None
    assert queue.get(job.id, tenant_id="beta") is None


def test_cross_tenant_ack_heartbeat_nack_rejected(queue: JobQueue):
    """Unrelated tenant cannot heartbeat/ack/nack even with a valid lease token."""
    from uuid import uuid4

    queue.enqueue(tenant_id="alpha", payload={})
    job = queue.lease(tenant_id="alpha", worker_id="w1")
    assert job is not None
    assert job.lease_token is not None
    assert job.lease_tenant_id == "alpha"

    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.ack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.heartbeat(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.nack(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
            error="nope",
        )
    with pytest.raises(LeaseError, match="not found for tenant"):
        queue.ensure_lease(
            job.id,
            tenant_id="beta",
            worker_id="w1",
            lease_token=job.lease_token,
        )

    # Wrong token still rejected for the owning tenant.
    with pytest.raises(LeaseError):
        queue.heartbeat(
            job.id,
            tenant_id="alpha",
            worker_id="w1",
            lease_token=uuid4(),
        )

    # Owning tenant can still complete.
    _ack(queue, job, "w1")
    assert queue.get(job.id, tenant_id="alpha") is not None
    assert queue.get(job.id, tenant_id="beta") is None
    final = queue.get(job.id, tenant_id="alpha")
    assert final is not None
    assert final.status == JobStatus.COMPLETED


def test_queue_name_isolation(queue: JobQueue):
    queue.enqueue(tenant_id="t1", queue_name="email", payload={"t": "e"})
    queue.enqueue(tenant_id="t1", queue_name="sms", payload={"t": "s"})

    email = queue.lease(tenant_id="t1", worker_id="w1", queue_name="email")
    assert email is not None
    assert email.payload["t"] == "e"

    sms = queue.lease(tenant_id="t1", worker_id="w1", queue_name="sms")
    assert sms is not None
    assert sms.payload["t"] == "s"
