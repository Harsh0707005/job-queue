"""Workflow external activities: start, heartbeat, timeout → compensate."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, LeaseError
from job_queue.models import JobStatus, WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack


def _activity_steps(*, timeout: float, heartbeat: float, name: str = "charge"):
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        {
            "name": name,
            "kind": "activity",
            "activity": name,
            "timeout_seconds": timeout,
            "heartbeat_timeout_seconds": heartbeat,
            "payload": {"op": name},
            "compensate": {"kind": "effect", "payload": {"op": f"undo_{name}"}},
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def _drive_effect(queue: JobQueue, tenant_id: str):
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})
    return j


def test_activity_start_complete_continues(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-ok",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=5),
    )
    _drive_effect(queue, "act-ok")
    job = queue.lease(tenant_id="act-ok", worker_id="w")
    assert job is not None
    assert job.payload["step"]["kind"] == "activity"
    started = queue.start_workflow_activity(
        job.id,
        tenant_id="act-ok",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert started.status == "running"
    again = queue.start_workflow_activity(
        job.id,
        tenant_id="act-ok",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert again.id == started.id
    done = queue.complete_workflow_activity(
        job.id,
        tenant_id="act-ok",
        worker_id="w",
        lease_token=job.lease_token,
        result={"charged": True},
    )
    assert done.status == "completed"
    _ack(queue, job, "w", result=done.result)
    j = _drive_effect(queue, "act-ok")
    assert j.payload.get("step_name") == "ship"
    fin = queue.get_workflow(run.id, tenant_id="act-ok")
    assert fin is not None
    assert fin.status == WorkflowRunStatus.COMPLETED.value


def test_activity_heartbeat_timeout_fails_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-hb",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=0.2),
    )
    _drive_effect(queue, "act-hb")
    job = queue.lease(tenant_id="act-hb", worker_id="w")
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-hb",
        worker_id="w",
        lease_token=job.lease_token,
    )
    time.sleep(0.35)
    n = queue.process_workflow_activities(tenant_id="act-hb", limit=10)
    assert n >= 1
    act = queue.get_workflow_activity(job.id, tenant_id="act-hb")
    assert act is not None
    assert act.status == "timed_out"
    assert "heartbeat" in (act.error or "")
    dead = queue.get(job.id, tenant_id="act-hb")
    assert dead is not None
    assert dead.status == JobStatus.DEAD
    mid = queue.get_workflow(run.id, tenant_id="act-hb")
    assert mid is not None
    assert mid.status == WorkflowRunStatus.COMPENSATING.value
    with pytest.raises(LeaseError):
        queue.heartbeat(
            job.id,
            tenant_id="act-hb",
            worker_id="w",
            lease_token=job.lease_token,
        )
    c = queue.lease(tenant_id="act-hb", worker_id="undo")
    assert c is not None
    assert c.payload.get("direction") == "compensate"
    assert c.payload.get("step_name") == "reserve"
    _ack(queue, c, "undo", result={"undid": True})
    # Charge also has compensate.
    c2 = queue.lease(tenant_id="act-hb", worker_id="undo")
    if c2 is not None:
        _ack(queue, c2, "undo", result={"undid": True})
    failed = queue.get_workflow(run.id, tenant_id="act-hb")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="act-hb")
    charge = next(s for s in steps if s.name == "charge")
    assert charge.status == WorkflowStepStatus.FAILED.value


def test_activity_start_to_close_timeout_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-to",
        max_attempts=1,
        steps=_activity_steps(timeout=0.2, heartbeat=5),
    )
    _drive_effect(queue, "act-to")
    job = queue.lease(tenant_id="act-to", worker_id="w")
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-to",
        worker_id="w",
        lease_token=job.lease_token,
    )
    # Keep the activity heartbeat alive so only start-to-close fires.
    time.sleep(0.12)
    queue.heartbeat_workflow_activity(
        job.id,
        tenant_id="act-to",
        worker_id="w",
        lease_token=job.lease_token,
    )
    time.sleep(0.15)
    n = queue.process_workflow_activities(tenant_id="act-to", limit=10)
    assert n >= 1
    act = queue.get_workflow_activity(job.id, tenant_id="act-to")
    assert act is not None
    assert act.status == "timed_out"
    assert "activity timeout" in (act.error or "")
    c = queue.lease(tenant_id="act-to", worker_id="undo")
    assert c is not None
    assert c.payload.get("direction") == "compensate"
    _ack(queue, c, "undo")
    extra = queue.lease(tenant_id="act-to", worker_id="undo")
    if extra is not None:
        _ack(queue, extra, "undo")
    failed = queue.get_workflow(run.id, tenant_id="act-to")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_activity_heartbeat_extends_due(queue: JobQueue):
    queue.start_workflow(
        tenant_id="act-ext",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=0.4),
    )
    _drive_effect(queue, "act-ext")
    job = queue.lease(tenant_id="act-ext", worker_id="w")
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-ext",
        worker_id="w",
        lease_token=job.lease_token,
    )
    time.sleep(0.25)
    queue.heartbeat_workflow_activity(
        job.id,
        tenant_id="act-ext",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert queue.process_workflow_activities(tenant_id="act-ext", limit=10) == 0
    act = queue.get_workflow_activity(job.id, tenant_id="act-ext")
    assert act is not None
    assert act.status == "running"
    queue.complete_workflow_activity(
        job.id,
        tenant_id="act-ext",
        worker_id="w",
        lease_token=job.lease_token,
        result={"ok": True},
    )
    _ack(queue, job, "w", result={"ok": True})


def test_activity_requires_timeout(queue: JobQueue):
    with pytest.raises(ValueError, match="timeout"):
        queue.start_workflow(
            tenant_id="act-bad",
            steps=[{"name": "x", "kind": "activity", "activity": "x"}],
        )


def test_activity_tenant_isolation(queue: JobQueue):
    queue.start_workflow(
        tenant_id="act-a",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=0.2, name="aa"),
    )
    queue.start_workflow(
        tenant_id="act-b",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=0.2, name="bb"),
    )
    _drive_effect(queue, "act-a")
    _drive_effect(queue, "act-b")
    ja = queue.lease(tenant_id="act-a", worker_id="wa")
    jb = queue.lease(tenant_id="act-b", worker_id="wb")
    assert ja is not None and jb is not None
    queue.start_workflow_activity(
        ja.id, tenant_id="act-a", worker_id="wa", lease_token=ja.lease_token
    )
    queue.start_workflow_activity(
        jb.id, tenant_id="act-b", worker_id="wb", lease_token=jb.lease_token
    )
    assert queue.get_workflow_activity(ja.id, tenant_id="act-b") is None
    time.sleep(0.35)
    n = queue.process_workflow_activities(tenant_id="act-a", limit=10)
    assert n >= 1
    still = queue.get_workflow_activity(jb.id, tenant_id="act-b")
    assert still is not None
    assert still.status == "running"
    dead_a = queue.get(ja.id, tenant_id="act-a")
    assert dead_a is not None and dead_a.status == JobStatus.DEAD
    live_b = queue.get(jb.id, tenant_id="act-b")
    assert live_b is not None and live_b.status == JobStatus.LEASED


def test_activity_timeout_after_lease_reclaim_still_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-reclaim",
        max_attempts=5,
        visibility_timeout_seconds=1,
        steps=_activity_steps(timeout=30, heartbeat=0.2),
    )
    _drive_effect(queue, "act-reclaim")
    job = queue.lease(
        tenant_id="act-reclaim",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-reclaim",
        worker_id="dead",
        lease_token=job.lease_token,
    )
    time.sleep(1.2)
    n = queue.process_workflow_activities(tenant_id="act-reclaim", limit=10)
    assert n >= 1
    with pytest.raises(LeaseError):
        _ack(queue, job, "dead", result={"stale": True})
    reclaimed = queue.lease(tenant_id="act-reclaim", worker_id="alive")
    # Job is dead, so reclaim should yield the compensate job instead.
    assert reclaimed is not None
    assert reclaimed.payload.get("direction") == "compensate"
    _ack(queue, reclaimed, "alive")
    extra = queue.lease(tenant_id="act-reclaim", worker_id="alive")
    if extra is not None:
        _ack(queue, extra, "alive")
    failed = queue.get_workflow(run.id, tenant_id="act-reclaim")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
