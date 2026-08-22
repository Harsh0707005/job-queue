"""Named wait_signal steps: early latch, deadline undo, tenant isolation."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack, _nack


def _drive(queue: JobQueue, tenant_id: str, *, fail: bool = False):
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    if fail:
        _nack(queue, j, "w", error="step boom", requeue=True)
    else:
        _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})
    return j


def _steps_wait(*, timeout: float | None = 30.0) -> list[dict]:
    wait: dict = {
        "name": "gate",
        "kind": "wait_signal",
        "signal": "approved",
    }
    if timeout is not None:
        wait["timeout_seconds"] = timeout
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        wait,
        {
            "name": "ship",
            "kind": "effect",
            "payload": {"op": "ship"},
        },
    ]


def test_wait_signal_late_send_unblocks(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-late",
        max_attempts=1,
        steps=_steps_wait(),
    )
    _drive(queue, "sig-late")
    parked = queue.get_workflow(run.id, tenant_id="sig-late")
    assert parked is not None
    assert parked.status == WorkflowRunStatus.RUNNING.value
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-late")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_SIGNAL.value
    assert gate.job_id is None
    assert queue.lease(tenant_id="sig-late", worker_id="w") is None

    out = queue.send_workflow_signal(
        run.id,
        tenant_id="sig-late",
        name="approved",
        payload={"by": "ops"},
    )
    assert out["created"] is True
    assert out["unblocked"] == 1
    assert out["signal"].payload == {"by": "ops"}

    j = _drive(queue, "sig-late")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="sig-late")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-late")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.COMPLETED.value
    assert gate.result["signal"] == "approved"
    assert gate.result["payload"] == {"by": "ops"}


def test_wait_signal_early_send_still_counts(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-early",
        max_attempts=1,
        steps=_steps_wait(),
    )
    out = queue.send_workflow_signal(
        run.id, tenant_id="sig-early", name="approved", payload={"early": True}
    )
    assert out["created"] is True
    assert out["unblocked"] == 0
    _drive(queue, "sig-early")
    j = _drive(queue, "sig-early")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="sig-early")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-early")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.COMPLETED.value
    assert gate.result["payload"] == {"early": True}


def test_wait_signal_first_step_parks_until_send(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-first",
        max_attempts=1,
        steps=[
            {"name": "gate", "kind": "wait_signal", "signal": "go"},
            {"name": "work", "kind": "effect", "payload": {"op": "work"}},
        ],
    )
    assert queue.lease(tenant_id="sig-first", worker_id="w") is None
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-first")
    assert steps[0].status == WorkflowStepStatus.AWAITING_SIGNAL.value
    queue.send_workflow_signal(run.id, tenant_id="sig-first", name="go")
    j = _drive(queue, "sig-first")
    assert j.payload.get("step_name") == "work"
    done = queue.get_workflow(run.id, tenant_id="sig-first")
    assert done is not None and done.status == WorkflowRunStatus.COMPLETED.value


def test_wait_signal_duplicate_send_is_idempotent(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-dup",
        max_attempts=1,
        steps=[{"name": "gate", "kind": "wait_signal", "signal": "go"}],
    )
    first = queue.send_workflow_signal(
        run.id, tenant_id="sig-dup", name="go", payload={"n": 1}
    )
    second = queue.send_workflow_signal(
        run.id, tenant_id="sig-dup", name="go", payload={"n": 2}
    )
    assert first["created"] is True
    assert second["created"] is False
    assert second["signal"].payload == {"n": 1}
    listed = queue.list_workflow_signals(run.id, tenant_id="sig-dup")
    assert len(listed) == 1
    assert listed[0].payload == {"n": 1}
    done = queue.get_workflow(run.id, tenant_id="sig-dup")
    assert done is not None and done.status == WorkflowRunStatus.COMPLETED.value


def test_wait_signal_deadline_zero_fails_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-dl0",
        max_attempts=1,
        steps=_steps_wait(timeout=0),
    )
    _drive(queue, "sig-dl0")
    done = queue.get_workflow(run.id, tenant_id="sig-dl0")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPENSATING.value
    assert "deadline" in (done.fail_reason or "")
    j = queue.lease(tenant_id="sig-dl0", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    assert j.payload.get("step_name") == "reserve"
    _ack(queue, j, "w", result={"undid": "reserve"})
    failed = queue.get_workflow(run.id, tenant_id="sig-dl0")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-dl0")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.FAILED.value
    comps = [s for s in steps if s.direction == "compensate"]
    assert any(s.status == WorkflowStepStatus.COMPENSATED.value for s in comps)


def test_wait_signal_deadline_processor_fails_and_undoes(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-dl",
        max_attempts=1,
        steps=_steps_wait(timeout=0.2),
    )
    _drive(queue, "sig-dl")
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-dl")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_SIGNAL.value
    time.sleep(0.35)
    n = queue.process_workflow_signal_timeouts(tenant_id="sig-dl", limit=10)
    assert n >= 1
    j = queue.lease(tenant_id="sig-dl", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w", result={"undid": True})
    failed = queue.get_workflow(run.id, tenant_id="sig-dl")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_wait_signal_late_send_does_not_revive_failed_wait(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-dead",
        max_attempts=1,
        steps=_steps_wait(timeout=0),
    )
    _drive(queue, "sig-dead")
    out = queue.send_workflow_signal(
        run.id, tenant_id="sig-dead", name="approved", payload={"too": "late"}
    )
    assert out["created"] is True
    assert out["unblocked"] == 0
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-dead")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.FAILED.value


def test_wait_signal_tenant_cannot_send_or_list_foreign(queue: JobQueue):
    a = queue.start_workflow(
        tenant_id="sig-a",
        max_attempts=1,
        steps=[{"name": "gate", "kind": "wait_signal", "signal": "go"}],
    )
    b = queue.start_workflow(
        tenant_id="sig-b",
        max_attempts=1,
        steps=[{"name": "gate", "kind": "wait_signal", "signal": "go"}],
    )
    with pytest.raises(ValueError, match="not found"):
        queue.send_workflow_signal(a.id, tenant_id="sig-b", name="go")
    with pytest.raises(ValueError, match="not found"):
        queue.list_workflow_signals(a.id, tenant_id="sig-b")
    with pytest.raises(ValueError, match="not found"):
        queue.get_workflow_signal(a.id, tenant_id="sig-b", name="go")
    assert queue.get_workflow(a.id, tenant_id="sig-b") is None

    steps_a = queue.list_workflow_steps(a.id, tenant_id="sig-a")
    assert steps_a[0].status == WorkflowStepStatus.AWAITING_SIGNAL.value

    queue.send_workflow_signal(b.id, tenant_id="sig-b", name="go", payload={"who": "b"})
    done_b = queue.get_workflow(b.id, tenant_id="sig-b")
    assert done_b is not None and done_b.status == WorkflowRunStatus.COMPLETED.value
    still_a = queue.get_workflow(a.id, tenant_id="sig-a")
    assert still_a is not None and still_a.status == WorkflowRunStatus.RUNNING.value
    assert queue.list_workflow_signals(a.id, tenant_id="sig-a") == []

    queue.send_workflow_signal(a.id, tenant_id="sig-a", name="go", payload={"who": "a"})
    done_a = queue.get_workflow(a.id, tenant_id="sig-a")
    assert done_a is not None and done_a.status == WorkflowRunStatus.COMPLETED.value
    listed_a = queue.list_workflow_signals(a.id, tenant_id="sig-a")
    assert [s.payload for s in listed_a] == [{"who": "a"}]
    listed_b = queue.list_workflow_signals(b.id, tenant_id="sig-b")
    assert [s.payload for s in listed_b] == [{"who": "b"}]


def test_wait_signal_requires_name(queue: JobQueue):
    with pytest.raises(ValueError, match="signal"):
        queue.start_workflow(
            tenant_id="sig-noname",
            steps=[{"name": "gate", "kind": "wait_signal"}],
        )


def test_wait_signal_cancel_skips_parked_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-can",
        max_attempts=1,
        steps=_steps_wait(),
    )
    _drive(queue, "sig-can")
    out = queue.cancel_workflow(run.id, tenant_id="sig-can")
    assert out["run"].status in (
        WorkflowRunStatus.COMPENSATING.value,
        WorkflowRunStatus.CANCELLED.value,
    )
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-can")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.SKIPPED.value
    j = queue.lease(tenant_id="sig-can", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w")
    done = queue.get_workflow(run.id, tenant_id="sig-can")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value


def test_wait_signal_http_send_and_isolation(queue: JobQueue):
    from http.server import ThreadingHTTPServer
    import threading

    from job_queue.coordinator.app import Coordinator, _make_handler
    from job_queue.remote.client import CoordinatorClient

    run = queue.start_workflow(
        tenant_id="sig-http",
        max_attempts=1,
        steps=[{"name": "gate", "kind": "wait_signal", "signal": "go"}],
    )
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(
            f"http://127.0.0.1:{server.server_address[1]}"
        )
        foreign = client.send_workflow_signal(
            run.id, tenant_id="other", name="go"
        )
        assert foreign.get("_http") == 404
        still = client.get_workflow(run.id, tenant_id="sig-http")
        assert still["run"]["status"] == WorkflowRunStatus.RUNNING.value
        ok = client.send_workflow_signal(
            run.id, tenant_id="sig-http", name="go", payload={"k": 1}
        )
        assert ok["created"] is True
        listed = client.list_workflow_signals(run.id, tenant_id="sig-http")
        assert listed["count"] == 1
        done = client.get_workflow(run.id, tenant_id="sig-http")
        assert done["run"]["status"] == WorkflowRunStatus.COMPLETED.value
    finally:
        server.shutdown()


def test_wait_signal_does_not_enqueue_job(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="sig-noj",
        max_attempts=1,
        steps=[{"name": "gate", "kind": "wait_signal", "signal": "go"}],
    )
    jobs = queue.list_jobs(tenant_id="sig-noj")
    assert jobs == []
    steps = queue.list_workflow_steps(run.id, tenant_id="sig-noj")
    assert steps[0].job_id is None
    assert steps[0].status == WorkflowStepStatus.AWAITING_SIGNAL.value
    _ = JobStatus
