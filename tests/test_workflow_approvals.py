"""Workflow approval steps: park, approve, reject+undo, reclaim, load."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from job_queue import ApprovalError, JobQueue, LeaseError
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack


def _drive(queue: JobQueue, tenant_id: str, *, worker: str = "w"):
    j = queue.lease(tenant_id=tenant_id, worker_id=worker)
    assert j is not None
    _ack(queue, j, worker, result={"ok": True, "name": j.payload.get("step_name")})
    return j


def _approval_steps(*, timeout: float | None = None, message: str = "sign-off"):
    gate: dict = {
        "name": "gate",
        "kind": "approval",
        "message": message,
    }
    if timeout is not None:
        gate["timeout_seconds"] = timeout
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve"},
            "compensate": {"kind": "effect", "payload": {"op": "release"}},
        },
        gate,
        {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
    ]


def test_approval_parks_without_a_job_then_approve_continues(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-ok",
        max_attempts=1,
        steps=_approval_steps(),
    )
    j = _drive(queue, "ap-ok")
    assert j.payload.get("step_name") == "reserve"
    assert queue.lease(tenant_id="ap-ok", worker_id="w") is None
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-ok")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_APPROVAL.value
    assert gate.job_id is None
    assert (gate.result or {}).get("message") == "sign-off"
    waiting = queue.list_awaiting_workflow_approvals(tenant_id="ap-ok")
    assert [s.id for s in waiting] == [gate.id]

    out = queue.approve_workflow(
        run.id, tenant_id="ap-ok", decided_by="ops", comment="lgtm"
    )
    assert out["step"].status == WorkflowStepStatus.COMPLETED.value
    assert out["step"].result["decision"] == "approved"
    # Idempotent.
    again = queue.approve_workflow(run.id, tenant_id="ap-ok")
    assert again["step"].id == out["step"].id

    j = _drive(queue, "ap-ok")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="ap-ok")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    assert queue.list_awaiting_workflow_approvals(tenant_id="ap-ok") == []


def test_approval_reject_compensates_completed_work(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-no",
        max_attempts=1,
        steps=_approval_steps(),
    )
    _drive(queue, "ap-no")
    out = queue.reject_workflow(run.id, tenant_id="ap-no", reason="nope")
    assert out["run"].status == WorkflowRunStatus.COMPENSATING.value
    assert out["step"].status == WorkflowStepStatus.FAILED.value
    assert out["step"].result["decision"] == "rejected"
    assert "nope" in (out["step"].error or "")

    j = queue.lease(tenant_id="ap-no", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    assert j.payload.get("step_name") == "reserve"
    _ack(queue, j, "w", result={"undid": "reserve"})
    failed = queue.get_workflow(run.id, tenant_id="ap-no")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-no")
    assert not any(
        s.name == "ship"
        and s.direction == "forward"
        and s.status == WorkflowStepStatus.COMPLETED.value
        for s in steps
    )
    again = queue.reject_workflow(run.id, tenant_id="ap-no")
    assert again["step"].status == WorkflowStepStatus.FAILED.value
    with pytest.raises(ApprovalError, match="already failed"):
        queue.approve_workflow(run.id, tenant_id="ap-no")


def test_approval_zero_timeout_rejects_immediately(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-0",
        max_attempts=1,
        steps=_approval_steps(timeout=0),
    )
    _drive(queue, "ap-0")
    # Zero-second deadline fires in the same park txn.
    parked = queue.get_workflow(run.id, tenant_id="ap-0")
    assert parked is not None
    assert parked.status == WorkflowRunStatus.COMPENSATING.value
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-0")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.FAILED.value
    assert (gate.result or {}).get("decision") == "timeout"
    assert gate.job_id is None
    j = queue.lease(tenant_id="ap-0", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w")
    failed = queue.get_workflow(run.id, tenant_id="ap-0")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_approval_survives_dead_coordinator_then_approve(queue: JobQueue):
    """No worker job: a missed control tick cannot lose or double-run the wait."""
    run = queue.start_workflow(
        tenant_id="ap-crash",
        max_attempts=1,
        steps=_approval_steps(),
    )
    _drive(queue, "ap-crash")
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-crash")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_APPROVAL.value
    # Coordinator is "dead": no process_* ticks, no worker lease.
    time.sleep(0.15)
    still = queue.list_workflow_steps(run.id, tenant_id="ap-crash")
    gate2 = next(s for s in still if s.name == "gate")
    assert gate2.status == WorkflowStepStatus.AWAITING_APPROVAL.value
    assert gate2.id == gate.id
    assert queue.lease(tenant_id="ap-crash", worker_id="w") is None
    queue.approve_workflow(run.id, tenant_id="ap-crash")
    _drive(queue, "ap-crash")
    done = queue.get_workflow(run.id, tenant_id="ap-crash")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_approval_timeout_after_missed_ticks_rejects_once(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-late",
        max_attempts=1,
        steps=_approval_steps(timeout=0.2),
    )
    _drive(queue, "ap-late")
    time.sleep(0.35)
    # Coordinator was down for the whole window; one tick rejects once.
    assert queue.process_workflow_approval_timeouts(tenant_id="ap-late", limit=10) >= 1
    assert queue.process_workflow_approval_timeouts(tenant_id="ap-late", limit=10) == 0
    j = queue.lease(tenant_id="ap-late", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w")
    failed = queue.get_workflow(run.id, tenant_id="ap-late")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_approval_survives_sibling_lease_reclaim(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-reclaim",
        max_attempts=5,
        visibility_timeout_seconds=1,
        steps=[
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve"},
                "compensate": {"kind": "effect", "payload": {"op": "release"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [{"name": "slow", "kind": "effect", "payload": {"op": "slow"}}],
                    [{"name": "gate", "kind": "approval", "message": "hold"}],
                ],
            },
            {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
        ],
    )
    _drive(queue, "ap-reclaim")
    slow = queue.lease(
        tenant_id="ap-reclaim",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert slow is not None
    assert slow.payload.get("step_name") == "slow"
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-reclaim")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.AWAITING_APPROVAL.value
    assert gate.job_id is None

    time.sleep(1.15)
    reclaimed = queue.lease(
        tenant_id="ap-reclaim",
        worker_id="alive",
        visibility_timeout_seconds=30,
    )
    assert reclaimed is not None
    assert reclaimed.id == slow.id
    with pytest.raises(LeaseError):
        _ack(queue, slow, "dead", result={"stale": True})

    still = queue.list_workflow_steps(run.id, tenant_id="ap-reclaim")
    gate2 = next(s for s in still if s.name == "gate")
    assert gate2.status == WorkflowStepStatus.AWAITING_APPROVAL.value
    assert gate2.id == gate.id
    queue.approve_workflow(run.id, tenant_id="ap-reclaim")
    _ack(queue, reclaimed, "alive", result={"ok": True})
    j = _drive(queue, "ap-reclaim")
    assert j.payload.get("step_name") == "ship"
    done = queue.get_workflow(run.id, tenant_id="ap-reclaim")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_approval_reject_after_reclaim_still_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-rej-rc",
        max_attempts=5,
        visibility_timeout_seconds=1,
        steps=[
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve"},
                "compensate": {"kind": "effect", "payload": {"op": "release"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [{"name": "slow", "kind": "effect", "payload": {"op": "slow"}}],
                    [{"name": "gate", "kind": "approval"}],
                ],
            },
        ],
    )
    _drive(queue, "ap-rej-rc")
    slow = queue.lease(
        tenant_id="ap-rej-rc",
        worker_id="dead",
        visibility_timeout_seconds=1,
    )
    assert slow is not None
    queue.reject_workflow(run.id, tenant_id="ap-rej-rc", reason="no")
    parked = queue.get_workflow(run.id, tenant_id="ap-rej-rc")
    assert parked is not None
    assert parked.status == WorkflowRunStatus.COMPENSATING.value
    time.sleep(1.1)
    reclaimed = queue.lease(
        tenant_id="ap-rej-rc",
        worker_id="alive",
        visibility_timeout_seconds=30,
    )
    if reclaimed is not None:
        with pytest.raises(LeaseError):
            _ack(queue, slow, "dead", result={"stale": True})
        _ack(queue, reclaimed, "alive", result={"ok": True})
    deadline = time.time() + 5
    while time.time() < deadline:
        got = queue.get_workflow(run.id, tenant_id="ap-rej-rc")
        if got is not None and got.status == WorkflowRunStatus.FAILED.value:
            return
        nxt = queue.lease(tenant_id="ap-rej-rc", worker_id="w")
        if nxt is not None:
            _ack(queue, nxt, "w", result={"ok": True})
    raise AssertionError("reject after reclaim did not settle")


def test_approval_wrong_tenant_and_isolation(queue: JobQueue):
    a = queue.start_workflow(
        tenant_id="ap-a", max_attempts=1, steps=_approval_steps()
    )
    b = queue.start_workflow(
        tenant_id="ap-b", max_attempts=1, steps=_approval_steps()
    )
    _drive(queue, "ap-a")
    _drive(queue, "ap-b")
    with pytest.raises(ValueError, match="not found"):
        queue.approve_workflow(a.id, tenant_id="ap-b")
    with pytest.raises(ValueError, match="not found"):
        queue.reject_workflow(b.id, tenant_id="ap-a")
    listed_a = queue.list_awaiting_workflow_approvals(tenant_id="ap-a")
    listed_b = queue.list_awaiting_workflow_approvals(tenant_id="ap-b")
    assert {s.run_id for s in listed_a} == {a.id}
    assert {s.run_id for s in listed_b} == {b.id}
    queue.approve_workflow(a.id, tenant_id="ap-a")
    still_b = queue.list_workflow_steps(b.id, tenant_id="ap-b")
    assert next(s for s in still_b if s.name == "gate").status == (
        WorkflowStepStatus.AWAITING_APPROVAL.value
    )


def test_approval_cancel_skips_parked_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="ap-can",
        max_attempts=1,
        steps=_approval_steps(),
    )
    _drive(queue, "ap-can")
    out = queue.cancel_workflow(run.id, tenant_id="ap-can")
    assert out["run"].status in (
        WorkflowRunStatus.COMPENSATING.value,
        WorkflowRunStatus.CANCELLED.value,
    )
    steps = queue.list_workflow_steps(run.id, tenant_id="ap-can")
    gate = next(s for s in steps if s.name == "gate")
    assert gate.status == WorkflowStepStatus.SKIPPED.value
    j = queue.lease(tenant_id="ap-can", worker_id="w")
    assert j is not None
    assert j.payload.get("direction") == "compensate"
    _ack(queue, j, "w")
    done = queue.get_workflow(run.id, tenant_id="ap-can")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value


def test_approval_cannot_be_a_compensate_step(queue: JobQueue):
    with pytest.raises(ValueError, match="cannot wait for approval"):
        queue.start_workflow(
            tenant_id="ap-badc",
            steps=[
                {
                    "name": "work",
                    "payload": {"op": "x"},
                    "compensate": {"kind": "approval"},
                }
            ],
        )


def test_approval_http_decide_and_isolation(queue: JobQueue):
    from http.server import ThreadingHTTPServer
    import threading

    from job_queue.coordinator.app import Coordinator, _make_handler
    from job_queue.remote.client import CoordinatorClient

    run = queue.start_workflow(
        tenant_id="ap-http",
        max_attempts=1,
        steps=_approval_steps(),
    )
    _drive(queue, "ap-http")
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(
            f"http://127.0.0.1:{server.server_address[1]}"
        )
        foreign = client.approve_workflow(run.id, tenant_id="other")
        assert foreign.get("_http") == 404
        still = client.get_workflow(run.id, tenant_id="ap-http")
        assert still["run"]["status"] == WorkflowRunStatus.RUNNING.value
        listed = client.list_awaiting_workflow_approvals(tenant_id="ap-http")
        assert listed["count"] == 1
        ok = client.approve_workflow(
            run.id, tenant_id="ap-http", decided_by="ops", comment="ok"
        )
        assert ok["step"]["status"] == WorkflowStepStatus.COMPLETED.value
        assert ok["step"]["result"]["decision"] == "approved"
        j = _drive(queue, "ap-http")
        assert j.payload.get("step_name") == "ship"
        done = client.get_workflow(run.id, tenant_id="ap-http")
        assert done["run"]["status"] == WorkflowRunStatus.COMPLETED.value
    finally:
        server.shutdown()


def test_workflow_approval_simulated_load(queue: JobQueue):
    """Multi-tenant concurrent approve / reject / late timeout + reclaim."""
    tenants = [f"ap-load-{i:02d}" for i in range(6)]
    approve_n = 3
    reject_n = 2
    timeout_n = 2
    runs: list[tuple[str, object, str]] = []

    for tid in tenants:
        for n in range(approve_n):
            r = queue.start_workflow(
                tenant_id=tid,
                max_attempts=4,
                visibility_timeout_seconds=8,
                steps=_approval_steps(),
            )
            runs.append((tid, r.id, "approve"))
        for n in range(reject_n):
            r = queue.start_workflow(
                tenant_id=tid,
                max_attempts=4,
                steps=_approval_steps(),
            )
            runs.append((tid, r.id, "reject"))
        for n in range(timeout_n):
            r = queue.start_workflow(
                tenant_id=tid,
                max_attempts=4,
                steps=_approval_steps(timeout=1.5),
            )
            runs.append((tid, r.id, "timeout"))

    # Drive every reserve so gates park. Mix workers to simulate a fleet.
    for i, (tid, _rid, _kind) in enumerate(runs):
        j = queue.lease(tenant_id=tid, worker_id=f"w{i % 4}")
        assert j is not None
        assert j.payload.get("step_name") == "reserve"
        _ack(queue, j, f"w{i % 4}", result={"ok": True})

    # Simulated coordinator outage: gates stay parked, no timeout processor.
    time.sleep(0.05)
    for tid, rid, kind in runs:
        if kind == "timeout":
            continue
        steps = queue.list_workflow_steps(rid, tenant_id=tid)
        gate = next(s for s in steps if s.name == "gate")
        assert gate.status == WorkflowStepStatus.AWAITING_APPROVAL.value
        assert gate.job_id is None

    errors: list[str] = []

    def decide(item: tuple[str, object, str]) -> None:
        tid, rid, kind = item
        try:
            if kind == "approve":
                queue.approve_workflow(rid, tenant_id=tid, decided_by="ops")
            elif kind == "reject":
                queue.reject_workflow(rid, tenant_id=tid, reason="no")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{tid}/{rid}/{kind}: {exc}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(decide, item) for item in runs if item[2] != "timeout"]
        for fut in as_completed(futs):
            fut.result()
    assert errors == []

    # Late timeout tick after the outage (crash-safe, once).
    time.sleep(1.7)
    timed = 0
    for tid in tenants:
        timed += queue.process_workflow_approval_timeouts(tenant_id=tid, limit=50)
    assert timed == len(tenants) * timeout_n
    for tid in tenants:
        assert queue.process_workflow_approval_timeouts(tenant_id=tid, limit=50) == 0

    # Drain remaining work (ship or compensate).
    deadline = time.time() + 20
    while time.time() < deadline:
        progressed = False
        for tid in tenants:
            j = queue.lease(tenant_id=tid, worker_id="drain")
            if j is None:
                continue
            progressed = True
            _ack(queue, j, "drain", result={"ok": True})
        if not progressed:
            statuses = [
                (queue.get_workflow(rid, tenant_id=tid) or type("R", (), {"status": ""})).status
                for tid, rid, _ in runs
            ]
            if all(s in ("completed", "failed", "cancelled") for s in statuses):
                break
        time.sleep(0.02)

    completed = failed = 0
    for tid, rid, kind in runs:
        got = queue.get_workflow(rid, tenant_id=tid)
        assert got is not None, f"missing {tid}/{rid}"
        steps = queue.list_workflow_steps(rid, tenant_id=tid)
        gate = next(s for s in steps if s.name == "gate")
        if kind == "approve":
            assert got.status == WorkflowRunStatus.COMPLETED.value, (
                f"{tid} {kind} {got.status}"
            )
            assert gate.status == WorkflowStepStatus.COMPLETED.value
            assert (gate.result or {}).get("decision") == "approved"
            completed += 1
        else:
            assert got.status == WorkflowRunStatus.FAILED.value, (
                f"{tid} {kind} {got.status} {got.fail_reason}"
            )
            assert gate.status == WorkflowStepStatus.FAILED.value
            want = "timeout" if kind == "timeout" else "rejected"
            assert (gate.result or {}).get("decision") == want
            failed += 1
        pending_fwd = [
            s.name
            for s in steps
            if s.direction == "forward" and s.status == WorkflowStepStatus.PENDING.value
        ]
        assert pending_fwd == [], f"{tid}/{rid} leftover pending {pending_fwd}"

    want_ok = len(tenants) * approve_n
    want_fail = len(tenants) * (reject_n + timeout_n)
    assert completed == want_ok
    assert failed == want_fail
