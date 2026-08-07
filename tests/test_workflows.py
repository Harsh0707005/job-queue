"""Multi-step saga workflows with reverse compensation."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack, _nack


def _drive_step(queue: JobQueue, tenant_id: str, *, fail: bool = False) -> None:
    j = queue.lease(tenant_id=tenant_id, worker_id="w")
    assert j is not None
    if fail:
        _nack(queue, j, "w", error="step boom", requeue=True)
    else:
        _ack(queue, j, "w", result={"ok": True, "name": j.payload.get("step_name")})


def test_workflow_happy_path_three_steps(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf1",
        name="checkout",
        max_attempts=1,
        steps=[
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve"},
                "compensate": {"kind": "effect", "payload": {"op": "release"}},
            },
            {
                "name": "charge",
                "kind": "effect",
                "payload": {"op": "charge"},
                "compensate": {"kind": "effect", "payload": {"op": "refund"}},
            },
            {
                "name": "ship",
                "kind": "effect",
                "payload": {"op": "ship"},
            },
        ],
    )
    assert run.status == WorkflowRunStatus.RUNNING.value
    for _ in range(3):
        _drive_step(queue, "wf1")
    done = queue.get_workflow(run.id, tenant_id="wf1")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf1")
    forwards = [s for s in steps if s.direction == "forward"]
    assert len(forwards) == 3
    assert [s.name for s in forwards] == ["reserve", "charge", "ship"]
    assert all(s.status == WorkflowStepStatus.COMPLETED.value for s in forwards)


def test_workflow_fail_compensates_reverse_order(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf2",
        max_attempts=1,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {
                "name": "b",
                "payload": {"op": "b"},
                "compensate": {"payload": {"op": "undo_b"}},
            },
            {
                "name": "c",
                "payload": {"op": "c"},
                "fail": True,
                "compensate": {"payload": {"op": "undo_c"}},
            },
        ],
    )
    _drive_step(queue, "wf2")  # a
    _drive_step(queue, "wf2")  # b
    _drive_step(queue, "wf2", fail=True)  # c fails → dead (max_attempts=1)

    # Compensate b then a (reverse).
    j1 = queue.lease(tenant_id="wf2", worker_id="w")
    assert j1 is not None
    assert j1.payload.get("direction") == "compensate"
    assert j1.payload.get("step_name") == "b"
    _ack(queue, j1, "w", result={"undid": "b"})

    j2 = queue.lease(tenant_id="wf2", worker_id="w")
    assert j2 is not None
    assert j2.payload.get("direction") == "compensate"
    assert j2.payload.get("step_name") == "a"
    _ack(queue, j2, "w", result={"undid": "a"})

    done = queue.get_workflow(run.id, tenant_id="wf2")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    assert done.fail_step_index == 2
    steps = queue.list_workflow_steps(run.id, tenant_id="wf2")
    comps = [
        s
        for s in steps
        if s.direction == "compensate"
        and s.status == WorkflowStepStatus.COMPENSATED.value
    ]
    # b and a compensated (c never completed). Reverse *execution* order
    # is b then a (asserted via lease order above); list is by step_index ASC.
    assert {s.name for s in comps} == {"a", "b"}
    by_done = sorted(comps, key=lambda s: s.completed_at or s.created_at)
    assert [s.name for s in by_done] == ["b", "a"]


def test_workflow_no_double_compensate_on_duplicate_terminal(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="wf3",
        max_attempts=1,
        steps=[
            {
                "name": "x",
                "payload": {"op": "x"},
                "compensate": {"payload": {"op": "undo_x"}},
            },
            {"name": "y", "payload": {"op": "y"}, "fail": True},
        ],
    )
    _drive_step(queue, "wf3")
    _drive_step(queue, "wf3", fail=True)
    jc = queue.lease(tenant_id="wf3", worker_id="w")
    assert jc is not None
    _ack(queue, jc, "w")
    steps1 = queue.list_workflow_steps(run.id, tenant_id="wf3")
    comps1 = [s for s in steps1 if s.direction == "compensate"]
    assert len(comps1) == 1
    assert comps1[0].status == WorkflowStepStatus.COMPENSATED.value
    # No more work.
    assert queue.lease(tenant_id="wf3", worker_id="w") is None
    done = queue.get_workflow(run.id, tenant_id="wf3")
    assert done is not None and done.status == WorkflowRunStatus.FAILED.value


def test_workflow_tenant_isolation(queue: JobQueue):
    r1 = queue.start_workflow(
        tenant_id="iso-a",
        max_attempts=1,
        steps=[{"name": "s", "payload": {}}],
    )
    with pytest.raises(ValueError, match="tenant"):
        queue.list_workflow_steps(r1.id, tenant_id="iso-b")
    assert queue.get_workflow(r1.id, tenant_id="iso-b") is None
    assert queue.get_workflow(r1.id, tenant_id="iso-a") is not None


def test_workflow_compensates_on_lease_expiry_dead(queue: JobQueue):
    """Worker crash / lease expiry that dead-letters a step must compensate."""
    import time

    run = queue.start_workflow(
        tenant_id="wf-exp",
        max_attempts=1,
        visibility_timeout_seconds=1,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {
                "name": "b",
                "payload": {"op": "b"},
                "compensate": {"payload": {"op": "undo_b"}},
            },
        ],
    )
    _drive_step(queue, "wf-exp")  # complete a
    # Lease b then abandon (crash simulation).
    jb = queue.lease(
        tenant_id="wf-exp",
        worker_id="doomed",
        visibility_timeout_seconds=1,
    )
    assert jb is not None
    assert jb.payload.get("step_name") == "b"
    time.sleep(1.3)
    # Next lease triggers dead-letter of exhausted expired lease + compensate.
    # With max_attempts=1, expired b becomes dead (not reclaimed).
    jc = queue.lease(tenant_id="wf-exp", worker_id="w2")
    assert jc is not None
    assert jc.payload.get("direction") == "compensate"
    assert jc.payload.get("step_name") == "a"
    _ack(queue, jc, "w2", result={"undid": "a"})
    done = queue.get_workflow(run.id, tenant_id="wf-exp")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-exp")
    forwards = {s.name: s for s in steps if s.direction == "forward"}
    assert forwards["a"].status == WorkflowStepStatus.COMPLETED.value
    assert forwards["b"].status == WorkflowStepStatus.FAILED.value
    comps = [
        s
        for s in steps
        if s.direction == "compensate"
        and s.status == WorkflowStepStatus.COMPENSATED.value
    ]
    assert [s.name for s in comps] == ["a"]


def test_workflow_enqueue_keeps_step_pending_until_lease(queue: JobQueue):
    """Enqueued compensate jobs stay pending; sweep must not redrive them."""
    run = queue.start_workflow(
        tenant_id="wf-pend",
        max_attempts=1,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {"name": "b", "payload": {"op": "b"}, "fail": True},
        ],
    )
    _drive_step(queue, "wf-pend")
    _drive_step(queue, "wf-pend", fail=True)
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-pend")
    comps = [s for s in steps if s.direction == "compensate"]
    assert len(comps) == 1
    assert comps[0].status == WorkflowStepStatus.PENDING.value
    assert comps[0].job_id is not None
    assert comps[0].retry_count == 0
    # Job is pending (not leased yet).
    j = queue.get(comps[0].job_id, tenant_id="wf-pend")
    assert j is not None and j.status == JobStatus.PENDING
    # Sweep must NOT treat this as stuck / redrive (would unique-conflict).
    n = queue.process_workflow_compensation(tenant_id="wf-pend", limit=20)
    assert n == 0
    steps2 = queue.list_workflow_steps(run.id, tenant_id="wf-pend")
    c2 = [s for s in steps2 if s.direction == "compensate"][0]
    assert c2.job_id == comps[0].job_id
    assert c2.retry_count == 0
    assert c2.status == WorkflowStepStatus.PENDING.value


def test_workflow_compensation_sweep_isolates_txn_per_repair(queue: JobQueue):
    """Each stuck compensate is repaired in its own txn (no all-or-nothing)."""
    # Two independent fail runs → two compensate jobs to force-dead without
    # inline redrive (simulate missed terminal hook / crash mid-sweep).
    runs = []
    for tid in ("wf-iso1", "wf-iso2"):
        r = queue.start_workflow(
            tenant_id=tid,
            max_attempts=1,
            max_compensate_retries=3,
            steps=[
                {
                    "name": "a",
                    "payload": {"op": "a"},
                    "compensate": {"payload": {"op": "undo_a"}},
                },
                {"name": "b", "payload": {"op": "b"}, "fail": True},
            ],
        )
        _drive_step(queue, tid)
        _drive_step(queue, tid, fail=True)
        runs.append((tid, r.id))

    # Force-dead the compensate jobs *without* running workflow terminal hooks,
    # leaving step_runs linked to dead jobs (stuck state for the sweeper).
    for tid, rid in runs:
        steps = queue.list_workflow_steps(rid, tenant_id=tid)
        comp = next(s for s in steps if s.direction == "compensate")
        assert comp.job_id is not None
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'dead',
                    leased_by = NULL,
                    lease_token = NULL,
                    completed_at = NOW(),
                    last_error = 'simulated kill',
                    updated_at = NOW()
                WHERE id = %s
                """,
                (comp.job_id,),
            )
            conn.execute(
                """
                UPDATE workflow_step_runs
                SET status = 'running', started_at = COALESCE(started_at, NOW())
                WHERE id = %s
                """,
                (comp.id,),
            )
            conn.commit()

    # Sweep must redrive both (separate transactions).
    n = queue.process_workflow_compensation(limit=20)
    assert n >= 2
    for tid, rid in runs:
        steps = queue.list_workflow_steps(rid, tenant_id=tid)
        comps = [s for s in steps if s.direction == "compensate"]
        assert comps
        assert comps[0].retry_count >= 1
        assert comps[0].job_id is not None
        j = queue.get(comps[0].job_id, tenant_id=tid)
        assert j is not None and j.status == JobStatus.PENDING
        # Second sweep is a no-op (healthy pending jobs).
    assert queue.process_workflow_compensation(limit=20) == 0


def test_workflow_parallel_happy_path_fanout_join(queue: JobQueue):
    """Parallel branches run concurrently then join into a later step."""
    run = queue.start_workflow(
        tenant_id="wf-par",
        max_attempts=1,
        steps=[
            {
                "name": "setup",
                "payload": {"op": "setup"},
                "compensate": {"payload": {"op": "undo_setup"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "name": "left",
                            "payload": {"op": "left"},
                            "compensate": {"payload": {"op": "undo_left"}},
                        }
                    ],
                    [
                        {
                            "name": "right",
                            "payload": {"op": "right"},
                            "compensate": {"payload": {"op": "undo_right"}},
                        }
                    ],
                ],
            },
            {
                "name": "join",
                "payload": {"op": "join"},
            },
        ],
    )
    # Root only: setup.
    j0 = queue.lease(tenant_id="wf-par", worker_id="w")
    assert j0 is not None and j0.payload.get("step_name") == "setup"
    _ack(queue, j0, "w", result={"ok": "setup"})

    # Fan-out: both branch roots ready.
    names: set[str] = set()
    j1 = queue.lease(tenant_id="wf-par", worker_id="w1")
    j2 = queue.lease(tenant_id="wf-par", worker_id="w2")
    assert j1 is not None and j2 is not None
    names.add(str(j1.payload.get("step_name")))
    names.add(str(j2.payload.get("step_name")))
    assert names == {"left", "right"}
    _ack(queue, j1, "w1", result={"ok": j1.payload.get("step_name")})
    _ack(queue, j2, "w2", result={"ok": j2.payload.get("step_name")})

    # Join after both branches complete.
    j3 = queue.lease(tenant_id="wf-par", worker_id="w")
    assert j3 is not None and j3.payload.get("step_name") == "join"
    _ack(queue, j3, "w", result={"ok": "join"})

    done = queue.get_workflow(run.id, tenant_id="wf-par")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    forwards = [
        s
        for s in queue.list_workflow_steps(run.id, tenant_id="wf-par")
        if s.direction == "forward"
    ]
    assert {s.name for s in forwards} == {"setup", "left", "right", "join"}
    assert all(s.status == WorkflowStepStatus.COMPLETED.value for s in forwards)
    # Expanded DAG keeps branch metadata on parallel leaves.
    assert any(s.get("parallel_group") == "fan" for s in done.steps_def)


def test_workflow_parallel_branch_fail_settles_sibling_then_compensates(
    queue: JobQueue,
):
    """One branch fails: in-flight sibling finishes, then reverse compensate.

    Workers are never force-killed mid-step; leased sibling work is allowed to
    complete, then undo runs for every completed forward step.
    """
    run = queue.start_workflow(
        tenant_id="wf-par-fail",
        max_attempts=1,
        steps=[
            {
                "name": "setup",
                "payload": {"op": "setup"},
                "compensate": {"payload": {"op": "undo_setup"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "name": "slow_ok",
                            "payload": {"op": "slow"},
                            "compensate": {"payload": {"op": "undo_slow"}},
                        }
                    ],
                    [
                        {
                            "name": "boom",
                            "payload": {"op": "boom"},
                            "fail": True,
                            "compensate": {"payload": {"op": "undo_boom"}},
                        }
                    ],
                ],
            },
            {
                "name": "join",
                "payload": {"op": "join"},
                "compensate": {"payload": {"op": "undo_join"}},
            },
        ],
    )
    # setup
    js = queue.lease(tenant_id="wf-par-fail", worker_id="w")
    assert js is not None
    _ack(queue, js, "w", result={"ok": "setup"})

    # Lease both branches (simulate concurrent multi-worker fleet).
    ja = queue.lease(tenant_id="wf-par-fail", worker_id="w-slow")
    jb = queue.lease(tenant_id="wf-par-fail", worker_id="w-boom")
    assert ja is not None and jb is not None
    by_name = {
        str(ja.payload.get("step_name")): (ja, "w-slow"),
        str(jb.payload.get("step_name")): (jb, "w-boom"),
    }
    assert set(by_name) == {"slow_ok", "boom"}

    # Fail boom first while slow_ok is still leased (worker mid-step).
    boom_job, boom_w = by_name["boom"]
    _nack(queue, boom_job, boom_w, error="branch boom", requeue=True)

    mid = queue.get_workflow(run.id, tenant_id="wf-par-fail")
    assert mid is not None
    assert mid.status == WorkflowRunStatus.COMPENSATING.value
    # Join must not have been enqueued.
    steps_mid = queue.list_workflow_steps(run.id, tenant_id="wf-par-fail")
    assert not any(s.name == "join" for s in steps_mid if s.direction == "forward")

    # Sibling still holds lease — no compensate yet (settle wait).
    # Complete slow_ok successfully (worker finishes naturally).
    slow_job, slow_w = by_name["slow_ok"]
    _ack(queue, slow_job, slow_w, result={"ok": "slow"})

    # Now reverse compensate: highest completed index first (slow_ok then setup).
    c1 = queue.lease(tenant_id="wf-par-fail", worker_id="w")
    assert c1 is not None
    assert c1.payload.get("direction") == "compensate"
    assert c1.payload.get("step_name") == "slow_ok"
    _ack(queue, c1, "w", result={"undid": "slow"})

    c2 = queue.lease(tenant_id="wf-par-fail", worker_id="w")
    assert c2 is not None
    assert c2.payload.get("direction") == "compensate"
    assert c2.payload.get("step_name") == "setup"
    _ack(queue, c2, "w", result={"undid": "setup"})

    done = queue.get_workflow(run.id, tenant_id="wf-par-fail")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-par-fail")
    forwards = {s.name: s for s in steps if s.direction == "forward"}
    assert forwards["setup"].status == WorkflowStepStatus.COMPLETED.value
    assert forwards["slow_ok"].status == WorkflowStepStatus.COMPLETED.value
    assert forwards["boom"].status == WorkflowStepStatus.FAILED.value
    assert "join" not in forwards
    comps = [
        s.name
        for s in steps
        if s.direction == "compensate"
        and s.status == WorkflowStepStatus.COMPENSATED.value
    ]
    assert set(comps) == {"setup", "slow_ok"}


def test_workflow_parallel_branch_fail_cancels_unstarted_sibling(queue: JobQueue):
    """Pending (not leased) sibling is skipped; no mid-step kill required."""
    run = queue.start_workflow(
        tenant_id="wf-par-skip",
        max_attempts=1,
        steps=[
            {
                "name": "setup",
                "payload": {"op": "setup"},
                "compensate": {"payload": {"op": "undo_setup"}},
            },
            {
                "type": "parallel",
                "name": "fan",
                "branches": [
                    [
                        {
                            "name": "keep",
                            "payload": {"op": "keep"},
                            "compensate": {"payload": {"op": "undo_keep"}},
                        }
                    ],
                    [
                        {
                            "name": "fail_me",
                            "payload": {"op": "x"},
                            "fail": True,
                        }
                    ],
                ],
            },
        ],
    )
    _drive_step(queue, "wf-par-skip")  # setup
    # Lease only the failing branch; leave sibling pending in the queue.
    j = queue.lease(tenant_id="wf-par-skip", worker_id="w")
    assert j is not None
    # Prefer failing the boom step; if we got keep first, ack and lease again.
    if j.payload.get("step_name") == "keep":
        _ack(queue, j, "w", result={"ok": True})
        j = queue.lease(tenant_id="wf-par-skip", worker_id="w")
        assert j is not None
    assert j.payload.get("step_name") == "fail_me"
    _nack(queue, j, "w", error="boom", requeue=True)

    steps = queue.list_workflow_steps(run.id, tenant_id="wf-par-skip")
    forwards = {s.name: s for s in steps if s.direction == "forward"}
    # Sibling either completed (if we acked keep first) or skipped if still pending.
    assert forwards["fail_me"].status == WorkflowStepStatus.FAILED.value
    if "keep" in forwards and forwards["keep"].status == WorkflowStepStatus.SKIPPED.value:
        # Unstarted path: only setup needs compensate (if defined and completed).
        c = queue.lease(tenant_id="wf-par-skip", worker_id="w")
        assert c is not None
        assert c.payload.get("step_name") == "setup"
        _ack(queue, c, "w")
    else:
        # keep completed before fail: compensate keep then setup.
        for expected in ("keep", "setup"):
            c = queue.lease(tenant_id="wf-par-skip", worker_id="w")
            assert c is not None
            assert c.payload.get("direction") == "compensate"
            assert c.payload.get("step_name") == expected
            _ack(queue, c, "w")

    done = queue.get_workflow(run.id, tenant_id="wf-par-skip")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value


def test_workflow_compensation_does_not_redrive_healthy_pending_job(queue: JobQueue):
    """Even if step_run is wrongly marked running, pending job is not redriven."""
    run = queue.start_workflow(
        tenant_id="wf-healthy",
        max_attempts=1,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {"name": "b", "payload": {"op": "b"}, "fail": True},
        ],
    )
    _drive_step(queue, "wf-healthy")
    _drive_step(queue, "wf-healthy", fail=True)
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-healthy")
    comp = next(s for s in steps if s.direction == "compensate")
    # Simulate old buggy state: step marked running while job still pending.
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = 'running', started_at = NOW()
            WHERE id = %s
            """,
            (comp.id,),
        )
        conn.commit()
    n = queue.process_workflow_compensation(tenant_id="wf-healthy", limit=10)
    assert n == 0
    steps2 = queue.list_workflow_steps(run.id, tenant_id="wf-healthy")
    c2 = next(s for s in steps2 if s.direction == "compensate")
    assert c2.job_id == comp.job_id
    assert c2.retry_count == 0
    # Lease flips to running for real.
    j = queue.lease(tenant_id="wf-healthy", worker_id="w")
    assert j is not None and j.id == comp.job_id
    steps3 = queue.list_workflow_steps(run.id, tenant_id="wf-healthy")
    c3 = next(s for s in steps3 if s.direction == "compensate")
    assert c3.status == WorkflowStepStatus.RUNNING.value
    _ack(queue, j, "w")


def test_workflow_compensate_redrive_after_kill(queue: JobQueue):
    """Killed compensate worker must not leave the run stuck compensating."""
    import time

    run = queue.start_workflow(
        tenant_id="wf-comp-kill",
        max_attempts=1,
        visibility_timeout_seconds=1,
        max_compensate_retries=3,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {
                "name": "b",
                "payload": {"op": "b"},
                "compensate": {"payload": {"op": "undo_b"}},
            },
            {"name": "c", "payload": {"op": "c"}, "fail": True},
        ],
    )
    _drive_step(queue, "wf-comp-kill")  # a
    _drive_step(queue, "wf-comp-kill")  # b
    _drive_step(queue, "wf-comp-kill", fail=True)  # c → compensating

    # Lease first compensate (b) and crash (abandon lease).
    jc = queue.lease(
        tenant_id="wf-comp-kill",
        worker_id="comp-doomed",
        visibility_timeout_seconds=1,
    )
    assert jc is not None
    assert jc.payload.get("direction") == "compensate"
    assert jc.payload.get("step_name") == "b"
    time.sleep(1.3)
    # Dead-letter + redrive creates a fresh compensate job (same idempotency).
    n = queue.process_workflow_compensation(tenant_id="wf-comp-kill", limit=20)
    assert n >= 0
    # Either process redrive or lease maintenance path enqueued a new job.
    j2 = queue.lease(tenant_id="wf-comp-kill", worker_id="comp-ok")
    if j2 is None:
        queue.process_workflow_compensation(tenant_id="wf-comp-kill", limit=20)
        j2 = queue.lease(tenant_id="wf-comp-kill", worker_id="comp-ok")
    assert j2 is not None
    assert j2.payload.get("direction") == "compensate"
    assert j2.payload.get("step_name") == "b"
    assert j2.payload.get("idempotency_key") == jc.payload.get("idempotency_key")
    _ack(queue, j2, "comp-ok", result={"undid": "b"})

    # Next: compensate a.
    j3 = queue.lease(tenant_id="wf-comp-kill", worker_id="comp-ok")
    assert j3 is not None
    assert j3.payload.get("step_name") == "a"
    _ack(queue, j3, "comp-ok", result={"undid": "a"})

    done = queue.get_workflow(run.id, tenant_id="wf-comp-kill")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="wf-comp-kill")
    comps = {
        s.name: s
        for s in steps
        if s.direction == "compensate"
        and s.status == WorkflowStepStatus.COMPENSATED.value
    }
    assert set(comps) == {"a", "b"}
    # At least one redrive was recorded on b.
    b_steps = [s for s in steps if s.direction == "compensate" and s.name == "b"]
    assert b_steps and b_steps[0].retry_count >= 1


def test_workflow_compensates_on_max_runtime(queue: JobQueue):
    """Max-runtime force-release to dead must start reverse compensation."""
    import time

    run = queue.start_workflow(
        tenant_id="wf-rt",
        max_attempts=1,
        visibility_timeout_seconds=30,
        steps=[
            {
                "name": "a",
                "payload": {"op": "a"},
                "compensate": {"payload": {"op": "undo_a"}},
            },
            {
                "name": "b",
                "payload": {"op": "b"},
                "compensate": {"payload": {"op": "undo_b"}},
            },
        ],
    )
    # Override max_runtime on step jobs via direct lease after first step.
    _drive_step(queue, "wf-rt")
    jb = queue.lease(tenant_id="wf-rt", worker_id="slow")
    assert jb is not None
    # Force short max runtime and expired lease_started via SQL.
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET max_runtime_seconds = 1,
                lease_started_at = NOW() - INTERVAL '5 seconds',
                lease_expires_at = NOW() + INTERVAL '60 seconds'
            WHERE id = %s
            """,
            (jb.id,),
        )
        conn.commit()
    # Heartbeat / ack path runs max-runtime check.
    from job_queue.queue import LeaseError

    with pytest.raises(LeaseError):
        queue.heartbeat(
            jb.id,
            tenant_id="wf-rt",
            worker_id="slow",
            lease_token=jb.lease_token,
        )
    # Compensating job for a should be claimable.
    time.sleep(0.05)
    jc = queue.lease(tenant_id="wf-rt", worker_id="w2")
    assert jc is not None, "expected compensate job after max-runtime dead"
    assert jc.payload.get("direction") == "compensate"
    assert jc.payload.get("step_name") == "a"
    _ack(queue, jc, "w2")
    done = queue.get_workflow(run.id, tenant_id="wf-rt")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
