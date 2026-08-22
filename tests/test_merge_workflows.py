"""Merge moves workflow runs and reseals schedule generations under dest."""

from __future__ import annotations

import pytest

from job_queue import JobQueue, JobStatus, ScheduleError
from job_queue.crypto import schedule_generation_aad
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack
from tests.test_schedule_dag import _make_due
from tests.test_schedule_join import _pipe


def _drive(queue: JobQueue, tenant_id: str, worker: str = "w"):
    j = queue.lease(tenant_id=tenant_id, worker_id=worker)
    assert j is not None
    _ack(queue, j, worker, result={"ok": True, "name": j.payload.get("step_name")})
    return j


def test_merge_moves_live_workflow_run_and_ack_still_works(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="alpha",
        max_attempts=1,
        steps=[
            {
                "name": "reserve",
                "payload": {"op": "reserve"},
                "compensate": {"payload": {"op": "release"}},
            },
            {"name": "ship", "payload": {"op": "ship"}},
        ],
    )
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None
    assert leased.payload.get("step_name") == "reserve"

    result = queue.merge_tenant("alpha", "beta")
    assert result.workflow_runs_moved == 1
    assert queue.get_workflow(run.id, tenant_id="alpha") is None
    moved = queue.get_workflow(run.id, tenant_id="beta")
    assert moved is not None
    assert moved.tenant_id == "beta"
    assert moved.status == WorkflowRunStatus.RUNNING.value
    assert queue.tenant_is_empty("alpha") is True

    acked = queue.ack(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"ok": True},
    )
    assert acked.tenant_id == "beta"
    nxt = queue.lease(tenant_id="beta", worker_id="w2")
    assert nxt is not None
    assert nxt.payload.get("step_name") == "ship"
    _ack(queue, nxt, "w2")
    done = queue.get_workflow(run.id, tenant_id="beta")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value


def test_merge_moves_wait_signal_run_and_dest_can_send(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="alpha",
        max_attempts=1,
        steps=[
            {"name": "gate", "kind": "wait_signal", "signal": "go"},
            {"name": "work", "payload": {"op": "work"}},
        ],
    )
    queue.send_workflow_signal(
        run.id, tenant_id="alpha", name="early", payload={"too": "soon"}
    )
    result = queue.merge_tenant("alpha", "beta")
    assert result.workflow_runs_moved == 1
    assert queue.get_workflow(run.id, tenant_id="alpha") is None
    with pytest.raises(ValueError, match="not found"):
        queue.send_workflow_signal(run.id, tenant_id="alpha", name="go")
    out = queue.send_workflow_signal(
        run.id, tenant_id="beta", name="go", payload={"who": "dest"}
    )
    assert out["created"] is True
    j = _drive(queue, "beta")
    assert j.payload.get("step_name") == "work"
    done = queue.get_workflow(run.id, tenant_id="beta")
    assert done is not None and done.status == WorkflowRunStatus.COMPLETED.value
    listed = queue.list_workflow_signals(run.id, tenant_id="beta")
    names = {s.name for s in listed}
    assert names == {"early", "go"}


def test_merge_reseals_generation_so_dest_join_attaches(queue: JobQueue):
    up, w1, w2, join = _pipe(queue, tenant_id="alpha")
    fire = queue.fire_schedule(up.id, tenant_id="alpha")
    job = queue.lease(tenant_id="alpha", worker_id="w")
    assert job is not None and job.id == fire.job.id
    _ack(queue, job, "w", result={"secret": "from-alpha", "n": 1})

    result = queue.merge_tenant("alpha", "beta")
    assert result.schedules_moved == 4
    assert result.generations_resealed >= 1
    assert queue.get_schedule(up.id, tenant_id="alpha") is None
    assert queue.get_schedule(up.id, tenant_id="beta") is not None

    with queue._pool.connection() as conn:
        src_left = conn.execute(
            "SELECT count(*)::int AS n FROM schedule_generations WHERE tenant_id = %s",
            ("alpha",),
        ).fetchone()
        assert int(src_left["n"]) == 0
        dest_gen = conn.execute(
            """
            SELECT tenant_id, payload, payload_key_id, status
            FROM schedule_generations
            WHERE schedule_id = %s AND iteration = 1
            """,
            (up.id,),
        ).fetchone()
        assert dest_gen is not None
        assert dest_gen["tenant_id"] == "beta"
        assert dest_gen["status"] == "completed"
        dest_key = conn.execute(
            """
            SELECT key_id FROM tenant_payload_keys
            WHERE tenant_id = %s AND status = 'active'
            """,
            ("beta",),
        ).fetchone()
        assert dest_key is not None
        assert dest_gen["payload_key_id"] == dest_key["key_id"]
        opened = queue._open_dict(
            dest_gen["payload"],
            tenant_id="beta",
            aad=schedule_generation_aad(up.id, 1, tenant_id="beta"),
            conn=conn,
        )
        assert (opened.get("result") or {}).get("secret") == "from-alpha"
        with pytest.raises(Exception):
            queue._open_dict(
                dest_gen["payload"],
                tenant_id="beta",
                aad=schedule_generation_aad(up.id, 1, tenant_id="alpha"),
                conn=conn,
            )
        assert (
            queue._open_schedule_generation(
                conn, tenant_id="alpha", schedule_id=up.id, iteration=1
            )
            is None
        )

    _make_due(queue, w1.id)
    _make_due(queue, w2.id)
    fired = queue.process_due_schedules(tenant_id="beta")
    waiters = [f.job for f in fired if f.schedule.id in {w1.id, w2.id}]
    assert len(waiters) == 2
    for wj in waiters:
        assert (wj.payload.get("upstream") or {}).get("result") == {
            "secret": "from-alpha",
            "n": 1,
        }
    with pytest.raises(ScheduleError):
        queue.fire_schedule(w1.id, tenant_id="alpha")


def test_merge_mid_tick_completes_generation_under_dest(queue: JobQueue):
    up, w1, _w2, _join = _pipe(queue, tenant_id="alpha")
    fire = queue.fire_schedule(up.id, tenant_id="alpha")
    leased = queue.lease(tenant_id="alpha", worker_id="w1")
    assert leased is not None and leased.id == fire.job.id
    queue.merge_tenant("alpha", "beta")
    acked = queue.ack(
        leased.id,
        tenant_id="alpha",
        worker_id="w1",
        lease_token=leased.lease_token,
        result={"secret": "late-seal", "n": 1},
    )
    assert acked.status == JobStatus.COMPLETED
    assert acked.tenant_id == "beta"
    _make_due(queue, w1.id)
    fired = queue.process_due_schedules(tenant_id="beta")
    waiters = [f for f in fired if f.schedule.id == w1.id]
    assert len(waiters) == 1
    assert (waiters[0].job.payload.get("upstream") or {}).get("result") == {
        "secret": "late-seal",
        "n": 1,
    }


def test_merge_does_not_let_dest_join_see_foreign_or_other_tenant(queue: JobQueue):
    dest_up, dest_w1, dest_w2, dest_join = _pipe(queue, tenant_id="beta")
    queue.fire_schedule(dest_up.id, tenant_id="beta")
    dj = queue.lease(tenant_id="beta", worker_id="wd")
    assert dj is not None
    _ack(queue, dj, "wd", result={"secret": "dest-only", "n": 1})

    src_up, src_w1, src_w2, src_join = _pipe(queue, tenant_id="alpha")
    queue.fire_schedule(src_up.id, tenant_id="alpha")
    sj = queue.lease(tenant_id="alpha", worker_id="ws")
    assert sj is not None
    _ack(queue, sj, "ws", result={"secret": "src-only", "n": 1})

    other_up, other_w1, _ow2, other_join = _pipe(queue, tenant_id="gamma")
    queue.fire_schedule(other_up.id, tenant_id="gamma")
    oj = queue.lease(tenant_id="gamma", worker_id="wg")
    assert oj is not None
    _ack(queue, oj, "wg", result={"secret": "gamma-only", "n": 1})

    queue.merge_tenant("alpha", "beta")

    _make_due(queue, dest_w1.id)
    dest_fired = queue.process_due_schedules(tenant_id="beta")
    dest_waiters = [f.job for f in dest_fired if f.schedule.id in {dest_w1.id, dest_w2.id}]
    assert dest_waiters
    for wj in dest_waiters:
        assert (wj.payload.get("upstream") or {}).get("result") == {
            "secret": "dest-only",
            "n": 1,
        }
        assert (wj.payload.get("upstream") or {}).get("result") != {
            "secret": "src-only",
            "n": 1,
        }

    _make_due(queue, src_w1.id)
    _make_due(queue, src_w2.id)
    src_fired = queue.process_due_schedules(tenant_id="beta")
    src_waiters = [f.job for f in src_fired if f.schedule.id in {src_w1.id, src_w2.id}]
    assert len(src_waiters) == 2
    for wj in src_waiters:
        assert (wj.payload.get("upstream") or {}).get("result") == {
            "secret": "src-only",
            "n": 1,
        }

    _make_due(queue, other_w1.id)
    other_fired = queue.process_due_schedules(tenant_id="gamma")
    other_waiters = [f.job for f in other_fired if f.schedule.id == other_w1.id]
    assert len(other_waiters) == 1
    assert (other_waiters[0].payload.get("upstream") or {}).get("result") == {
        "secret": "gamma-only",
        "n": 1,
    }
    with pytest.raises(ScheduleError):
        queue.fire_schedule(other_join.id, tenant_id="beta")
    _ = dest_join
    _ = src_join
    _ = WorkflowStepStatus
