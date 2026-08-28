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


def test_slow_lease_to_start_does_not_deadletter_before_activity_timeout(
    queue: JobQueue,
):
    """max_runtime must not fire on lease time before the activity starts."""
    from job_queue.queue._common import _utcnow

    run = queue.start_workflow(
        tenant_id="act-slow-start",
        max_attempts=1,
        steps=_activity_steps(timeout=2, heartbeat=10),
    )
    _drive_effect(queue, "act-slow-start")
    job = queue.lease(tenant_id="act-slow-start", worker_id="w")
    assert job is not None
    # Enqueue used to set max_runtime = timeout+2 from lease_started_at.
    assert job.max_runtime_seconds >= 60
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET lease_started_at = NOW() - INTERVAL '20 seconds'
            WHERE id = %s
            """,
            (job.id,),
        )
        conn.commit()
    # Heartbeat would have dead-lettered under the old timeout+2 pin.
    alive = queue.heartbeat(
        job.id,
        tenant_id="act-slow-start",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert alive.status == JobStatus.LEASED
    started = queue.start_workflow_activity(
        job.id,
        tenant_id="act-slow-start",
        worker_id="w",
        lease_token=job.lease_token,
    )
    assert started.status == "running"
    refreshed = queue.get(job.id, tenant_id="act-slow-start")
    assert refreshed is not None
    hard = refreshed.lease_started_at
    assert hard is not None
    remaining = (
        hard.timestamp()
        + refreshed.max_runtime_seconds
        - _utcnow().timestamp()
    )
    assert remaining >= started.timeout_seconds
    queue.heartbeat(
        job.id,
        tenant_id="act-slow-start",
        worker_id="w",
        lease_token=job.lease_token,
    )
    done = queue.complete_workflow_activity(
        job.id,
        tenant_id="act-slow-start",
        worker_id="w",
        lease_token=job.lease_token,
        result={"ok": True},
    )
    _ack(queue, job, "w", result=done.result)
    _drive_effect(queue, "act-slow-start")
    fin = queue.get_workflow(run.id, tenant_id="act-slow-start")
    assert fin is not None
    assert fin.status == WorkflowRunStatus.COMPLETED.value


def test_activity_timeout_still_compensates_after_slow_start(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-slow-to",
        max_attempts=1,
        steps=_activity_steps(timeout=0.2, heartbeat=5),
    )
    _drive_effect(queue, "act-slow-to")
    job = queue.lease(tenant_id="act-slow-to", worker_id="w")
    assert job is not None
    with queue._pool.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET lease_started_at = NOW() - INTERVAL '15 seconds'
            WHERE id = %s
            """,
            (job.id,),
        )
        conn.commit()
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-slow-to",
        worker_id="w",
        lease_token=job.lease_token,
    )
    time.sleep(0.35)
    n = queue.process_workflow_activities(tenant_id="act-slow-to", limit=10)
    assert n >= 1
    mid = queue.get_workflow(run.id, tenant_id="act-slow-to")
    assert mid is not None
    assert mid.status == WorkflowRunStatus.COMPENSATING.value
    c = queue.lease(tenant_id="act-slow-to", worker_id="undo")
    assert c is not None
    assert c.payload.get("direction") == "compensate"
    _ack(queue, c, "undo")
    extra = queue.lease(tenant_id="act-slow-to", worker_id="undo")
    if extra is not None:
        _ack(queue, extra, "undo")
    failed = queue.get_workflow(run.id, tenant_id="act-slow-to")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value


def test_ack_without_completing_activity_is_refused(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-ack-early",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=5),
    )
    _drive_effect(queue, "act-ack-early")
    job = queue.lease(tenant_id="act-ack-early", worker_id="w")
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-ack-early",
        worker_id="w",
        lease_token=job.lease_token,
    )
    with pytest.raises(LeaseError, match="until the activity is completed"):
        _ack(queue, job, "w", result={"skipped": True})
    still = queue.get(job.id, tenant_id="act-ack-early")
    assert still is not None
    assert still.status == JobStatus.LEASED
    running = queue.get_workflow(run.id, tenant_id="act-ack-early")
    assert running is not None
    assert running.status == WorkflowRunStatus.RUNNING.value
    assert queue.lease(tenant_id="act-ack-early", worker_id="other") is None
    steps = queue.list_workflow_steps(run.id, tenant_id="act-ack-early")
    assert not any(s.name == "ship" for s in steps)


def test_ack_without_starting_activity_is_refused(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-ack-none",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=5),
    )
    _drive_effect(queue, "act-ack-none")
    job = queue.lease(tenant_id="act-ack-none", worker_id="w")
    assert job is not None
    with pytest.raises(LeaseError, match="not started"):
        _ack(queue, job, "w", result={"never": "started"})
    still = queue.get(job.id, tenant_id="act-ack-none")
    assert still is not None
    assert still.status == JobStatus.LEASED
    running = queue.get_workflow(run.id, tenant_id="act-ack-none")
    assert running is not None
    assert running.status == WorkflowRunStatus.RUNNING.value


def test_refused_ack_can_still_timeout_and_compensate(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="act-ack-to",
        max_attempts=1,
        steps=_activity_steps(timeout=30, heartbeat=0.2),
    )
    _drive_effect(queue, "act-ack-to")
    job = queue.lease(tenant_id="act-ack-to", worker_id="w")
    assert job is not None
    queue.start_workflow_activity(
        job.id,
        tenant_id="act-ack-to",
        worker_id="w",
        lease_token=job.lease_token,
    )
    with pytest.raises(LeaseError, match="until the activity is completed"):
        _ack(queue, job, "w")
    time.sleep(0.35)
    n = queue.process_workflow_activities(tenant_id="act-ack-to", limit=10)
    assert n >= 1
    act = queue.get_workflow_activity(job.id, tenant_id="act-ack-to")
    assert act is not None
    assert act.status == "timed_out"
    mid = queue.get_workflow(run.id, tenant_id="act-ack-to")
    assert mid is not None
    assert mid.status == WorkflowRunStatus.COMPENSATING.value
    c = queue.lease(tenant_id="act-ack-to", worker_id="undo")
    assert c is not None
    assert c.payload.get("direction") == "compensate"
    _ack(queue, c, "undo")
    extra = queue.lease(tenant_id="act-ack-to", worker_id="undo")
    if extra is not None:
        _ack(queue, extra, "undo")
    failed = queue.get_workflow(run.id, tenant_id="act-ack-to")
    assert failed is not None
    assert failed.status == WorkflowRunStatus.FAILED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="act-ack-to")
    assert not any(
        s.name == "ship" and s.status == WorkflowStepStatus.COMPLETED.value
        for s in steps
    )


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


def test_activity_runner_done_failed_is_a_failure():
    from job_queue.remote.worker_agent_workflow_work import activity_runner_failure

    assert activity_runner_failure(
        {"done": True, "status": "failed", "error": "charge declined"}
    ) == "charge declined"
    assert activity_runner_failure({"done": True, "status": "failed"}) == "activity failed"
    assert activity_runner_failure({"done": True, "status": "completed"}) is None
    assert activity_runner_failure({"done": True}) is None
    assert activity_runner_failure({"done": False, "error": "boom"}) == "boom"


def test_activity_runner_done_failed_fails_step_and_compensates(queue: JobQueue):
    """done=true + status=failed must not complete the activity or ship."""
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import urlparse

    from job_queue.coordinator import Coordinator, _make_handler
    from job_queue.remote import CoordinatorClient, RemoteWorkerAgent

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            return

        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length") or "0")
            self.rfile.read(n)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"accepted":true,"status":"running"}')

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/status":
                body = json.dumps(
                    {
                        "done": True,
                        "status": "failed",
                        "error": "charge declined",
                    }
                ).encode()
            else:
                body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    runner = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=runner.serve_forever, daemon=True).start()
    runner_url = f"http://127.0.0.1:{runner.server_address[1]}"

    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    coord_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        run = queue.start_workflow(
            tenant_id="act-done-fail",
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
                    "kind": "activity",
                    "activity": "charge",
                    "url": runner_url,
                    "timeout_seconds": 30,
                    "heartbeat_timeout_seconds": 10,
                    "payload": {"op": "charge", "fail": True, "work_ms": 0},
                    "compensate": {
                        "kind": "effect",
                        "payload": {"op": "undo_charge"},
                    },
                },
                {"name": "ship", "kind": "effect", "payload": {"op": "ship"}},
            ],
        )
        _drive_effect(queue, "act-done-fail")
        job = queue.lease(tenant_id="act-done-fail", worker_id="w")
        assert job is not None
        agent = RemoteWorkerAgent(
            CoordinatorClient(coord_url),
            worker_id="w",
            poll_interval_seconds=0.1,
        )
        with pytest.raises(RuntimeError, match="charge declined"):
            agent._do_workflow_activity(
                job,
                threading.Event(),
                job.payload,
                job.payload["step"],
                job.payload["step"].get("payload") or {},
            )
        act = queue.get_workflow_activity(job.id, tenant_id="act-done-fail")
        assert act is not None
        assert act.status == "failed"
        assert "declined" in (act.error or "")
        dead = queue.get(job.id, tenant_id="act-done-fail")
        assert dead is not None
        assert dead.status == JobStatus.DEAD
        mid = queue.get_workflow(run.id, tenant_id="act-done-fail")
        assert mid is not None
        assert mid.status == WorkflowRunStatus.COMPENSATING.value
        c = queue.lease(tenant_id="act-done-fail", worker_id="undo")
        assert c is not None
        assert c.payload.get("direction") == "compensate"
        assert c.payload.get("step_name") == "reserve"
        _ack(queue, c, "undo", result={"undid": True})
        extra = queue.lease(tenant_id="act-done-fail", worker_id="undo")
        if extra is not None:
            _ack(queue, extra, "undo")
        failed = queue.get_workflow(run.id, tenant_id="act-done-fail")
        assert failed is not None
        assert failed.status == WorkflowRunStatus.FAILED.value
        steps = queue.list_workflow_steps(run.id, tenant_id="act-done-fail")
        ship = next((s for s in steps if s.name == "ship"), None)
        assert ship is None or ship.status != WorkflowStepStatus.COMPLETED.value
        charge = next(s for s in steps if s.name == "charge")
        assert charge.status == WorkflowStepStatus.FAILED.value
    finally:
        server.shutdown()
        runner.shutdown()


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
