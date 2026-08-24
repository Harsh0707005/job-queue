"""Dynamic workflow fan-out: map a step result into N child steps."""

from __future__ import annotations

from job_queue import JobQueue, JobStatus
from job_queue.models import WorkflowRunStatus, WorkflowStepStatus
from tests.helpers_queue import _ack, _nack


def test_map_spawns_children_then_joins(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-ok",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"op": "list"}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "step": {
                    "name": "work",
                    "payload": {"op": "work"},
                    "compensate": {"payload": {"op": "undo"}},
                },
            },
            {"name": "done", "payload": {"op": "done"}},
        ],
    )
    j0 = queue.lease(tenant_id="map-ok", worker_id="w")
    assert j0 is not None and j0.payload.get("step_name") == "list"
    _ack(queue, j0, "w", result={"ids": ["a", "b", "c"]})

    names: list[str] = []
    items: list[object] = []
    for wid in ("w1", "w2", "w3"):
        j = queue.lease(tenant_id="map-ok", worker_id=wid)
        assert j is not None
        names.append(str(j.payload.get("step_name")))
        step = j.payload.get("step") or {}
        items.append((step.get("payload") or {}).get("item"))
        _ack(queue, j, wid, result={"item": (step.get("payload") or {}).get("item")})
    assert sorted(names) == ["work-0", "work-1", "work-2"]
    assert sorted(items) == ["a", "b", "c"]

    jn = queue.lease(tenant_id="map-ok", worker_id="w")
    assert jn is not None and jn.payload.get("step_name") == "done"
    _ack(queue, jn, "w", result={"ok": "done"})

    done = queue.get_workflow(run.id, tenant_id="map-ok")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
    steps = queue.list_workflow_steps(run.id, tenant_id="map-ok")
    forwards = [s for s in steps if s.direction == "forward"]
    assert {s.name for s in forwards} == {
        "list",
        "each",
        "work-0",
        "work-1",
        "work-2",
        "done",
    }
    assert all(s.status == WorkflowStepStatus.COMPLETED.value for s in forwards)
    join = next(s for s in forwards if s.name == "each")
    assert join.result is not None
    assert join.result.get("count") == 3


def test_map_empty_items_skips_to_join(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-empty",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"op": "list"}},
            {
                "type": "foreach",
                "name": "each",
                "items": "ids",
                "step": {"name": "work", "payload": {"op": "work"}},
            },
            {"name": "done", "payload": {"op": "done"}},
        ],
    )
    j0 = queue.lease(tenant_id="map-empty", worker_id="w")
    assert j0 is not None
    _ack(queue, j0, "w", result={"ids": []})
    jn = queue.lease(tenant_id="map-empty", worker_id="w")
    assert jn is not None and jn.payload.get("step_name") == "done"
    _ack(queue, jn, "w", result={"ok": True})
    done = queue.get_workflow(run.id, tenant_id="map-empty")
    assert done is not None and done.status == WorkflowRunStatus.COMPLETED.value
    names = [
        s.name
        for s in queue.list_workflow_steps(run.id, tenant_id="map-empty")
        if s.direction == "forward"
    ]
    assert "work-0" not in names
    assert "each" in names


def test_map_child_fail_skips_unstarted_and_compensates(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-fail",
        max_attempts=1,
        steps=[
            {
                "name": "list",
                "payload": {"op": "list"},
                "compensate": {"payload": {"op": "undo_list"}},
            },
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "step": {
                    "name": "work",
                    "payload": {"op": "work"},
                    "compensate": {"payload": {"op": "undo_work"}},
                },
            },
            {"name": "done", "payload": {"op": "done"}},
        ],
    )
    j0 = queue.lease(tenant_id="map-fail", worker_id="w")
    assert j0 is not None
    _ack(queue, j0, "w", result={"ids": [1, 2, 3, 4]})

    kids = []
    for i in range(4):
        j = queue.lease(tenant_id="map-fail", worker_id=f"w{i}")
        assert j is not None
        kids.append((j, f"w{i}"))
    # Complete one, fail one, leave two leased.
    _ack(queue, kids[0][0], kids[0][1], result={"item": 1})
    _nack(queue, kids[1][0], kids[1][1], error="boom", requeue=True)

    mid = queue.get_workflow(run.id, tenant_id="map-fail")
    assert mid is not None
    assert mid.status == WorkflowRunStatus.COMPENSATING.value
    steps_mid = queue.list_workflow_steps(run.id, tenant_id="map-fail")
    assert not any(s.name == "done" for s in steps_mid if s.direction == "forward")

    # In-flight siblings finish naturally.
    _ack(queue, kids[2][0], kids[2][1], result={"item": 3})
    _ack(queue, kids[3][0], kids[3][1], result={"item": 4})

    undid: list[str] = []
    while True:
        jc = queue.lease(tenant_id="map-fail", worker_id="wc")
        if jc is None:
            break
        assert jc.payload.get("direction") == "compensate"
        undid.append(str(jc.payload.get("step_name")))
        _ack(queue, jc, "wc", result={"undid": jc.payload.get("step_name")})

    done = queue.get_workflow(run.id, tenant_id="map-fail")
    assert done is not None
    assert done.status == WorkflowRunStatus.FAILED.value
    # Completed forwards undo in reverse index order; failed child does not.
    assert "list" in undid
    assert any(n.startswith("work-") for n in undid)
    assert "done" not in undid


def test_map_cancel_mid_fanout_skips_pending(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-can",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"op": "list"}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "step": {"name": "work", "payload": {"op": "work"}},
            },
        ],
    )
    j0 = queue.lease(tenant_id="map-can", worker_id="w")
    assert j0 is not None
    _ack(queue, j0, "w", result={"ids": [1, 2, 3]})
    held = queue.lease(tenant_id="map-can", worker_id="hold")
    assert held is not None
    queue.cancel_workflow(
        run.id,
        tenant_id="map-can",
        release_in_flight=False,
        compensate=False,
    )
    # Pending children skipped; leased one still held.
    still = queue.get(held.id, tenant_id="map-can")
    assert still is not None and still.status == JobStatus.LEASED
    _ack(queue, held, "hold", result={"ok": True})
    done = queue.get_workflow(run.id, tenant_id="map-can")
    assert done is not None
    assert done.status == WorkflowRunStatus.CANCELLED.value
    forwards = [
        s
        for s in queue.list_workflow_steps(run.id, tenant_id="map-can")
        if s.direction == "forward"
    ]
    skipped = [s for s in forwards if s.status == WorkflowStepStatus.SKIPPED.value]
    assert skipped
    assert queue.lease(tenant_id="map-can", worker_id="w") is None


def test_map_overflow_fails_closed_without_spawning(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-big",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"op": "list"}},
            {
                "type": "map",
                "name": "each",
                "items": "ids",
                "max_items": 3,
                "step": {"name": "work", "payload": {"op": "work"}},
            },
            {"name": "done", "payload": {"op": "done"}},
        ],
    )
    j0 = queue.lease(tenant_id="map-big", worker_id="w")
    assert j0 is not None
    _ack(queue, j0, "w", result={"ids": list(range(50))})
    assert queue.lease(tenant_id="map-big", worker_id="w") is None
    done = queue.get_workflow(run.id, tenant_id="map-big")
    assert done is not None
    assert done.status in (
        WorkflowRunStatus.FAILED.value,
        WorkflowRunStatus.COMPENSATING.value,
    )
    names = [
        s.name
        for s in queue.list_workflow_steps(run.id, tenant_id="map-big")
        if s.direction == "forward"
    ]
    assert not any(n.startswith("work-") for n in names)
    join = next(
        s
        for s in queue.list_workflow_steps(run.id, tenant_id="map-big")
        if s.name == "each"
    )
    assert join.status == WorkflowStepStatus.FAILED.value
    assert "max_items" in (join.error or "")


def test_map_result_list_without_path_key(queue: JobQueue):
    run = queue.start_workflow(
        tenant_id="map-list",
        max_attempts=1,
        steps=[
            {"name": "list", "payload": {"op": "list"}},
            {
                "type": "fanout",
                "name": "each",
                "items": "items",
                "step": {"name": "work", "payload": {"op": "work"}},
            },
        ],
    )
    j0 = queue.lease(tenant_id="map-list", worker_id="w")
    assert j0 is not None
    _ack(queue, j0, "w", result=["x", "y"])
    seen = set()
    for _ in range(2):
        j = queue.lease(tenant_id="map-list", worker_id="w")
        assert j is not None
        seen.add(str(j.payload.get("step_name")))
        _ack(queue, j, "w", result={"ok": True})
    assert seen == {"work-0", "work-1"}
    done = queue.get_workflow(run.id, tenant_id="map-list")
    assert done is not None
    assert done.status == WorkflowRunStatus.COMPLETED.value
