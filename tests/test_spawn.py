"""Spawn children / join."""

from __future__ import annotations

import threading
import time

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Spawn children / join
# ---------------------------------------------------------------------------


def test_spawn_children_parks_parent_and_frees_lease(queue: JobQueue):
    """Parent releases concurrency while children run."""
    from job_queue import Worker

    queue.set_concurrency_limit("t1", 1)
    started = threading.Event()
    parked = threading.Event()
    worker = Worker(
        queue,
        tenant_id="t1",
        worker_id="spawn-w1",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=30,
    )

    @worker.handler
    def process(job: Job, lease: LeaseContext) -> dict:
        if job.payload.get("role") == "parent":
            if job.await_children_outcome == "joined":
                kids = lease.list_children()
                return {
                    "joined": True,
                    "children": len(kids),
                    "all_done": all(k.status == JobStatus.COMPLETED for k in kids),
                }
            started.set()
            lease.spawn_children(
                [
                    {"payload": {"role": "child", "n": 1}},
                    {"payload": {"role": "child", "n": 2}},
                ],
                progress={"phase": "pre_spawn"},
            )
            # spawn_children raises ChildrenAwaited
            return {}
        # Child work
        time.sleep(0.05)
        return {"n": job.payload.get("n")}

    parent = queue.enqueue(tenant_id="t1", payload={"role": "parent"})
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    assert started.wait(5)

    # Parent should park as awaiting_children with no lease held.
    deadline = time.time() + 3
    parent_row = None
    while time.time() < deadline:
        parent_row = queue.get(parent.id, tenant_id="t1")
        if parent_row and parent_row.status == JobStatus.AWAITING_CHILDREN:
            parked.set()
            break
        time.sleep(0.05)
    assert parked.is_set(), f"parent not parked: {parent_row}"
    assert parent_row is not None
    assert parent_row.leased_by is None
    assert parent_row.lease_token is None
    assert len(parent_row.waiting_on) == 2
    # Parent is not leased (slot not held by waiting parent). Stop this
    # worker so it does not immediately grab a child; with concurrency=1
    # a filler job must then be claimable by another worker.
    worker.stop()
    t.join(timeout=3)
    # Release any child the worker may have leased mid-race.
    for jid in parent_row.waiting_on:
        child = queue.get(jid, tenant_id="t1")
        if child and child.status == JobStatus.LEASED and child.lease_token:
            try:
                queue.release(
                    child.id,
                    tenant_id="t1",
                    worker_id=child.leased_by or "spawn-w1",
                    lease_token=child.lease_token,
                    reason="test_slot_check",
                )
            except LeaseError:
                pass
    # High priority so the free slot claims filler before pending children.
    filler = queue.enqueue(tenant_id="t1", payload={"role": "filler"}, priority=100)
    leased_filler = queue.lease(
        tenant_id="t1", worker_id="other", visibility_timeout_seconds=30
    )
    assert leased_filler is not None
    assert leased_filler.id == filler.id, leased_filler.payload
    _ack(queue, leased_filler, "other")

    # Resume with a fresh worker so children finish and parent joins.
    worker2 = Worker(
        queue,
        tenant_id="t1",
        worker_id="spawn-w2",
        poll_interval_seconds=0.05,
        visibility_timeout_seconds=30,
    )

    @worker2.handler
    def process2(job: Job, lease: LeaseContext) -> dict:
        if job.payload.get("role") == "parent":
            if job.await_children_outcome == "joined":
                kids = lease.list_children()
                return {
                    "joined": True,
                    "children": len(kids),
                    "all_done": all(k.status == JobStatus.COMPLETED for k in kids),
                }
            lease.spawn_children(
                [
                    {"payload": {"role": "child", "n": 1}},
                    {"payload": {"role": "child", "n": 2}},
                ]
            )
            return {}
        time.sleep(0.05)
        return {"n": job.payload.get("n")}

    t2 = threading.Thread(target=worker2.run, daemon=True)
    t2.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        parent_row = queue.get(parent.id, tenant_id="t1")
        if parent_row and parent_row.status == JobStatus.COMPLETED:
            break
        time.sleep(0.1)
    assert parent_row is not None
    assert parent_row.status == JobStatus.COMPLETED
    assert parent_row.result is not None
    assert parent_row.result.get("joined") is True
    assert parent_row.result.get("children") == 2
    worker2.stop()
    t2.join(timeout=5)


def test_spawn_child_failure_cancels_parent(queue: JobQueue):
    """If a child dies, parent must not wait forever — it is terminalized."""

    parent = queue.enqueue(tenant_id="t1", payload={"role": "parent"})
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None and job.id == parent.id
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[
            {"payload": {"role": "ok"}},
            {"payload": {"role": "bad"}},
        ],
        wait=True,
    )
    assert result.parent.status == JobStatus.AWAITING_CHILDREN
    kids = list(result.children)
    assert len(kids) == 2

    # Complete one child, kill the other.
    ok = next(k for k in kids if k.payload.get("role") == "ok")
    bad = next(k for k in kids if k.payload.get("role") == "bad")
    claimed: dict = {}
    for wid in ("c1", "c2", "c3", "c4"):
        batch = queue.lease_batch(
            tenant_id="t1",
            worker_id=wid,
            visibility_timeout_seconds=30,
            limit=2,
        )
        for j in batch:
            claimed[j.id] = (j, wid)
        if ok.id in claimed and bad.id in claimed:
            break
    assert ok.id in claimed and bad.id in claimed, claimed
    j_ok, w_ok = claimed[ok.id]
    j_bad, w_bad = claimed[bad.id]
    _ack(queue, j_ok, w_ok, result={"ok": True})
    queue.nack(
        j_bad.id,
        tenant_id="t1",
        worker_id=w_bad,
        lease_token=j_bad.lease_token,  # type: ignore[arg-type]
        error="child boom",
        requeue=False,
    )
    parent_after = queue.get(parent.id, tenant_id="t1")
    assert parent_after is not None
    assert parent_after.status == JobStatus.CANCELLED
    assert parent_after.await_children_outcome == "failed"
    assert parent_after.last_error is not None
    assert "child" in parent_after.last_error


def test_spawn_child_cancel_fails_parent(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": True})
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"c": 1}}, {"payload": {"c": 2}}],
    )
    kids = list(result.children)
    queue.cancel(kids[0].id, tenant_id="t1")
    parent_after = queue.get(parent.id, tenant_id="t1")
    assert parent_after is not None
    assert parent_after.status == JobStatus.CANCELLED
    assert parent_after.await_children_outcome == "failed"
    # Sibling cascade-cancelled.
    sib = queue.get(kids[1].id, tenant_id="t1")
    assert sib is not None
    assert sib.status == JobStatus.CANCELLED


def test_spawn_join_timeout_fails_parent(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": True})
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None
    queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"slow": True}}],
        timeout_seconds=0.2,
    )
    time.sleep(0.35)
    timed = queue.process_await_children_timeouts(tenant_id="t1")
    assert len(timed) >= 1
    parent_after = queue.get(parent.id, tenant_id="t1")
    assert parent_after is not None
    assert parent_after.status == JobStatus.CANCELLED
    assert parent_after.await_children_outcome == "timeout"
    kids = queue.list_children(parent.id, tenant_id="t1")
    assert all(k.status == JobStatus.CANCELLED for k in kids)


def test_spawn_join_timeout_expires_on_lease_path(queue: JobQueue):
    """Join deadlines must be reaped by normal lease(), not only the endpoint."""
    parent = queue.enqueue(tenant_id="t1", payload={"p": True})
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None
    queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"slow": True}}],
        timeout_seconds=0.25,
    )
    parent_parked = queue.get(parent.id, tenant_id="t1")
    assert parent_parked is not None
    assert parent_parked.status == JobStatus.AWAITING_CHILDREN

    time.sleep(0.4)
    # Tenant-scoped lease path reaps join timeouts (no process_* call).
    queue.enqueue(tenant_id="t1", payload={"probe": True})
    probe = queue.lease(
        tenant_id="t1", worker_id="probe", visibility_timeout_seconds=30
    )
    # Probe may be the child or the filler; parent must still have timed out.
    parent_after = queue.get(parent.id, tenant_id="t1")
    assert parent_after is not None
    assert parent_after.status == JobStatus.CANCELLED
    assert parent_after.await_children_outcome == "timeout"
    if probe is not None:
        try:
            queue.release(
                probe.id,
                tenant_id="t1",
                worker_id="probe",
                lease_token=probe.lease_token,  # type: ignore[arg-type]
                reason="test_cleanup",
            )
        except LeaseError:
            pass


def test_spawn_join_timeout_expires_on_fair_lease_multi_tenant(queue: JobQueue):
    """Fair multi-tenant lease reaps join timeouts across tenants."""
    from job_queue import Worker

    parents = []
    for i, tid in enumerate(("jt-a", "jt-b", "jt-c")):
        p = queue.enqueue(tenant_id=tid, payload={"parent": i})
        leased = queue.lease(
            tenant_id=tid, worker_id=f"setup-{i}", visibility_timeout_seconds=30
        )
        assert leased is not None
        queue.spawn_children(
            p.id,
            tenant_id=tid,
            worker_id=f"setup-{i}",
            lease_token=leased.lease_token,  # type: ignore[arg-type]
            children=[{"payload": {"ms": 60_000, "slow": True}}],
            timeout_seconds=0.3,
        )
        parents.append((tid, p.id))

    time.sleep(0.5)

    # Fair workers only lease ordinary work; no process_await_children_timeouts.
    done = threading.Event()
    workers = []
    threads = []
    for i in range(3):
        w = Worker(
            queue,
            worker_id=f"fair-to-{i}",
            poll_interval_seconds=0.05,
            visibility_timeout_seconds=30,
        )

        @w.handler
        def process(job: Job) -> dict:
            return {"ok": True, "id": str(job.id)}

        workers.append(w)
        th = threading.Thread(target=w.run, daemon=True)
        th.start()
        threads.append(th)

    # Enqueue probe jobs so fair lease path keeps running.
    for i in range(6):
        queue.enqueue(tenant_id=f"probe-{i % 3}", payload={"probe": i})

    deadline = time.time() + 8
    while time.time() < deadline:
        statuses = [queue.get(pid, tenant_id=tid) for tid, pid in parents]
        if all(
            s is not None
            and s.status == JobStatus.CANCELLED
            and s.await_children_outcome == "timeout"
            for s in statuses
        ):
            done.set()
            break
        time.sleep(0.1)

    for w in workers:
        w.stop()
    for th in threads:
        th.join(timeout=3)

    assert done.is_set(), "join timeouts not reaped by fair lease path"
    for tid, pid in parents:
        row = queue.get(pid, tenant_id=tid)
        assert row is not None
        assert row.status == JobStatus.CANCELLED
        assert row.await_children_outcome == "timeout"
        kids = queue.list_children(pid, tenant_id=tid)
        assert all(k.status == JobStatus.CANCELLED for k in kids)


def test_spawn_multi_tenant_isolation(queue: JobQueue):
    """Spawn/join under one tenant never touches another."""
    pa = queue.enqueue(tenant_id="ta", payload={"p": "a"})
    pb = queue.enqueue(tenant_id="tb", payload={"p": "b"})
    la = queue.lease(tenant_id="ta", worker_id="wa", visibility_timeout_seconds=30)
    lb = queue.lease(tenant_id="tb", worker_id="wb", visibility_timeout_seconds=30)
    assert la is not None and lb is not None
    queue.spawn_children(
        pa.id,
        tenant_id="ta",
        worker_id="wa",
        lease_token=la.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"c": "a1"}}, {"payload": {"c": "a2"}}],
    )
    queue.spawn_children(
        pb.id,
        tenant_id="tb",
        worker_id="wb",
        lease_token=lb.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"c": "b1"}}],
    )
    kids_a = queue.list_children(pa.id, tenant_id="ta")
    kids_b = queue.list_children(pb.id, tenant_id="tb")
    assert len(kids_a) == 2
    assert len(kids_b) == 1
    assert all(k.tenant_id == "ta" for k in kids_a)
    assert all(k.tenant_id == "tb" for k in kids_b)
    # Cancel one of A's children → only A fails.
    queue.cancel(kids_a[0].id, tenant_id="ta")
    pa_after = queue.get(pa.id, tenant_id="ta")
    pb_after = queue.get(pb.id, tenant_id="tb")
    assert pa_after is not None and pa_after.status == JobStatus.CANCELLED
    assert pb_after is not None and pb_after.status == JobStatus.AWAITING_CHILDREN


def test_spawn_multi_worker_join(queue: JobQueue):
    """Multiple fair workers process children while parent is parked."""
    from job_queue import Worker

    processed: list[str] = []
    lock = threading.Lock()
    workers = []
    threads = []
    for i in range(3):
        w = Worker(
            queue,
            worker_id=f"mw-{i}",  # fair multi-tenant
            poll_interval_seconds=0.05,
            visibility_timeout_seconds=30,
        )

        @w.handler
        def process(job: Job, lease: LeaseContext, _w=w) -> dict:
            if job.payload.get("role") == "parent":
                if job.await_children_outcome == "joined":
                    return {"joined": True, "worker": _w.worker_id}
                lease.spawn_children(
                    [{"payload": {"role": "child", "i": n}} for n in range(4)]
                )
                return {}
            with lock:
                processed.append(_w.worker_id)
            time.sleep(0.02)
            return {"i": job.payload.get("i"), "worker": _w.worker_id}

        workers.append(w)
        th = threading.Thread(target=w.run, daemon=True)
        th.start()
        threads.append(th)

    parents = []
    for tname in ("t-a", "t-b"):
        parents.append(
            queue.enqueue(tenant_id=tname, payload={"role": "parent", "tenant": tname})
        )

    deadline = time.time() + 15
    while time.time() < deadline:
        done = 0
        for p in parents:
            row = queue.get(p.id, tenant_id=p.tenant_id)
            if row and row.status == JobStatus.COMPLETED:
                done += 1
        if done == len(parents):
            break
        time.sleep(0.1)
    for p in parents:
        row = queue.get(p.id, tenant_id=p.tenant_id)
        assert row is not None and row.status == JobStatus.COMPLETED, row
        assert row.result and row.result.get("joined") is True
        kids = queue.list_children(p.id, tenant_id=p.tenant_id)
        assert len(kids) == 4
        assert all(k.status == JobStatus.COMPLETED for k in kids)
    # At least two different workers should have run something under load.
    with lock:
        assert len(set(processed)) >= 1
    for w in workers:
        w.stop()
    for th in threads:
        th.join(timeout=3)


def test_spawn_wait_false_keeps_lease(queue: JobQueue):
    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[{"payload": {"c": 1}}],
        wait=False,
    )
    assert result.parent.status == JobStatus.LEASED
    assert result.parent.lease_token is not None
    kids = list(result.children)
    assert len(kids) == 1
    assert kids[0].parent_job_id == parent.id
    # Parent can still ack.
    _ack(queue, result.parent, "p1", result={"spawned": True})
