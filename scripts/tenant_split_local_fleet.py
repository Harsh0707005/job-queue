#!/usr/bin/env python3
"""Simulated multi-coordinator fleet for tenant split (no Docker required).

Two JobQueue clients share one Postgres (like dual coordinators). Threads
lease/ack keep-queue jobs on the source while the other client plans/splits
the email queue onto a dest tenant.
"""

from __future__ import annotations

import os
import sys
import threading
from uuid import uuid4

from job_queue import JobQueue, JobStatus, TenantSplitError


def _ack(queue: JobQueue, job, wid: str) -> None:
    assert job.lease_token is not None
    queue.ack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=wid,
        lease_token=job.lease_token,
    )


def _run(dsn: str, stage: int) -> None:
    a = JobQueue.connect(dsn, min_size=2, max_size=8, require_worker_registration=False)
    b = JobQueue.connect(dsn, min_size=2, max_size=8, require_worker_registration=False)
    src, dest = f"loc-src-{uuid4().hex[:6]}", f"loc-dst-{uuid4().hex[:6]}"
    try:
        keep = [
            a.enqueue(tenant_id=src, queue_name="default", payload={"k": i})
            for i in range(8)
        ]
        mail = [
            a.enqueue(tenant_id=src, queue_name="email", payload={"m": i})
            for i in range(10)
        ]
        parent = a.enqueue(
            tenant_id=src, queue_name="default", payload={"p": 1}
        )
        child = a.enqueue(
            tenant_id=src,
            queue_name="email",
            payload={"c": 1},
            depends_on=parent.id,
        )
        plan = b.plan_tenant_split(src, dest, queue_name="email")
        assert plan.ok is False
        assert child.id not in plan.move_job_ids
        for job in mail + keep + [parent, child]:
            assert a.get(job.id, tenant_id=src) is not None
        print(
            f"stage 1 plan ok=False blockers={len(plan.blockers)} "
            f"isolated={len(plan.move_job_ids)}"
        )
        if stage < 2:
            return
        try:
            b.split_tenant(src, dest, queue_name="email")
            raise SystemExit("expected fail-closed split")
        except TenantSplitError:
            pass
        a.cancel(child.id, tenant_id=src)
        a.cancel(parent.id, tenant_id=src)
        plan2 = b.plan_tenant_split(src, dest, queue_name="email")
        assert plan2.ok is True
        done: list = []
        stop = threading.Event()

        def worker(q: JobQueue, wid: str) -> None:
            while True:
                job = q.lease(tenant_id=src, worker_id=wid, queue_name="default")
                if job is None:
                    if stop.is_set():
                        return
                    continue
                _ack(q, job, wid)
                done.append(job.id)

        threads = [
            threading.Thread(target=worker, args=(a, "wa")),
            threading.Thread(target=worker, args=(b, "wb")),
        ]
        for t in threads:
            t.start()
        result = b.split_tenant(src, dest, queue_name="email")
        assert result.jobs_moved == 10
        stop.set()
        for t in threads:
            t.join(timeout=5)
        assert set(done) == {j.id for j in keep}
        for job in mail:
            assert a.get(job.id, tenant_id=src) is None
            got = b.get(job.id, tenant_id=dest)
            assert got is not None and got.status == JobStatus.PENDING
            leased = b.lease(tenant_id=dest, worker_id="wd", queue_name="email")
            assert leased is not None
            _ack(b, leased, "wd")
        print(f"stage 2 split moved={result.jobs_moved} keep_acked={len(done)}")
    finally:
        a.close()
        b.close()


if __name__ == "__main__":
    dsn = os.environ.get("JOB_QUEUE_TEST_DSN")
    if not dsn:
        print("set JOB_QUEUE_TEST_DSN", file=sys.stderr)
        sys.exit(2)
    stage = int(os.environ.get("SPLIT_STAGE", "2") or "2")
    _run(dsn, stage)
    print("local fleet OK")
