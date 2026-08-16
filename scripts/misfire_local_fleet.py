#!/usr/bin/env python3
"""Multi-worker local fleet for schedule misfire policy (no Docker).

Two JobQueue clients (dual coordinators) plus four lease threads and two
process_due tickers share one Postgres.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import timedelta
from uuid import uuid4

from job_queue import JobQueue, JobStatus
from job_queue.queue._common import _utcnow


def _ack(queue: JobQueue, job, wid: str) -> None:
    assert job.lease_token is not None
    queue.ack(
        job.id,
        tenant_id=job.tenant_id,
        worker_id=wid,
        lease_token=job.lease_token,
    )


def _run(dsn: str) -> None:
    a = JobQueue.connect(dsn, min_size=2, max_size=8, require_worker_registration=False)
    b = JobQueue.connect(dsn, min_size=2, max_size=8, require_worker_registration=False)
    tag = uuid4().hex[:6]
    skip_t, one_t, catch_t, live_t = (
        f"loc-skip-{tag}",
        f"loc-one-{tag}",
        f"loc-catch-{tag}",
        f"loc-live-{tag}",
    )
    stop = threading.Event()
    errors: list[BaseException] = []

    def ticker(q: JobQueue, name: str) -> None:
        try:
            while not stop.is_set():
                q.process_due_schedules(limit=50)
                time.sleep(0.15)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def worker(q: JobQueue, wid: str) -> None:
        try:
            while not stop.is_set():
                job = q.lease(worker_id=wid)
                if job is None:
                    time.sleep(0.05)
                    continue
                _ack(q, job, wid)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    try:
        future = _utcnow() + timedelta(hours=1)
        skip_s = a.create_schedule(
            tenant_id=skip_t,
            interval_seconds=60,
            payload={"p": "skip"},
            misfire_policy="skip",
            start_at=future,
        )
        one_s = a.create_schedule(
            tenant_id=one_t,
            interval_seconds=60,
            payload={"p": "one"},
            misfire_policy="catch_up_one",
            start_at=future,
        )
        catch_s = a.create_schedule(
            tenant_id=catch_t,
            interval_seconds=60,
            payload={"p": "catch"},
            misfire_policy="catch_up",
            catch_up_max=3,
            start_at=future,
        )
        a.create_schedule(
            tenant_id=live_t,
            interval_seconds=1,
            payload={"p": "live"},
            misfire_policy="catch_up_one",
        )
        threads = [
            threading.Thread(target=ticker, args=(a, "ta")),
            threading.Thread(target=ticker, args=(b, "tb")),
            threading.Thread(target=worker, args=(a, "wa")),
            threading.Thread(target=worker, args=(b, "wb")),
            threading.Thread(target=worker, args=(a, "wc")),
            threading.Thread(target=worker, args=(b, "wd")),
        ]
        for t in threads:
            t.start()
        time.sleep(1.5)
        live_mid = a.count(tenant_id=live_t, status=JobStatus.COMPLETED)
        if live_mid < 1:
            raise SystemExit(f"live tenant did not complete during warmup: {live_mid}")
        a.pause_tenant(skip_t)
        a.pause_tenant(one_t)
        a.pause_tenant(catch_t)
        with a._pool.connection() as conn:
            conn.execute(
                """
                UPDATE schedules
                SET next_run_at = NOW() - INTERVAL '300 seconds'
                WHERE id = ANY(%s)
                """,
                ([skip_s.id, one_s.id, catch_s.id],),
            )
            conn.commit()
        time.sleep(0.8)
        assert a.count(tenant_id=skip_t) == 0
        assert a.count(tenant_id=one_t) == 0
        assert a.count(tenant_id=catch_t) == 0
        a.resume_tenant(skip_t)
        a.resume_tenant(one_t)
        a.resume_tenant(catch_t)
        # Apply policy a few times so catch_up can drain one-at-a-time.
        for _ in range(8):
            a.process_due_schedules(limit=50)
            b.process_due_schedules(limit=50)
            time.sleep(0.15)
        a.pause_tenant(skip_t)
        a.pause_tenant(one_t)
        a.pause_tenant(catch_t)
        stop.set()
        for t in threads:
            t.join(timeout=5)
        if errors:
            raise SystemExit(f"worker/ticker errors: {errors!r}")
        skip = a.get_schedule(skip_s.id, tenant_id=skip_t)
        one = a.get_schedule(one_s.id, tenant_id=one_t)
        catch = a.get_schedule(catch_s.id, tenant_id=catch_t)
        assert skip is not None and one is not None and catch is not None
        skip_n = a.count(tenant_id=skip_t)
        one_n = a.count(tenant_id=one_t)
        catch_n = a.count(tenant_id=catch_t)
        live_n = a.count(tenant_id=live_t, status=JobStatus.COMPLETED)
        print(
            f"skip jobs={skip_n} missed={skip.missed_ticks} "
            f"one jobs={one_n} missed={one.missed_ticks} "
            f"catch jobs={catch_n} missed={catch.missed_ticks} "
            f"live_completed={live_n}"
        )
        if skip_n != 0:
            raise SystemExit(f"skip flooded {skip_n}")
        if skip.missed_ticks < 4:
            raise SystemExit(f"skip missed_ticks too low {skip.missed_ticks}")
        if one_n != 1:
            raise SystemExit(f"catch_up_one unexpected job count {one_n}")
        if catch_n < 1 or catch_n > 3:
            raise SystemExit(f"catch_up unexpected job count {catch_n}")
        if live_n < live_mid:
            raise SystemExit("live tenant stalled")
        print("misfire local fleet OK")
    finally:
        stop.set()
        a.close()
        b.close()


if __name__ == "__main__":
    dsn = os.environ.get("JOB_QUEUE_TEST_DSN") or os.environ.get("JOB_QUEUE_DSN")
    if not dsn:
        print("set JOB_QUEUE_TEST_DSN or JOB_QUEUE_DSN", file=sys.stderr)
        sys.exit(2)
    os.environ.setdefault("JOB_QUEUE_ALLOW_PRIVATE_WEBHOOKS", "1")
    _run(dsn)
