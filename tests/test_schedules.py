"""Recurring schedules."""

from __future__ import annotations

import threading
import time

import pytest

from job_queue import JobQueue, JobStatus

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Recurring schedules
# ---------------------------------------------------------------------------


def test_create_list_get_schedule(queue: JobQueue):
    from job_queue import Schedule

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=30,
        payload={"task": "heartbeat-check"},
        queue_name="ops",
        name="ops-heartbeat",
        priority=5,
        max_attempts=2,
        visibility_timeout_seconds=45,
    )
    assert isinstance(sched, Schedule)
    assert sched.enabled is True
    assert sched.interval_seconds == 30
    assert sched.cron_expr is None
    assert sched.timezone == "UTC"
    assert sched.queue_name == "ops"
    assert sched.payload == {"task": "heartbeat-check"}
    assert sched.name == "ops-heartbeat"

    got = queue.get_schedule(sched.id, tenant_id="t1")
    assert got is not None
    assert got.id == sched.id
    assert queue.get_schedule(sched.id, tenant_id="other") is None

    listed = queue.list_schedules(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].id == sched.id


def test_create_cron_schedule_weekdays_and_dom(queue: JobQueue):
    from datetime import datetime, timezone

    # Tuesday 2026-07-28 08:00 UTC → next weekday 09:00 is same day.
    base = datetime(2026, 7, 28, 8, 0, 0, tzinfo=timezone.utc)
    weekdays = queue.create_schedule(
        tenant_id="t1",
        cron="0 9 * * 1-5",
        payload={"kind": "standup"},
        name="weekday-standup",
        timezone="UTC",
        start_at=base,
    )
    assert weekdays.cron_expr == "0 9 * * 1-5"
    assert weekdays.interval_seconds is None
    assert weekdays.next_run_at == datetime(2026, 7, 28, 9, 0, 0, tzinfo=timezone.utc)

    # After the 1st → next is 1st of following month.
    after_first = datetime(2026, 7, 2, 0, 0, 0, tzinfo=timezone.utc)
    monthly = queue.create_schedule(
        tenant_id="t1",
        cron="0 0 1 * *",
        payload={"kind": "billing"},
        name="month-start",
        start_at=after_first,
    )
    assert monthly.next_run_at == datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_cron_schedule_fires_and_advances(queue: JobQueue):
    # Every second (6-field) so tests stay short.
    sched = queue.create_schedule(
        tenant_id="t1",
        cron="*/1 * * * * *",
        payload={"tick": True},
        name="every-sec",
    )
    # First next_run is ~1s in the future; wait for due.
    deadline = time.time() + 3.0
    fires: list = []
    while time.time() < deadline and not fires:
        fires = queue.process_due_schedules(tenant_id="t1")
        if not fires:
            time.sleep(0.15)
    assert len(fires) == 1
    assert fires[0].job.payload == {"tick": True}
    assert fires[0].schedule.id == sched.id
    assert fires[0].schedule.next_run_at > fires[0].schedule.last_enqueued_at  # type: ignore[operator]
    # Complete so next tick can fire (no-overlap).
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")
    time.sleep(1.2)
    second = queue.process_due_schedules(tenant_id="t1")
    assert len(second) == 1


def test_schedule_fire_bypasses_pending_cap_and_enqueue_rate(queue: JobQueue):
    """System timers still materialize under tenant throttles."""
    from job_queue import RateLimitError

    # --- pending cap bypass ---
    queue.set_pending_limit("t1", 1)
    queue.enqueue(tenant_id="t1", payload={"manual": 1})
    with pytest.raises(ValueError, match="pending job cap"):
        queue.enqueue(tenant_id="t1", payload={"manual": 2})

    queue.create_schedule(
        tenant_id="t1",
        cron="*/1 * * * * *",
        payload={"from": "schedule-cap"},
        name="under-cap",
    )
    deadline = time.time() + 3.0
    fires: list = []
    while time.time() < deadline and not fires:
        fires = queue.process_due_schedules(tenant_id="t1")
        if not fires:
            time.sleep(0.15)
    assert len(fires) == 1
    assert fires[0].job.payload == {"from": "schedule-cap"}
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 2

    # Drain so rate-limit section has a clean pending set.
    for _ in range(5):
        j = queue.lease(tenant_id="t1", worker_id="w1")
        if j is None:
            break
        _ack(queue, j, "w1")
    queue.set_pending_limit("t1", None)

    # --- enqueue rate bypass ---
    queue.set_enqueue_rate_limit("t1", 1.0, burst=1)
    queue.enqueue(tenant_id="t1", payload={"manual": 3})
    with pytest.raises(RateLimitError):
        queue.enqueue(tenant_id="t1", payload={"manual": 4})

    queue.create_schedule(
        tenant_id="t1",
        cron="*/1 * * * * *",
        payload={"from": "schedule-rate"},
        name="under-rate",
    )
    deadline = time.time() + 3.0
    rate_fires: list = []
    while time.time() < deadline and not rate_fires:
        batch = queue.process_due_schedules(tenant_id="t1")
        rate_fires = [f for f in batch if f.job.payload.get("from") == "schedule-rate"]
        if not rate_fires:
            time.sleep(0.15)
    assert len(rate_fires) >= 1


def test_process_due_schedules_enqueues_job_with_metadata(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"n": 7},
        queue_name="metrics",
        priority=9,
        max_attempts=4,
        visibility_timeout_seconds=20,
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    fire = fires[0]
    assert fire.schedule.id == sched.id
    assert fire.schedule.last_enqueued_at is not None
    assert fire.schedule.next_run_at > fire.schedule.last_enqueued_at
    assert fire.job.tenant_id == "t1"
    assert fire.job.queue_name == "metrics"
    assert fire.job.payload == {"n": 7}
    assert fire.job.priority == 9
    assert fire.job.max_attempts == 4
    assert fire.job.visibility_timeout_seconds == 20
    assert fire.job.status == JobStatus.PENDING

    leased = queue.lease(tenant_id="t1", worker_id="w1", queue_name="metrics")
    assert leased is not None
    assert leased.id == fire.job.id
    _ack(queue, leased, "w1")


def test_schedule_does_not_double_fire_until_interval_elapses(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=2,
        payload={"tick": True},
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    # Complete so a later tick can enqueue (no-overlap policy).
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    second = queue.process_due_schedules(tenant_id="t1")
    assert second == []  # interval not elapsed

    time.sleep(2.2)
    third = queue.process_due_schedules(tenant_id="t1")
    assert len(third) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_pause_and_resume_schedule(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"x": 1},
    )
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    paused = queue.pause_schedule(sched.id, tenant_id="t1")
    assert paused.enabled is False
    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []

    resumed = queue.resume_schedule(sched.id, tenant_id="t1")
    assert resumed.enabled is True
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1


def test_delete_schedule_stops_future_fires(queue: JobQueue):
    from job_queue import ScheduleError

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={},
    )
    assert queue.delete_schedule(sched.id, tenant_id="t1") is True
    assert queue.get_schedule(sched.id, tenant_id="t1") is None
    assert queue.delete_schedule(sched.id, tenant_id="t1") is False

    time.sleep(1.1)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1") == 0

    with pytest.raises(ScheduleError):
        queue.pause_schedule(sched.id, tenant_id="t1")


def test_schedule_name_unique_per_tenant(queue: JobQueue):
    from job_queue import ScheduleError

    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=10,
        payload={},
        name="daily",
    )
    with pytest.raises(ScheduleError, match="already exists"):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=10,
            payload={},
            name="daily",
        )
    # Same name other tenant is fine.
    other = queue.create_schedule(
        tenant_id="t2",
        interval_seconds=10,
        payload={},
        name="daily",
    )
    assert other.tenant_id == "t2"


def test_schedule_tenant_isolation_on_process(queue: JobQueue):
    queue.create_schedule(tenant_id="alpha", interval_seconds=60, payload={"t": "a"})
    queue.create_schedule(tenant_id="beta", interval_seconds=60, payload={"t": "b"})

    alpha_fires = queue.process_due_schedules(tenant_id="alpha")
    assert len(alpha_fires) == 1
    assert alpha_fires[0].job.payload == {"t": "a"}
    assert queue.count(tenant_id="beta", status=JobStatus.PENDING) == 0

    beta_fires = queue.process_due_schedules(tenant_id="beta")
    assert len(beta_fires) == 1


def test_concurrent_process_due_schedules_single_fire(queue: JobQueue):
    """Many tickers race; each due schedule fires exactly once per window."""
    n_schedules = 5
    for i in range(n_schedules):
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=30,
            payload={"i": i},
            name=f"s-{i}",
        )

    barrier = threading.Barrier(8)
    all_fires: list = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def ticker() -> None:
        barrier.wait()
        try:
            fires = queue.process_due_schedules(tenant_id="t1", limit=100)
            with lock:
                all_fires.extend(fires)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=ticker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    assert errors == []
    assert len(all_fires) == n_schedules
    job_ids = [f.job.id for f in all_fires]
    assert len(set(job_ids)) == n_schedules
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == n_schedules

    # Not due again immediately.
    assert queue.process_due_schedules(tenant_id="t1") == []


def test_schedule_runner_fires_and_stop(queue: JobQueue):
    from job_queue import ScheduleRunner

    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"runner": True},
    )
    seen: list = []
    runner = ScheduleRunner(
        queue,
        tenant_id="t1",
        poll_interval_seconds=0.2,
        on_fire=lambda f: seen.append(f.job.id),
    )

    # Background run briefly.
    t = threading.Thread(target=runner.run, daemon=True)
    t.start()
    time.sleep(1.5)
    runner.stop()
    t.join(timeout=5)
    assert not t.is_alive()
    assert len(seen) >= 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) >= 1


def test_schedule_start_at_future_not_due_yet(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    future = datetime.now(timezone.utc) + timedelta(seconds=3)
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=60,
        payload={"later": True},
        start_at=future,
    )
    assert queue.process_due_schedules(tenant_id="t1") == []
    time.sleep(3.2)
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1


def test_schedule_skips_tick_while_previous_job_pending(queue: JobQueue):
    """Slow work: interval elapses but prior job still pending → no overlap."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"work": "slow"},
        name="slow-job",
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1
    first_job = first[0].job
    assert first_job.schedule_id == sched.id
    assert first_job.status == JobStatus.PENDING

    time.sleep(1.2)
    # Due again, but previous still pending.
    skipped = queue.process_due_schedules(tenant_id="t1")
    assert skipped == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 0

    # Still only the original job.
    pending = queue.lease(tenant_id="t1", worker_id="w1")
    assert pending is not None
    assert pending.id == first_job.id


def test_schedule_skips_tick_while_previous_job_leased(queue: JobQueue):
    """Worker holds lease longer than interval → no second enqueue."""
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"work": "held"},
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1

    leased = queue.lease(tenant_id="t1", worker_id="slow-worker")
    assert leased is not None
    assert leased.status == JobStatus.LEASED

    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.LEASED) == 1

    _ack(queue, leased, "slow-worker")
    # Schedule remains due from skipped ticks → fires once free.
    fires = queue.process_due_schedules(tenant_id="t1")
    assert len(fires) == 1
    assert fires[0].job.id != leased.id
    assert fires[0].job.schedule_id == first[0].schedule.id


def test_schedule_fires_again_only_after_prior_completes(queue: JobQueue):
    queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"n": 1},
    )
    f1 = queue.process_due_schedules(tenant_id="t1")
    assert len(f1) == 1

    time.sleep(1.2)
    assert queue.process_due_schedules(tenant_id="t1") == []

    j = queue.lease(tenant_id="t1", worker_id="w1")
    assert j is not None
    _ack(queue, j, "w1")

    f2 = queue.process_due_schedules(tenant_id="t1")
    assert len(f2) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) == 1


def test_concurrent_schedule_ticks_no_overlap_under_load(queue: JobQueue):
    """Many tickers + uncompleted first job: never more than one active job."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=1,
        payload={"k": "v"},
        name="solo",
    )
    first = queue.process_due_schedules(tenant_id="t1")
    assert len(first) == 1

    time.sleep(1.2)
    barrier = threading.Barrier(10)
    fire_counts: list[int] = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def ticker() -> None:
        barrier.wait()
        try:
            fires = queue.process_due_schedules(tenant_id="t1")
            with lock:
                fire_counts.append(len(fires))
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=ticker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    assert errors == []
    assert sum(fire_counts) == 0  # all skipped — prior still pending
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM jobs
            WHERE schedule_id = %s AND status IN ('pending', 'leased')
            """,
            (sched.id,),
        ).fetchone()
    assert row is not None
    assert int(row["n"]) == 1
