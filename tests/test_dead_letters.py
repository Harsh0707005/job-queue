"""Dead letter inspect + replay."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import pytest

from job_queue import Job, JobQueue, JobStatus, LeaseContext, LeaseError, Worker

from tests.helpers_queue import (
    _ack,
    _drive_to_dead,
)

# ---------------------------------------------------------------------------
# Dead letter inspect + replay
# ---------------------------------------------------------------------------

def test_list_dead_letters_empty(queue: JobQueue):
    assert queue.list_dead_letters(tenant_id="t1") == []

def test_list_dead_letters_returns_dead_jobs(queue: JobQueue):
    d1 = _drive_to_dead(queue, tenant_id="t1", payload={"n": 1})
    d2 = _drive_to_dead(queue, tenant_id="t1", payload={"n": 2})

    # Non-dead noise should not appear.
    queue.enqueue(tenant_id="t1", payload={"live": True})

    dead = queue.list_dead_letters(tenant_id="t1")
    ids = {j.id for j in dead}
    assert ids == {d1.id, d2.id}
    assert all(j.status == JobStatus.DEAD for j in dead)
    assert all(j.last_error is not None for j in dead)

def test_list_dead_letters_tenant_isolation(queue: JobQueue):
    a = _drive_to_dead(queue, tenant_id="alpha", payload={"t": "a"})
    b = _drive_to_dead(queue, tenant_id="beta", payload={"t": "b"})

    only_a = queue.list_dead_letters(tenant_id="alpha")
    only_b = queue.list_dead_letters(tenant_id="beta")
    assert [j.id for j in only_a] == [a.id]
    assert [j.id for j in only_b] == [b.id]

def test_list_dead_letters_filter_queue_name_and_paging(queue: JobQueue):
    email = _drive_to_dead(
        queue, tenant_id="t1", payload={"q": "e"}, queue_name="email"
    )
    _drive_to_dead(queue, tenant_id="t1", payload={"q": "s"}, queue_name="sms")

    only_email = queue.list_dead_letters(tenant_id="t1", queue_name="email")
    assert len(only_email) == 1
    assert only_email[0].id == email.id

    # Create several dead jobs then page.
    for i in range(5):
        _drive_to_dead(queue, tenant_id="t2", payload={"i": i})
    page1 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=0)
    page2 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=2)
    page3 = queue.list_dead_letters(tenant_id="t2", limit=2, offset=4)
    assert len(page1) == 2
    assert len(page2) == 2
    assert len(page3) == 1
    all_ids = [j.id for j in page1 + page2 + page3]
    assert len(all_ids) == len(set(all_ids)) == 5

def test_replay_dead_letter_makes_job_claimable(queue: JobQueue):
    from job_queue import ReplayError  # noqa: F401

    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"task": "retry-me"}, max_attempts=2
    )
    assert dead.attempts == 2
    assert dead.last_error is not None

    replayed = queue.replay_dead_letter(dead.id, tenant_id="t1")
    assert replayed.status == JobStatus.PENDING
    assert replayed.attempts == 0
    assert replayed.leased_by is None
    assert replayed.lease_token is None
    # Forensic error kept by default.
    assert replayed.last_error == dead.last_error

    leased = queue.lease(tenant_id="t1", worker_id="rescuer")
    assert leased is not None
    assert leased.id == dead.id
    assert leased.attempts == 1
    _ack(queue, leased, "rescuer")
    assert queue.get(dead.id, tenant_id="t1").status == JobStatus.COMPLETED

def test_replay_dead_letter_clear_error_and_custom_max_attempts(queue: JobQueue):
    dead = _drive_to_dead(queue, tenant_id="t1", payload={}, max_attempts=1)
    replayed = queue.replay_dead_letter(
        dead.id,
        tenant_id="t1",
        clear_error=True,
        max_attempts=5,
    )
    assert replayed.last_error is None
    assert replayed.max_attempts == 5
    assert replayed.attempts == 0

def test_replay_dead_letter_with_delay(queue: JobQueue):
    dead = _drive_to_dead(queue, tenant_id="t1", payload={"later": True})
    replayed = queue.replay_dead_letter(
        dead.id, tenant_id="t1", delay_seconds=2
    )
    assert replayed.status == JobStatus.PENDING
    assert queue.lease(tenant_id="t1", worker_id="w1") is None

    time.sleep(2.2)
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    assert leased.id == dead.id

def test_replay_rejects_non_dead_and_cross_tenant(queue: JobQueue):
    from job_queue import ReplayError

    pending = queue.enqueue(tenant_id="t1", payload={})
    with pytest.raises(ReplayError, match="not dead-lettered"):
        queue.replay_dead_letter(pending.id, tenant_id="t1")

    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    with pytest.raises(ReplayError, match="not dead-lettered"):
        queue.replay_dead_letter(leased.id, tenant_id="t1")

    dead = _drive_to_dead(queue, tenant_id="alpha", payload={})
    with pytest.raises(ReplayError, match="not found"):
        queue.replay_dead_letter(dead.id, tenant_id="beta")

    with pytest.raises(ReplayError, match="not found"):
        queue.replay_dead_letter(uuid4(), tenant_id="t1")

def test_replay_dedupe_conflict(queue: JobQueue):
    from job_queue import ReplayError

    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"v": 1}, dedupe_key="shared"
    )
    # Active job holds the same dedupe key.
    live = queue.enqueue(
        tenant_id="t1", payload={"v": 2}, dedupe_key="shared"
    )
    assert live.status == JobStatus.PENDING

    with pytest.raises(ReplayError, match="dedupe_key"):
        queue.replay_dead_letter(dead.id, tenant_id="t1")

    # After live completes, replay succeeds.
    leased = queue.lease(tenant_id="t1", worker_id="w1")
    assert leased is not None
    _ack(queue, leased, "w1")

    replayed = queue.replay_dead_letter(dead.id, tenant_id="t1")
    assert replayed.status == JobStatus.PENDING

def test_replay_dead_letters_bulk_by_ids_and_filter(queue: JobQueue):
    d1 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 1}, queue_name="a")
    d2 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 2}, queue_name="a")
    d3 = _drive_to_dead(queue, tenant_id="t1", payload={"i": 3}, queue_name="b")

    # Bulk by explicit ids (include a non-dead id — skipped).
    pending = queue.enqueue(tenant_id="t1", payload={"skip": True})
    replayed = queue.replay_dead_letters(
        tenant_id="t1",
        job_ids=[d1.id, d2.id, pending.id],
    )
    assert {j.id for j in replayed} == {d1.id, d2.id}
    assert all(j.status == JobStatus.PENDING for j in replayed)

    # d3 still dead; replay whole queue "b"
    only_b = queue.replay_dead_letters(tenant_id="t1", queue_name="b")
    assert len(only_b) == 1
    assert only_b[0].id == d3.id

    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 0
    assert queue.count(tenant_id="t1", status=JobStatus.PENDING) >= 3

def test_replay_dead_letters_limit(queue: JobQueue):
    for i in range(4):
        _drive_to_dead(queue, tenant_id="t1", payload={"i": i})

    first_batch = queue.replay_dead_letters(tenant_id="t1", limit=2)
    assert len(first_batch) == 2
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 2

    second_batch = queue.replay_dead_letters(tenant_id="t1", limit=10)
    assert len(second_batch) == 2
    assert queue.count(tenant_id="t1", status=JobStatus.DEAD) == 0

def test_worker_processes_replayed_dead_letter(queue: JobQueue):
    """End-to-end: dead → list → replay → worker completes."""
    dead = _drive_to_dead(
        queue, tenant_id="t1", payload={"x": 99}, max_attempts=1
    )
    listed = queue.list_dead_letters(tenant_id="t1")
    assert len(listed) == 1
    assert listed[0].id == dead.id

    queue.replay_dead_letter(dead.id, tenant_id="t1", clear_error=True)

    seen: list[int] = []
    worker = Worker(queue, tenant_id="t1", worker_id="dlq-worker")

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.payload["x"])

    assert worker.run_once() is not None
    assert seen == [99]
    assert queue.count(tenant_id="t1", status=JobStatus.COMPLETED) == 1
    assert queue.list_dead_letters(tenant_id="t1") == []

def test_replay_dedupe_race_with_concurrent_enqueue(queue: JobQueue):
    """Replay vs enqueue on the same dedupe key must not leak unique_violation.

    Under load, either replay wins (dead → pending) or enqueue wins (new/existing
    active row) and losers get clean outcomes (ReplayError / existing job).
    Never more than one active job for the key, and never a raw DB error.
    """
    from job_queue import ReplayError
    from psycopg.errors import UniqueViolation

    dead = _drive_to_dead(
        queue,
        tenant_id="t1",
        payload={"origin": "dead"},
        dedupe_key="race-key",
        max_attempts=1,
    )
    assert dead.status == JobStatus.DEAD

    n_replay = 12
    n_enqueue = 12
    barrier = threading.Barrier(n_replay + n_enqueue)
    outcomes: list[tuple[str, object]] = []
    raw_errors: list[BaseException] = []
    lock = threading.Lock()

    def record(kind: str, value: object) -> None:
        with lock:
            outcomes.append((kind, value))

    def record_error(exc: BaseException) -> None:
        with lock:
            raw_errors.append(exc)

    def replayer() -> None:
        barrier.wait()
        try:
            job = queue.replay_dead_letter(dead.id, tenant_id="t1")
            record("replay_ok", job)
        except ReplayError as exc:
            record("replay_err", str(exc))
        except Exception as exc:  # noqa: BLE001 — we assert none of these
            record_error(exc)

    def enqueuer(i: int) -> None:
        barrier.wait()
        try:
            job = queue.enqueue(
                tenant_id="t1",
                payload={"origin": f"enq-{i}"},
                dedupe_key="race-key",
            )
            record("enq_ok", job)
        except Exception as exc:  # noqa: BLE001
            record_error(exc)

    threads = [threading.Thread(target=replayer) for _ in range(n_replay)]
    threads += [
        threading.Thread(target=enqueuer, args=(i,)) for i in range(n_enqueue)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    # No raw unique violations or other unexpected DB errors.
    assert raw_errors == [], f"unexpected errors: {raw_errors!r}"
    assert not any(isinstance(e, UniqueViolation) for e in raw_errors)

    assert len(outcomes) == n_replay + n_enqueue

    # At most one successful replay (job can only leave dead once).
    replay_oks = [j for k, j in outcomes if k == "replay_ok"]
    assert len(replay_oks) <= 1

    # Exactly one active (pending/leased) job for the dedupe key.
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n, array_agg(id) AS ids
            FROM jobs
            WHERE tenant_id = %s
              AND dedupe_key = %s
              AND status IN ('pending', 'leased')
            """,
            ("t1", "race-key"),
        ).fetchone()
    assert row is not None
    assert int(row["n"]) == 1, f"expected 1 active job, got {row}"

    # Enqueues always return some job (either newly inserted or the active one).
    enq_oks = [j for k, j in outcomes if k == "enq_ok"]
    assert len(enq_oks) == n_enqueue
    active_id = enq_oks[0].id
    assert all(j.id == active_id for j in enq_oks)

    # If replay succeeded, it must be that same active job.
    if replay_oks:
        assert replay_oks[0].id == active_id
        assert replay_oks[0].id == dead.id
    else:
        # Enqueue won the race: dead job stays dead (or was never activated).
        still = queue.get(dead.id, tenant_id="t1")
        assert still is not None
        # Either still dead, or if somehow replayed only one active exists above.
        assert still.status in (JobStatus.DEAD, JobStatus.PENDING, JobStatus.LEASED)

