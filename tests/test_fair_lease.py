"""Fair multi-tenant leasing."""

from __future__ import annotations

import threading
import time

from job_queue import Job, JobQueue, JobStatus, Worker

from tests.helpers_queue import (
    _ack,
)

# ---------------------------------------------------------------------------
# Fair multi-tenant leasing
# ---------------------------------------------------------------------------


def test_fair_lease_without_tenant_id(queue: JobQueue):
    queue.enqueue(tenant_id="alpha", payload={"t": "a"})
    queue.enqueue(tenant_id="beta", payload={"t": "b"})

    j1 = queue.lease(worker_id="shared-1")
    j2 = queue.lease(worker_id="shared-1")
    assert j1 is not None and j2 is not None
    tenants = {j1.tenant_id, j2.tenant_id}
    assert tenants == {"alpha", "beta"}
    assert queue.lease(worker_id="shared-1") is None


def test_fair_lease_prefers_under_served_tenants(queue: JobQueue):
    """Noisy tenant with many jobs must not monopolize early leases."""
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i})
    queue.enqueue(tenant_id="quiet", payload={"only": True})

    claimed_tenants: list[str] = []
    for _ in range(5):
        job = queue.lease(worker_id="fair-w")
        assert job is not None
        claimed_tenants.append(job.tenant_id)

    # Quiet tenant should appear within the first few leases (fairness).
    assert "quiet" in claimed_tenants
    # Not all five from noisy before quiet got a turn.
    assert claimed_tenants.count("noisy") < 5


def test_fair_lease_ack_drain_does_not_starve_late_tenant(queue: JobQueue):
    """Real starvation case: noisy backlog enqueued first, workers lease+ack.

    With rank-by-created_at (or lease-count fairness that collapses when jobs
    are acked immediately), the entire noisy backlog would drain before quiet
    ever runs. Round-robin per tenant must serve quiet early.
    """
    noisy_n = 50
    for i in range(noisy_n):
        queue.enqueue(tenant_id="noisy", payload={"i": i, "t": "noisy"})
    quiet = queue.enqueue(tenant_id="quiet", payload={"t": "quiet"})

    order: list[str] = []
    quiet_position: int | None = None
    for step in range(noisy_n + 1):
        job = queue.lease(worker_id="drain-w")
        assert job is not None, f"expected a job at step {step}"
        order.append(job.tenant_id)
        if job.id == quiet.id:
            quiet_position = step
        _ack(queue, job, "drain-w")

    assert quiet_position is not None, "quiet job never leased"
    # Must not wait until after the full noisy backlog (position 50).
    # Round-robin: at worst quiet is 2nd (after noisy's first rn=1 job).
    assert quiet_position <= 1, (
        f"quiet starved until position {quiet_position}; order={order[:10]}..."
    )
    assert order.count("quiet") == 1
    assert order.count("noisy") == noisy_n
    assert queue.lease(worker_id="drain-w") is None


def test_fair_lease_round_robin_three_tenants_with_backlog(queue: JobQueue):
    """Each tenant with a backlog gets a turn before any gets a second job."""
    for t in ("a", "b", "c"):
        for i in range(10):
            queue.enqueue(tenant_id=t, payload={"t": t, "i": i})

    first_round: list[str] = []
    for _ in range(3):
        job = queue.lease(worker_id="rr-w")
        assert job is not None
        first_round.append(job.tenant_id)
        _ack(queue, job, "rr-w")

    assert set(first_round) == {"a", "b", "c"}

    second_round: list[str] = []
    for _ in range(3):
        job = queue.lease(worker_id="rr-w")
        assert job is not None
        second_round.append(job.tenant_id)
        _ack(queue, job, "rr-w")

    assert set(second_round) == {"a", "b", "c"}


def test_fair_lease_ack_drain_multi_worker_no_starvation(queue: JobQueue):
    """Several workers race lease+ack against a noisy backlog + quiet jobs."""
    for i in range(40):
        queue.enqueue(tenant_id="noisy", payload={"i": i})
    quiet_ids = set()
    for i in range(5):
        q = queue.enqueue(tenant_id="quiet", payload={"q": i})
        quiet_ids.add(q.id)

    completed_quiet: set = set()
    lock = threading.Lock()
    errors: list[BaseException] = []

    def worker_loop(wid: str) -> None:
        try:
            while True:
                job = queue.lease(worker_id=wid)
                if job is None:
                    # Might be racing; brief retry then exit if still empty.
                    time.sleep(0.05)
                    job = queue.lease(worker_id=wid)
                    if job is None:
                        return
                if job.id in quiet_ids:
                    with lock:
                        completed_quiet.add(job.id)
                _ack(queue, job, wid)
        except BaseException as exc:  # noqa: BLE001
            with lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=worker_loop, args=(f"mw-{i}",)) for i in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    assert errors == [], f"worker errors: {errors!r}"
    assert completed_quiet == quiet_ids
    assert queue.count(tenant_id="noisy", status=JobStatus.COMPLETED) == 40
    assert queue.count(tenant_id="quiet", status=JobStatus.COMPLETED) == 5


def test_fair_lease_priority_lane_before_low(queue: JobQueue):
    """High-priority work of any tenant runs before low-priority noise."""
    for i in range(20):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=0)
    high = queue.enqueue(tenant_id="quiet", payload={"t": "q"}, priority=10)

    first = queue.lease(worker_id="pri-w")
    assert first is not None
    assert first.id == high.id
    _ack(queue, first, "pri-w")


def test_fair_lease_high_priority_lane_fair_across_tenants(queue: JobQueue):
    """Within the high lane, tenants rotate — one tenant cannot monopolize."""
    for i in range(15):
        queue.enqueue(tenant_id="a", payload={"i": i}, priority=10)
    for i in range(15):
        queue.enqueue(tenant_id="b", payload={"i": i}, priority=10)
    # Low noise that must wait until the high lane drains.
    for i in range(10):
        queue.enqueue(tenant_id="c", payload={"i": i}, priority=0)

    first_30: list[str] = []
    for _ in range(30):
        job = queue.lease(worker_id="lane-w")
        assert job is not None
        first_30.append(job.tenant_id)
        assert job.priority == 10, "low lane must not interleave before high drains"
        _ack(queue, job, "lane-w")

    assert first_30.count("a") == 15
    assert first_30.count("b") == 15
    # Round-robin within lane: no long monopoly streak of a single tenant.
    max_streak = 1
    streak = 1
    for i in range(1, len(first_30)):
        if first_30[i] == first_30[i - 1]:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 1
    assert max_streak <= 2, f"tenant monopolized high lane: streak={max_streak}"

    low = queue.lease(worker_id="lane-w")
    assert low is not None
    assert low.priority == 0
    assert low.tenant_id == "c"


def test_fair_lease_high_of_quiet_beats_low_of_noisy(queue: JobQueue):
    """Noisy tenant flooding low priority cannot block quiet high priority."""
    for i in range(40):
        queue.enqueue(tenant_id="noisy", payload={"i": i}, priority=1)
    queue.enqueue(tenant_id="quiet", payload={"t": "q"}, priority=100)

    order: list[tuple[str, int]] = []
    for _ in range(5):
        job = queue.lease(worker_id="pri-w")
        assert job is not None
        order.append((job.tenant_id, job.priority))
        _ack(queue, job, "pri-w")

    assert order[0] == ("quiet", 100)
    # Remaining early leases are noisy low only after high is done.
    assert all(t == "noisy" and p == 1 for t, p in order[1:])


def test_fair_lease_respects_tenant_concurrency_limit(queue: JobQueue):
    queue.set_concurrency_limit("capped", 1)
    queue.enqueue(tenant_id="capped", payload={"n": 1})
    queue.enqueue(tenant_id="capped", payload={"n": 2})
    queue.enqueue(tenant_id="other", payload={"n": 3})

    first = queue.lease(worker_id="w1")
    assert first is not None
    # Hold first without acking.
    second = queue.lease(worker_id="w2")
    assert second is not None

    if first.tenant_id == "capped":
        # Second must not be another capped pending job while first held.
        assert second.tenant_id == "other"
        third = queue.lease(worker_id="w3")
        assert third is None
    else:
        # First was other; second can be capped.
        assert first.tenant_id == "other"
        assert second.tenant_id == "capped"
        third = queue.lease(worker_id="w3")
        assert third is None  # capped at limit, other done


def test_fair_lease_respects_delay_and_dependencies(queue: JobQueue):
    from datetime import datetime, timedelta, timezone

    parent = queue.enqueue(tenant_id="t1", payload={"p": 1})
    queue.enqueue(
        tenant_id="t1",
        payload={"child": True},
        depends_on=parent.id,
    )
    queue.enqueue(
        tenant_id="t2",
        payload={"later": True},
        available_at=datetime.now(timezone.utc) + timedelta(seconds=2),
    )
    ready = queue.enqueue(tenant_id="t3", payload={"ready": True})

    first = queue.lease(worker_id="w1")
    assert first is not None
    # Child blocked; delayed not due — only parent or ready.
    assert first.id in {parent.id, ready.id}
    if first.id == parent.id:
        _ack(queue, first, "w1")
        second = queue.lease(worker_id="w1")
        assert second is not None
        assert second.id in {ready.id, parent.id} or second.payload.get("child")
    else:
        assert first.id == ready.id


def test_fair_lease_queue_name_isolation(queue: JobQueue):
    queue.enqueue(tenant_id="a", queue_name="email", payload={"q": "e"})
    queue.enqueue(tenant_id="b", queue_name="sms", payload={"q": "s"})

    job = queue.lease(worker_id="w1", queue_name="email")
    assert job is not None
    assert job.queue_name == "email"
    assert job.tenant_id == "a"
    assert queue.lease(worker_id="w1", queue_name="email") is None


def test_tenant_scoped_lease_still_works(queue: JobQueue):
    queue.enqueue(tenant_id="only-me", payload={"x": 1})
    queue.enqueue(tenant_id="other", payload={"x": 2})

    job = queue.lease(tenant_id="only-me", worker_id="w1")
    assert job is not None
    assert job.tenant_id == "only-me"
    assert queue.lease(tenant_id="only-me", worker_id="w1") is None


def test_fair_worker_processes_across_tenants(queue: JobQueue):
    queue.enqueue(tenant_id="a", payload={"n": 1})
    queue.enqueue(tenant_id="b", payload={"n": 2})

    seen: list[str] = []
    worker = Worker(queue, worker_id="fair-worker")  # no tenant_id

    @worker.handler
    def handle(job: Job) -> None:
        seen.append(job.tenant_id)

    n = worker.run(max_jobs=2, stop_when_idle=True)
    assert n == 2
    assert set(seen) == {"a", "b"}
