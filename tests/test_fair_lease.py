"""Fair multi-tenant leasing."""

from __future__ import annotations

import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from job_queue import Job, JobQueue, JobStatus, Worker
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.remote import CoordinatorClient

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


def test_fair_multi_worker_respects_per_tenant_cap(queue: JobQueue):
    """High concurrency fair lease must not exceed per-tenant caps."""
    import threading

    tenants = [f"ft{i}" for i in range(5)]
    for tid in tenants:
        queue.set_concurrency_limit(tid, 2)
        for j in range(20):
            queue.enqueue(tenant_id=tid, payload={"j": j})

    barrier = threading.Barrier(16)
    held: list = []
    lock = threading.Lock()

    def worker(wid: str) -> None:
        barrier.wait()
        batch = queue.lease_batch(worker_id=wid, limit=4)  # fair multi-tenant
        with lock:
            held.extend(batch)

    threads = [threading.Thread(target=worker, args=(f"fw{i}",)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # No duplicate claims.
    ids = [j.id for j in held]
    assert len(ids) == len(set(ids))
    # Per-tenant leased count <= 2.
    from collections import Counter

    by_tenant = Counter(j.tenant_id for j in held)
    for tid, n in by_tenant.items():
        assert n <= 2, f"tenant {tid} leased {n} > cap 2"
    for tid in tenants:
        assert queue.count(tenant_id=tid, status=JobStatus.LEASED) <= 2


def test_lease_weight_default_is_one(queue: JobQueue):
    assert queue.get_tenant_lease_weight("never-set") == 1
    assert queue.set_tenant_lease_weight("never-set", None) == 1
    stats = queue.get_tenant_stats("ghost")
    assert stats.lease_weight == 1


def test_lease_weight_validation(queue: JobQueue):
    with pytest.raises(ValueError, match="lease_weight"):
        queue.set_tenant_lease_weight("t", 0)
    with pytest.raises(ValueError, match="lease_weight"):
        queue.set_tenant_lease_weight("t", 101)
    with pytest.raises(ValueError, match="tenant_id"):
        queue.set_tenant_lease_weight("", 2)


def test_lease_weight_gives_more_turns_without_starving(queue: JobQueue):
    """WFQ: weight 4/2/1 yields ~4:2:1 over a full virtual-time cycle."""
    queue.set_tenant_lease_weight("heavy", 4)
    queue.set_tenant_lease_weight("mid", 2)
    # light stays at the default of 1
    assert queue.get_tenant_lease_weight("light") == 1
    assert queue.get_tenant_stats("heavy").lease_weight == 4

    for tid in ("heavy", "mid", "light"):
        for i in range(40):
            queue.enqueue(tenant_id=tid, payload={"t": tid, "i": i})

    order: list[str] = []
    for _ in range(28):
        job = queue.lease(worker_id="wfq-w")
        assert job is not None
        order.append(job.tenant_id)
        _ack(queue, job, "wfq-w")

    assert order.count("heavy") == 16
    assert order.count("mid") == 8
    assert order.count("light") == 4
    # Low-weight tenant is served in the first cycle, not after the backlog.
    assert "light" in order[:7]


def test_lease_weight_equal_weights_still_round_robin(queue: JobQueue):
    queue.set_tenant_lease_weight("a", 3)
    queue.set_tenant_lease_weight("b", 3)
    for tid in ("a", "b"):
        for i in range(10):
            queue.enqueue(tenant_id=tid, payload={"t": tid, "i": i})

    first: list[str] = []
    for _ in range(4):
        job = queue.lease(worker_id="eq-w")
        assert job is not None
        first.append(job.tenant_id)
        _ack(queue, job, "eq-w")
    assert first.count("a") == 2
    assert first.count("b") == 2


def test_lease_weight_reset_returns_to_one(queue: JobQueue):
    queue.set_tenant_lease_weight("was-heavy", 8)
    assert queue.set_tenant_lease_weight("was-heavy", None) == 1
    assert queue.get_tenant_lease_weight("was-heavy") == 1

    queue.set_tenant_lease_weight("peer", 8)
    for tid in ("was-heavy", "peer"):
        for i in range(8):
            queue.enqueue(tenant_id=tid, payload={"t": tid, "i": i})

    order: list[str] = []
    for _ in range(9):
        job = queue.lease(worker_id="rst-w")
        assert job is not None
        order.append(job.tenant_id)
        _ack(queue, job, "rst-w")
    # 8:1 → eight peer, one was-heavy in the first cycle of 9.
    assert order.count("peer") == 8
    assert order.count("was-heavy") == 1


def test_lease_weight_ignored_on_scoped_lease(queue: JobQueue):
    queue.set_tenant_lease_weight("heavy", 10)
    queue.set_tenant_lease_weight("light", 1)
    for i in range(10):
        queue.enqueue(tenant_id="heavy", payload={"i": i})
    light = queue.enqueue(tenant_id="light", payload={"only": True})

    got = queue.lease(tenant_id="light", worker_id="scoped")
    assert got is not None
    assert got.id == light.id
    _ack(queue, got, "scoped")


def test_lease_weight_priority_still_wins(queue: JobQueue):
    queue.set_tenant_lease_weight("heavy", 10)
    for i in range(20):
        queue.enqueue(tenant_id="heavy", payload={"i": i}, priority=0)
    high = queue.enqueue(tenant_id="light", payload={"t": "q"}, priority=10)

    first = queue.lease(worker_id="pri-w")
    assert first is not None
    assert first.id == high.id
    _ack(queue, first, "pri-w")


def test_lease_weight_export_restore(queue: JobQueue):
    queue.set_tenant_lease_weight("src-w", 7)
    queue.enqueue(tenant_id="src-w", payload={"n": 1})
    snap = queue.export_tenant("src-w")
    rst = queue.restore_tenant(
        "dst-w", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    assert queue.get_tenant_lease_weight("dst-w") == 7
    assert queue.get_tenant_stats("dst-w").lease_weight == 7


def test_lease_weight_http_roundtrip(queue: JobQueue):
    coord = Coordinator(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        client = CoordinatorClient(f"http://127.0.0.1:{server.server_address[1]}")
        unset = client.get_tenant_lease_weight("http-w")
        assert unset["lease_weight"] == 1
        set_w = client.set_tenant_lease_weight("http-w", 5)
        assert set_w["lease_weight"] == 5
        got = client.get_tenant_lease_weight("http-w")
        assert got["lease_weight"] == 5
        client.enqueue(tenant_id="http-w", payload={"n": 1})
        job = client.lease(tenant_id="http-w", worker_id="w")
        assert job is not None
        assert job.lease_token is not None
        client.ack(
            job.id,
            tenant_id="http-w",
            worker_id="w",
            lease_token=job.lease_token,
        )
        cleared = client.set_tenant_lease_weight("http-w", clear=True)
        assert cleared["lease_weight"] == 1
        again = client.get_tenant_lease_weight("http-w")
        assert again["lease_weight"] == 1
    finally:
        server.shutdown()
