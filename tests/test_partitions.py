"""Queue partitions: assignment, ownership, rebalance, lease filtering."""

from __future__ import annotations

import time

import pytest

from job_queue import JobQueue, JobStatus
from tests.helpers_queue import _ack


@pytest.fixture(autouse=True)
def _strict_partition_registration(queue: JobQueue):
    """Partition suite exercises real membership (production default)."""
    queue.require_worker_registration = True
    yield
    queue.require_worker_registration = True


def test_enqueue_assigns_partition_ids(queue: JobQueue):
    n = queue.get_partition_count()
    assert n >= 1
    parts = set()
    for i in range(40):
        j = queue.enqueue(tenant_id=f"t{i % 5}", payload={"i": i})
        assert 0 <= j.partition_id < n
        parts.add(j.partition_id)
    # With 40 jobs / 5 tenants over 32 partitions, expect some spread.
    assert len(parts) >= 5


def test_registered_workers_split_partitions_and_lease_only_owned(queue: JobQueue):
    queue.set_partition_count(8)
    queue.register_worker("part-w1")
    queue.register_worker("part-w2")
    # Ownership is rebalanced on each register; re-read after both joined.
    p1 = set(queue.list_worker_partitions("part-w1"))
    p2 = set(queue.list_worker_partitions("part-w2"))
    assert p1 and p2
    assert p1.isdisjoint(p2)
    assert p1 | p2 == set(range(8))

    # Enqueue many jobs; each worker only leases its partitions.
    for i in range(40):
        queue.enqueue(tenant_id="pt", payload={"i": i})

    seen1: set[int] = set()
    seen2: set[int] = set()
    for _ in range(40):
        j1 = queue.lease(worker_id="part-w1", tenant_id="pt")
        if j1 is not None:
            assert j1.partition_id in p1
            seen1.add(j1.partition_id)
            _ack(queue, j1, "part-w1")
        j2 = queue.lease(worker_id="part-w2", tenant_id="pt")
        if j2 is not None:
            assert j2.partition_id in p2
            seen2.add(j2.partition_id)
            _ack(queue, j2, "part-w2")
    assert seen1
    assert seen2


def test_unregistered_worker_cannot_lease_any_shard(queue: JobQueue):
    """Membership required: free-for-all workers must not steal shards."""
    queue.set_partition_count(4)
    queue.register_worker("fleet-w")
    j = queue.enqueue(tenant_id="u1", payload={"x": 1})
    assert 0 <= j.partition_id < 4

    # Unregistered id must get nothing even when work is ready.
    for _ in range(5):
        assert queue.lease(worker_id="unregistered-w", tenant_id="u1") is None
        assert queue.lease(worker_id="unregistered-w") is None  # fair path too

    # Registered owner still drains.
    got = queue.lease(worker_id="fleet-w", tenant_id="u1")
    assert got is not None
    assert got.id == j.id
    _ack(queue, got, "fleet-w")


def test_unregistered_worker_cannot_lease_even_when_fleet_empty(queue: JobQueue):
    """No registered peers still blocks unregistered claims under strict mode."""
    queue.set_partition_count(4)
    queue.enqueue(tenant_id="empty-fleet", payload={"x": 1})
    assert queue.lease(worker_id="stranger", tenant_id="empty-fleet") is None
    assert queue.lease(worker_id="stranger") is None


def test_rebalance_on_worker_join_and_leave(queue: JobQueue):
    queue.set_partition_count(6)
    a = queue.register_worker("rb-a")
    assert set(a["partitions"]) == set(range(6))

    b = queue.register_worker("rb-b")
    pa = set(queue.list_worker_partitions("rb-a"))
    pb = set(b["partitions"])
    assert pa.isdisjoint(pb)
    assert pa | pb == set(range(6))
    assert len(pa) == 3 and len(pb) == 3

    queue.deregister_worker("rb-b")
    pa2 = set(queue.list_worker_partitions("rb-a"))
    assert pa2 == set(range(6))
    assert queue.list_worker_partitions("rb-b") == []


def test_rebalance_on_worker_death_timeout(queue: JobQueue):
    queue.set_partition_count(4)
    queue.register_worker("live-w", heartbeat_ttl_seconds=1)
    queue.register_worker("dying-w", heartbeat_ttl_seconds=1)
    # Let both go stale, then heartbeat only the survivor so dying-w expires.
    time.sleep(1.2)
    out = queue.worker_heartbeat("live-w", heartbeat_ttl_seconds=1)
    assert set(out["partitions"]) == set(range(4))
    # Explicit sweep should be a no-op for live-w (just refreshed).
    n = queue.process_worker_timeouts(heartbeat_ttl_seconds=1)
    assert n == 0
    live_parts = set(queue.list_worker_partitions("live-w"))
    assert live_parts == set(range(4))
    assert queue.list_worker_partitions("dying-w") == []


def test_partition_assignments_api_shape(queue: JobQueue):
    queue.set_partition_count(4)
    queue.register_worker("shape-w")
    rows = queue.list_partition_assignments()
    assert len(rows) == 4
    assert all(r["worker_id"] == "shape-w" for r in rows)


def test_dead_worker_cannot_lease_after_rebalance(queue: JobQueue):
    """Once marked dead, a worker must not steal partitions (empty filter)."""
    queue.set_partition_count(4)
    queue.register_worker("alive-w")
    queue.register_worker("zombie-w")
    for i in range(12):
        queue.enqueue(tenant_id="dz", payload={"i": i})

    queue.deregister_worker("zombie-w")
    # Zombie still tries to lease under its old worker_id — must get nothing.
    for _ in range(5):
        j = queue.lease(worker_id="zombie-w", tenant_id="dz")
        assert j is None

    # Survivor drains the backlog.
    n = 0
    for _ in range(20):
        j = queue.lease(worker_id="alive-w", tenant_id="dz")
        if j is None:
            break
        _ack(queue, j, "alive-w")
        n += 1
    assert n == 12


def test_shrink_partitions_remaps_jobs_so_they_remain_claimable(queue: JobQueue):
    """Shrinking partition_count must not strand jobs on deleted shards."""
    queue.set_partition_count(16)
    queue.register_worker("shrink-w")

    jobs = [queue.enqueue(tenant_id="sh", payload={"i": i}) for i in range(48)]
    high = [j for j in jobs if j.partition_id >= 4]
    # With 48 jobs over 16 shards we expect some in the upper range.
    assert high, "expected some jobs on partitions >= 4 to exercise shrink"

    queue.set_partition_count(4)
    assert queue.get_partition_count() == 4
    assert set(queue.list_worker_partitions("shrink-w")) == set(range(4))

    # Every pending job must sit in the live range.
    for j in jobs:
        refreshed = queue.get(j.id, tenant_id="sh")
        assert refreshed is not None
        assert refreshed.status == JobStatus.PENDING
        assert 0 <= refreshed.partition_id < 4

    # Registered worker drains the full backlog (nothing stranded).
    claimed = 0
    for _ in range(80):
        got = queue.lease(worker_id="shrink-w", tenant_id="sh")
        if got is None:
            break
        assert 0 <= got.partition_id < 4
        _ack(queue, got, "shrink-w")
        claimed += 1
    assert claimed == len(jobs)


def test_shrink_partitions_remaps_even_without_workers(queue: JobQueue):
    """Rematerialize runs on shrink even before any worker has registered."""
    queue.set_partition_count(8)
    jobs = [queue.enqueue(tenant_id="nw", payload={"i": i}) for i in range(20)]
    assert any(j.partition_id >= 2 for j in jobs)

    queue.set_partition_count(2)
    for j in jobs:
        refreshed = queue.get(j.id, tenant_id="nw")
        assert refreshed is not None
        assert 0 <= refreshed.partition_id < 2

    # After a worker joins the shrunk map, all work is claimable.
    queue.register_worker("late-w")
    n = 0
    for _ in range(40):
        got = queue.lease(worker_id="late-w", tenant_id="nw")
        if got is None:
            break
        _ack(queue, got, "late-w")
        n += 1
    assert n == len(jobs)


def test_rebalance_bumps_generation_on_owner_change(queue: JobQueue):
    queue.set_partition_count(4)
    queue.register_worker("gen-a")
    with queue._pool.connection() as conn:
        gens1 = queue._owned_partition_generations_locked(conn, "gen-a")
    assert set(gens1) == set(range(4))
    # Second worker steals half the shards → those generations must bump.
    queue.register_worker("gen-b")
    with queue._pool.connection() as conn:
        gens_a = queue._owned_partition_generations_locked(conn, "gen-a")
        gens_b = queue._owned_partition_generations_locked(conn, "gen-b")
    assert gens_a and gens_b
    assert set(gens_a).isdisjoint(set(gens_b))
    # Partitions that moved off gen-a should no longer appear; those that stayed
    # keep their generation; new owner sees generation >= 1.
    assert all(g >= 1 for g in gens_b.values())


def test_guard_rejects_lost_partition_and_stale_generation(queue: JobQueue):
    """FOR SHARE ownership + generation fence abort post-rebalance claims."""
    from job_queue.queue._common import LeaseError

    queue.set_partition_count(4)
    queue.register_worker("own-a")
    with queue._pool.connection() as conn:
        gens = queue._owned_partition_generations_locked(conn, "own-a")
    assert 0 in gens
    old_gen0 = gens[0]

    queue.register_worker("own-b")
    still = set(queue.list_worker_partitions("own-a"))
    lost = [p for p in range(4) if p not in still]
    assert lost, "expected rebalance to move some partitions"

    with queue._pool.connection() as conn:
        # Lost shard: ownership check fails.
        with pytest.raises(LeaseError, match="not owned|rebalanced"):
            queue._guard_partition_claim(
                conn,
                worker_id="own-a",
                partition_id=lost[0],
                expected_generation=old_gen0,
            )
        # Still-owned shard with wrong generation: generation fence fails.
        kept = sorted(still)[0]
        with queue._pool.connection() as c2:
            cur = queue._owned_partition_generations_locked(c2, "own-a")[kept]
        with pytest.raises(LeaseError, match="generation mismatch"):
            queue._guard_partition_claim(
                conn,
                worker_id="own-a",
                partition_id=kept,
                expected_generation=cur + 99,
            )
        # Correct owner + generation succeeds.
        queue._guard_partition_claim(
            conn,
            worker_id="own-a",
            partition_id=kept,
            expected_generation=cur,
        )


def test_stale_partition_snapshot_cannot_claim_after_rebalance(queue: JobQueue):
    """Stale owned_parts list must not lease shards lost in rebalance.

    Simulates the TOCTOU where a worker captured partitions before join, then
    claim runs with that snapshot after ownership moved.
    """
    from datetime import datetime, timezone

    from job_queue.db import transaction

    queue.set_partition_count(8)
    queue.register_worker("snap-a")
    stale_parts = list(range(8))  # snapshot: believed to own everything
    with queue._pool.connection() as conn:
        stale_gens = queue._owned_partition_generations_locked(conn, "snap-a")

    for i in range(40):
        queue.enqueue(tenant_id="snap", payload={"i": i})

    # Peer joins → snap-a loses half the shards; generations bump on movers.
    queue.register_worker("snap-b")
    live = set(queue.list_worker_partitions("snap-a"))
    assert live and live != set(stale_parts)

    now = datetime.now(timezone.utc)
    stolen: list[int] = []
    claimed_jobs = []
    # Fair claim path with *stale* partition list + generations.
    # Commit each claim before ack (do not nest ack under the claim txn).
    for _ in range(40):
        with transaction(queue._pool) as conn:
            job = queue._try_claim_fair_candidate(
                conn,
                worker_id="snap-a",
                queue_name="default",
                visibility_timeout_seconds=30,
                now=now,
                partitions=stale_parts,
                partition_generations=stale_gens,
            )
            if job is None:
                break
            stolen.append(int(job.partition_id))
            claimed_jobs.append(job)
    for job in claimed_jobs:
        _ack(queue, job, "snap-a")

    # May claim some still-owned shards, but never a reassigned one.
    assert all(p in live for p in stolen), f"stole lost partitions: {stolen} live={live}"

    # Remaining work is claimable by the new owner.
    n_b = 0
    for _ in range(40):
        j = queue.lease(worker_id="snap-b", tenant_id="snap")
        if j is None:
            break
        assert j.partition_id in set(queue.list_worker_partitions("snap-b"))
        _ack(queue, j, "snap-b")
        n_b += 1
    assert n_b >= 1 or stolen  # fleet made progress under the fence
