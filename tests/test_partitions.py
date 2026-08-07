"""Queue partitions: assignment, ownership, rebalance, lease filtering."""

from __future__ import annotations

import time

from job_queue import JobQueue
from tests.helpers_queue import _ack


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


def test_unregistered_worker_leases_all_partitions(queue: JobQueue):
    """Legacy / unit-test path: no registration → full queue access."""
    queue.set_partition_count(4)
    queue.register_worker("other-w")
    queue.enqueue(tenant_id="u1", payload={"x": 1})
    j = queue.lease(worker_id="unregistered-w", tenant_id="u1")
    assert j is not None
    _ack(queue, j, "unregistered-w")


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
