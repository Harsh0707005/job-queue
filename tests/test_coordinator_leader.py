"""One coordinator leads control ticks; failover when it dies."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, WebhookEvent
from job_queue.queue.coordinator_leader import HandoffError
from job_queue.coordinator import Coordinator, _make_handler
from job_queue.queue._common import _utcnow
from job_queue.remote import CoordinatorClient
from tests.helpers_queue import _WebhookCapture, _ack
from tests.test_schedule_dag import _make_due


def test_acquire_is_exclusive(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    first = queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert first is not None
    assert first.node_id == "n1"
    assert first.epoch == 1
    assert (
        queue.acquire_or_renew_control_leader(node_id="n2", lease_token=b) is None
    )
    again = queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert again is not None
    assert again.epoch == 1
    got = queue.get_control_leader()
    assert got is not None
    assert got.node_id == "n1"
    assert queue.holds_control_leader(node_id="n1", lease_token=a)
    assert not queue.holds_control_leader(node_id="n2", lease_token=b)


def test_expired_lease_is_stolen_and_epoch_advances(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    first = queue.acquire_or_renew_control_leader(
        node_id="n1", lease_token=a, ttl_seconds=0.15
    )
    assert first is not None
    time.sleep(0.2)
    stolen = queue.acquire_or_renew_control_leader(
        node_id="n2", lease_token=b, ttl_seconds=2.0
    )
    assert stolen is not None
    assert stolen.node_id == "n2"
    assert stolen.epoch == 2
    assert not queue.holds_control_leader(node_id="n1", lease_token=a)
    assert (
        queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
        is None
    )


def test_release_lets_peer_take_over(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    assert queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    assert queue.release_control_leader(node_id="n1", lease_token=a) is True
    next_l = queue.acquire_or_renew_control_leader(node_id="n2", lease_token=b)
    assert next_l is not None
    assert next_l.node_id == "n2"
    assert next_l.epoch == 2


def test_bump_ticks_only_while_holding(queue: JobQueue):
    a = uuid4()
    b = uuid4()
    queue.acquire_or_renew_control_leader(node_id="n1", lease_token=a)
    bumped = queue.bump_control_tick(lease_token=a, kind="schedules", count=3)
    assert bumped is not None
    assert bumped.schedule_ticks == 3
    assert queue.bump_control_tick(lease_token=b, kind="schedules", count=9) is None
    got = queue.get_control_leader()
    assert got is not None
    assert got.schedule_ticks == 3


def test_two_control_loops_one_leader(queue: JobQueue):
    c1 = Coordinator(queue, node_id="loop-a")
    c2 = Coordinator(queue, node_id="loop-b")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    c2.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    try:
        deadline = time.time() + 3
        leaders: set[str] = set()
        while time.time() < deadline:
            leaders = {
                n
                for n, c in (("loop-a", c1), ("loop-b", c2))
                if c._control_loop is not None and c._control_loop.is_leader()
            }
            if len(leaders) == 1:
                break
            time.sleep(0.05)
        assert len(leaders) == 1
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id in ("loop-a", "loop-b")
    finally:
        c1.stop_control()
        c2.stop_control()


def test_failover_when_leader_stops(queue: JobQueue):
    c1 = Coordinator(queue, node_id="lead")
    c2 = Coordinator(queue, node_id="standby")
    c1.start_control(ttl_seconds=0.4, interval_seconds=0.08)
    c2.start_control(ttl_seconds=0.4, interval_seconds=0.08)
    try:
        deadline = time.time() + 3
        first = None
        while time.time() < deadline:
            if c1._control_loop is not None and c1._control_loop.is_leader():
                first, other = c1, c2
                break
            if c2._control_loop is not None and c2._control_loop.is_leader():
                first, other = c2, c1
                break
            time.sleep(0.05)
        else:
            raise AssertionError("neither coordinator became leader")
        first.stop_control()
        deadline = time.time() + 4
        while time.time() < deadline:
            if other._control_loop is not None and other._control_loop.is_leader():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("standby never took over")
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id == other.node_id
        assert lease.epoch >= 2
    finally:
        c1.stop_control()
        c2.stop_control()


def test_leader_fires_schedule_follower_does_not_double(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    c1 = Coordinator(queue, node_id="sched-a")
    c2 = Coordinator(queue, node_id="sched-b")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.08)
    c2.start_control(ttl_seconds=2.0, interval_seconds=0.08)
    try:
        deadline = time.time() + 4
        jobs = []
        while time.time() < deadline:
            jobs = queue.list_jobs(tenant_id="t1")
            if jobs:
                break
            time.sleep(0.05)
        assert len(jobs) == 1
        assert jobs[0].schedule_id == sched.id
        time.sleep(0.3)
        assert len(queue.list_jobs(tenant_id="t1")) == 1
    finally:
        c1.stop_control()
        c2.stop_control()


def _serve(coord: Coordinator) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(coord))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_follower_http_control_returns_503(queue: JobQueue):
    c1 = Coordinator(queue, node_id="http-lead")
    c2 = Coordinator(queue, node_id="http-follow")
    c1.start_control(ttl_seconds=3.0, interval_seconds=0.1)
    c2.start_control(ttl_seconds=3.0, interval_seconds=0.1)
    s1, url1 = _serve(c1)
    s2, url2 = _serve(c2)
    try:
        deadline = time.time() + 5
        lead = follow = None
        while time.time() < deadline:
            if c1._control_loop is not None and c1._control_loop.is_leader():
                lead, follow, follow_url = c1, c2, url2
                break
            if c2._control_loop is not None and c2._control_loop.is_leader():
                lead, follow, follow_url = c2, c1, url1
                break
            time.sleep(0.05)
        else:
            raise AssertionError("leader not elected")
        status, body = follow.handle("POST", "/v1/schedules/process-due", {}, {})
        assert status == 503
        assert body.get("code") == "not_leader"
        status, body = follow.handle("POST", "/v1/alerts/evaluate", {}, {})
        assert status == 503
        status, body = follow.handle("POST", "/v1/admin/purge-expired-jobs", {}, {})
        assert status == 503
        status, body = follow.handle(
            "POST", "/v1/admin/payload-keys/advance", {}, {}
        )
        assert status == 503
        status, body = follow.handle(
            "POST", "/v1/admin/restores/advance", {}, {}
        )
        assert status == 503
        # Worker APIs stay up on the follower.
        status, body = follow.handle(
            "POST",
            "/v1/jobs",
            {"tenant_id": "t1", "payload": {"ok": True}},
            {},
        )
        assert status in (200, 201)
        h = CoordinatorClient(follow_url).health()
        assert h.get("ok") is True
        assert h.get("leader") is False
        assert h.get("role") == "standby"
        assert h.get("leader_node") == lead.node_id
        assert lead.node_id not in (h.get("standbys") or [])
        status, body = lead.handle("GET", "/v1/coordinator/leader", {}, {})
        assert status == 200
        assert body.get("leader") is True
        assert isinstance(body.get("lease"), dict)
        assert body["lease"]["node_id"] == lead.node_id
    finally:
        s1.shutdown()
        s2.shutdown()
        c1.stop_control()
        c2.stop_control()


def test_http_process_due_without_control_still_works(queue: JobQueue):
    coord = Coordinator(queue, node_id="solo")
    server, url = _serve(coord)
    try:
        client = CoordinatorClient(url)
        assert client.health()["ok"] is True
        queue.create_schedule(
            tenant_id="t1",
            interval_seconds=60,
            payload={"n": 1},
        )
        n = client.process_due_schedules()
        assert n >= 1
    finally:
        server.shutdown()


def test_old_leader_fenced_after_lease_stolen(queue: JobQueue):
    """After a steal, the old token must not fire schedules or claim webhooks."""
    from job_queue import WebhookEvent

    old_tok = uuid4()
    new_tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=old_tok, ttl_seconds=0.2
    )
    assert held is not None
    old_epoch = held.epoch

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    queue.register_webhook(
        tenant_id="t1",
        url="http://127.0.0.1:9/hook",
        events=[WebhookEvent.JOB_COMPLETED.value],
    )
    job = queue.enqueue(tenant_id="t1", payload={"x": 1})
    leased = queue.lease(tenant_id="t1", worker_id="w")
    assert leased is not None
    _ack(queue, leased, "w")
    pending_hooks = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
    assert pending_hooks

    time.sleep(0.25)
    stolen = queue.acquire_or_renew_control_leader(
        node_id="new", lease_token=new_tok, ttl_seconds=5.0
    )
    assert stolen is not None
    assert stolen.epoch == old_epoch + 1
    assert stolen.node_id == "new"

    # Simulated stale tick from the deposed leader.
    assert (
        queue.process_due_schedules(
            tenant_id="t1", leader_token=old_tok, leader_epoch=old_epoch
        )
        == []
    )
    claimed, auto = queue.claim_webhook_deliveries(
        leader_token=old_tok, leader_epoch=old_epoch
    )
    assert claimed == []
    assert auto == []
    ev = queue.evaluate_alerts(leader_token=old_tok, leader_epoch=old_epoch)
    assert ev.fired == []
    purged = queue.purge_expired_jobs(
        leader_token=old_tok, leader_epoch=old_epoch
    )
    assert purged.total_deleted == 0
    sched_jobs = [
        j for j in queue.list_jobs(tenant_id="t1") if j.schedule_id == sched.id
    ]
    assert sched_jobs == []
    still_pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
    assert len(still_pending) == len(pending_hooks)

    # The new leader can still do the work.
    fires = queue.process_due_schedules(
        tenant_id="t1", leader_token=new_tok, leader_epoch=stolen.epoch
    )
    assert [f.schedule.id for f in fires] == [sched.id]
    claimed, _ = queue.claim_webhook_deliveries(
        leader_token=new_tok, leader_epoch=stolen.epoch
    )
    assert len(claimed) == len(pending_hooks)


def test_control_loop_paused_tick_after_steal_does_not_fire(queue: JobQueue):
    """Old loop acquires, lease moves, then its delayed tick must no-op."""
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "fenced"},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)

    paused = threading.Event()
    resume = threading.Event()
    old = Coordinator(queue, node_id="stale")
    new = Coordinator(queue, node_id="fresh")

    def hold_after_acquire(loop, lease) -> None:
        if loop.node_id != "stale":
            return
        if paused.is_set():
            return
        paused.set()
        resume.wait(timeout=5)

    old.start_control(
        ttl_seconds=0.35,
        interval_seconds=0.08,
        before_tick=hold_after_acquire,
    )
    try:
        deadline = time.time() + 4
        while time.time() < deadline:
            if paused.is_set():
                break
            time.sleep(0.02)
        else:
            raise AssertionError("old leader never reached before_tick")
        assert queue.list_jobs(tenant_id="t1") == []

        time.sleep(0.4)
        new.start_control(ttl_seconds=2.0, interval_seconds=0.08)
        deadline = time.time() + 4
        while time.time() < deadline:
            if new._control_loop is not None and new._control_loop.is_leader():
                break
            time.sleep(0.02)
        else:
            raise AssertionError("new leader never took over")
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id == "fresh"
        assert lease.epoch >= 2

        deadline = time.time() + 3
        while time.time() < deadline:
            jobs = [
                j
                for j in queue.list_jobs(tenant_id="t1")
                if j.schedule_id == sched.id
            ]
            if jobs:
                break
            time.sleep(0.05)
        assert len(jobs) == 1

        resume.set()
        time.sleep(0.4)
        later = [
            j
            for j in queue.list_jobs(tenant_id="t1")
            if j.schedule_id == sched.id
        ]
        assert len(later) == 1
    finally:
        resume.set()
        old.stop_control()
        new.stop_control()


def test_handoff_old_leader_stops_new_claims_immediately(queue: JobQueue):
    old_tok = uuid4()
    new_tok = uuid4()
    other_tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=old_tok, ttl_seconds=30.0
    )
    assert held is not None
    old_epoch = held.epoch

    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    queue.touch_control_node("new")

    out = queue.handoff_control_leader("new", handoff_seconds=15.0)
    assert out.handoff_to == "new"
    assert out.epoch == old_epoch + 1
    assert not queue.holds_control_leader(node_id="old", lease_token=old_tok)

    # Old holder cannot renew or fire after the operator handoff.
    assert (
        queue.acquire_or_renew_control_leader(
            node_id="old", lease_token=old_tok, ttl_seconds=30.0
        )
        is None
    )
    assert (
        queue.process_due_schedules(
            tenant_id="t1", leader_token=old_tok, leader_epoch=old_epoch
        )
        == []
    )
    # A third node cannot sneak in while the handoff is pending.
    assert (
        queue.acquire_or_renew_control_leader(
            node_id="other", lease_token=other_tok, ttl_seconds=30.0
        )
        is None
    )

    claimed = queue.acquire_or_renew_control_leader(
        node_id="new", lease_token=new_tok, ttl_seconds=30.0
    )
    assert claimed is not None
    assert claimed.node_id == "new"
    assert claimed.handoff_to is None
    assert claimed.epoch >= old_epoch + 2
    fires = queue.process_due_schedules(
        tenant_id="t1",
        leader_token=new_tok,
        leader_epoch=claimed.epoch,
    )
    assert [f.schedule.id for f in fires] == [sched.id]


def test_handoff_to_current_leader_is_noop(queue: JobQueue):
    tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="keep", lease_token=tok, ttl_seconds=10.0
    )
    assert held is not None
    again = queue.handoff_control_leader("keep")
    assert again.node_id == "keep"
    assert again.epoch == held.epoch
    assert again.handoff_to is None
    assert queue.holds_control_leader(node_id="keep", lease_token=tok)


def test_handoff_unknown_node_refused_leader_keeps_ticking(queue: JobQueue):
    tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=tok, ttl_seconds=30.0
    )
    assert held is not None
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    with pytest.raises(HandoffError, match="not a live coordinator") as ei:
        queue.handoff_control_leader("no-such-node", handoff_seconds=30.0)
    assert ei.value.code == "handoff_target_down"
    lease = queue.get_control_leader()
    assert lease is not None
    assert lease.handoff_to is None
    assert lease.epoch == held.epoch
    assert queue.holds_control_leader(node_id="old", lease_token=tok)
    fires = queue.process_due_schedules(
        tenant_id="t1", leader_token=tok, leader_epoch=held.epoch
    )
    assert [f.schedule.id for f in fires] == [sched.id]


def test_cancel_handoff_to_missing_target_lets_old_leader_resume(queue: JobQueue):
    old_tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=old_tok, ttl_seconds=30.0
    )
    assert held is not None
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"n": 1},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    # Dest looked live, then never claimed (down / name that went stale).
    queue.touch_control_node("ghost")
    parked = queue.handoff_control_leader("ghost", handoff_seconds=30.0)
    assert parked.handoff_to == "ghost"
    assert (
        queue.process_due_schedules(
            tenant_id="t1", leader_token=old_tok, leader_epoch=held.epoch
        )
        == []
    )

    cleared = queue.cancel_control_leader_handoff()
    assert cleared is not None
    assert cleared.handoff_to is None
    again = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=old_tok, ttl_seconds=30.0
    )
    assert again is not None
    assert again.node_id == "old"
    fires = queue.process_due_schedules(
        tenant_id="t1", leader_token=old_tok, leader_epoch=again.epoch
    )
    assert [f.schedule.id for f in fires] == [sched.id]


def test_cancel_handoff_lets_other_live_node_take_over(queue: JobQueue):
    old_tok = uuid4()
    other_tok = uuid4()
    held = queue.acquire_or_renew_control_leader(
        node_id="old", lease_token=old_tok, ttl_seconds=30.0
    )
    assert held is not None
    queue.touch_control_node("ghost")
    queue.handoff_control_leader("ghost", handoff_seconds=30.0)
    assert (
        queue.acquire_or_renew_control_leader(
            node_id="other", lease_token=other_tok, ttl_seconds=30.0
        )
        is None
    )
    queue.cancel_control_leader_handoff()
    stolen = queue.acquire_or_renew_control_leader(
        node_id="other", lease_token=other_tok, ttl_seconds=30.0
    )
    assert stolen is not None
    assert stolen.node_id == "other"
    assert stolen.handoff_to is None


def test_http_handoff_unknown_is_409_and_cancel_unblocks(queue: JobQueue):
    lead = Coordinator(queue, node_id="http-keep")
    lead.start_control(ttl_seconds=30.0, interval_seconds=0.1)
    server, url = _serve(lead)
    try:
        deadline = time.time() + 4
        while time.time() < deadline:
            if lead._control_loop is not None and lead._control_loop.is_leader():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("leader not elected")
        status, body = lead.handle(
            "POST",
            "/v1/coordinator/leader/handoff",
            {"to_node_id": "typo-node", "handoff_seconds": 30},
            {},
        )
        assert status == 409
        assert body.get("code") == "handoff_target_down"
        assert lead._control_loop.is_leader()

        queue.touch_control_node("ghost")
        status, body = lead.handle(
            "POST",
            "/v1/coordinator/leader/handoff",
            {"to_node_id": "ghost", "handoff_seconds": 30},
            {},
        )
        assert status == 200
        assert body.get("handoff_to") == "ghost"
        deadline = time.time() + 3
        while time.time() < deadline:
            if not lead._control_loop.is_leader():
                break
            time.sleep(0.05)
        client = CoordinatorClient(url)
        cancelled = client.cancel_leader_handoff()
        assert cancelled.get("cancelled") is True
        deadline = time.time() + 4
        while time.time() < deadline:
            if lead._control_loop.is_leader():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("leader did not resume after cancel")
    finally:
        server.shutdown()
        lead.stop_control()


def test_handoff_control_loops_old_stops_new_ticks(queue: JobQueue):
    sched = queue.create_schedule(
        tenant_id="t1",
        interval_seconds=3600,
        payload={"who": "handoff"},
        start_at=_utcnow() + timedelta(hours=1),
    )
    _make_due(queue, sched.id)
    old = Coordinator(queue, node_id="coord-old")
    new = Coordinator(queue, node_id="coord-new")
    old.start_control(ttl_seconds=30.0, interval_seconds=0.08)
    new.start_control(ttl_seconds=30.0, interval_seconds=0.08)
    try:
        deadline = time.time() + 4
        lead = follow = None
        while time.time() < deadline:
            if old._control_loop is not None and old._control_loop.is_leader():
                lead, follow = old, new
                break
            if new._control_loop is not None and new._control_loop.is_leader():
                lead, follow = new, old
                break
            time.sleep(0.05)
        else:
            raise AssertionError("neither coordinator became leader")
        deadline = time.time() + 3
        while time.time() < deadline:
            if any(
                j.schedule_id == sched.id for j in queue.list_jobs(tenant_id="t1")
            ):
                break
            time.sleep(0.05)
        first_n = len(
            [j for j in queue.list_jobs(tenant_id="t1") if j.schedule_id == sched.id]
        )
        assert first_n == 1

        queue.handoff_control_leader(follow.node_id, handoff_seconds=15.0)
        deadline = time.time() + 4
        while time.time() < deadline:
            if follow._control_loop is not None and follow._control_loop.is_leader():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("handoff target never claimed")
        assert lead._control_loop is None or not lead._control_loop.is_leader()
        lease = queue.get_control_leader()
        assert lease is not None
        assert lease.node_id == follow.node_id
        time.sleep(0.4)
        later = [
            j for j in queue.list_jobs(tenant_id="t1") if j.schedule_id == sched.id
        ]
        assert len(later) == 1
    finally:
        old.stop_control()
        new.stop_control()


def test_handoff_http_roundtrip(queue: JobQueue):
    c1 = Coordinator(queue, node_id="http-a")
    c2 = Coordinator(queue, node_id="http-b")
    c1.start_control(ttl_seconds=30.0, interval_seconds=0.1)
    c2.start_control(ttl_seconds=30.0, interval_seconds=0.1)
    s1, url1 = _serve(c1)
    s2, url2 = _serve(c2)
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            if c1._control_loop is not None and c1._control_loop.is_leader():
                lead_url, dest = url1, "http-b"
                break
            if c2._control_loop is not None and c2._control_loop.is_leader():
                lead_url, dest = url2, "http-a"
                break
            time.sleep(0.05)
        else:
            raise AssertionError("no leader")
        client = CoordinatorClient([url1, url2])
        out = client.handoff_leader(dest, handoff_seconds=15.0)
        assert (out.get("handoff_to") or (out.get("lease") or {}).get("handoff_to")) == dest
        deadline = time.time() + 5
        while time.time() < deadline:
            h = CoordinatorClient(lead_url).health()
            other = url2 if lead_url == url1 else url1
            dest_h = CoordinatorClient(other).health()
            if dest_h.get("leader") is True and dest_h.get("node") == dest:
                assert h.get("leader") is False
                break
            time.sleep(0.05)
        else:
            raise AssertionError("http handoff never landed")
        status, body = (
            c1 if dest == "http-b" else c2
        ).handle("POST", "/v1/schedules/process-due", {}, {})
        assert status == 503
    finally:
        s1.shutdown()
        s2.shutdown()
        c1.stop_control()
        c2.stop_control()


def _seed_webhook_jobs(queue: JobQueue, capture: _WebhookCapture, n: int) -> None:
    queue.register_webhook(
        tenant_id="t1",
        url=capture.url,
        events=[WebhookEvent.JOB_COMPLETED.value],
        max_attempts=3,
    )
    for i in range(n):
        queue.enqueue(tenant_id="t1", payload={"i": i})
        job = queue.lease(tenant_id="t1", worker_id="w")
        assert job is not None
        _ack(queue, job, "w", result={"i": i})
    pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
    assert len(pending) == n


def test_handoff_mid_webhook_batch_does_not_double_post(queue: JobQueue):
    """Old leader stops mid-batch; leftovers are posted once by the new leader."""
    capture = _WebhookCapture()
    try:
        _seed_webhook_jobs(queue, capture, 3)
        old_tok = uuid4()
        new_tok = uuid4()
        held = queue.acquire_or_renew_control_leader(
            node_id="old", lease_token=old_tok, ttl_seconds=30.0
        )
        assert held is not None
        seen: list = []

        def before_post(item) -> None:
            seen.append(item.delivery.id)
            if len(seen) == 2:
                queue.touch_control_node("new")
                queue.handoff_control_leader("new", handoff_seconds=15.0)

        old_out = queue.process_webhook_deliveries(
            limit=10,
            claim_seconds=60,
            leader_token=old_tok,
            leader_epoch=held.epoch,
            on_before_post=before_post,
        )
        # First item posted; second aborted at the hook; third never posted.
        assert len(capture.received) == 1
        succeeded = [r for r in old_out if r.success]
        assert len(succeeded) == 1
        leftover = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(leftover) == 2

        claimed = queue.acquire_or_renew_control_leader(
            node_id="new", lease_token=new_tok, ttl_seconds=30.0
        )
        assert claimed is not None
        queue.process_webhook_deliveries(
            limit=10,
            claim_seconds=60,
            leader_token=new_tok,
            leader_epoch=claimed.epoch,
        )
        assert len(capture.received) == 3
        bodies = [r["body"].get("result", r["body"]) for r in capture.received]
        # Each job result posted exactly once.
        got_i = sorted(
            (b.get("i") if isinstance(b, dict) else None) for b in bodies
        )
        assert got_i == [0, 1, 2]
        assert queue.list_webhook_deliveries(tenant_id="t1", status="pending") == []
        done = queue.list_webhook_deliveries(tenant_id="t1", status="succeeded")
        assert len(done) == 3
    finally:
        capture.stop()


def test_posted_then_lost_claim_reclaim_does_not_repost(queue: JobQueue):
    """2xx evidence survives handoff + claim timeout so reclaim is not a new POST."""
    capture = _WebhookCapture()
    try:
        _seed_webhook_jobs(queue, capture, 1)
        old_tok = uuid4()
        new_tok = uuid4()
        held = queue.acquire_or_renew_control_leader(
            node_id="old", lease_token=old_tok, ttl_seconds=30.0
        )
        assert held is not None
        claimed, _ = queue.claim_webhook_deliveries(
            limit=1,
            claim_seconds=60,
            leader_token=old_tok,
            leader_epoch=held.epoch,
        )
        assert len(claimed) == 1
        item = claimed[0]
        from job_queue.webhooks import post_webhook

        code, _ = post_webhook(
            item.url,
            item.delivery.payload,
            secret=item.secret,
            idempotency_key=str(item.delivery.id),
        )
        assert 200 <= code < 300
        assert queue.note_webhook_last_status(item.delivery.id, code)
        assert len(capture.received) == 1

        queue.touch_control_node("new")
        queue.handoff_control_leader("new", handoff_seconds=15.0)
        # Simulate the old claim timing out before finalize.
        with queue._pool.connection() as conn:
            conn.execute(
                """
                UPDATE webhook_deliveries
                SET delivering_until = %s
                WHERE id = %s AND status = 'delivering'
                """,
                (datetime.now(timezone.utc) - timedelta(seconds=1), item.delivery.id),
            )
            conn.commit()

        stolen = queue.acquire_or_renew_control_leader(
            node_id="new", lease_token=new_tok, ttl_seconds=30.0
        )
        assert stolen is not None
        results = queue.process_webhook_deliveries(
            limit=10,
            claim_seconds=30,
            leader_token=new_tok,
            leader_epoch=stolen.epoch,
        )
        assert len(capture.received) == 1
        got = queue.get_webhook_delivery(item.delivery.id, tenant_id="t1")
        assert got is not None
        assert got.status == "succeeded"
        assert any(r.success and r.delivery.id == item.delivery.id for r in results)
    finally:
        capture.stop()


def test_workers_keep_leasing_while_leader_ticks(queue: JobQueue):
    c1 = Coordinator(queue, node_id="w-lead")
    c1.start_control(ttl_seconds=2.0, interval_seconds=0.1)
    try:
        for i in range(4):
            queue.enqueue(tenant_id="t1", payload={"i": i})
        done = 0
        for _ in range(8):
            job = queue.lease(tenant_id="t1", worker_id="w")
            if job is None:
                break
            _ack(queue, job, "w")
            done += 1
        assert done == 4
        assert all(
            j.status == JobStatus.COMPLETED for j in queue.list_jobs(tenant_id="t1")
        )
    finally:
        c1.stop_control()
