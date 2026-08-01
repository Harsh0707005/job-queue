"""Labels: list + bulk cancel."""

from __future__ import annotations

import threading
import time

from job_queue import Job, JobQueue, JobStatus, LeaseContext

from tests.helpers_queue import (
    _WebhookCapture,
    _ack,
)

# ---------------------------------------------------------------------------
# Labels: list + bulk cancel
# ---------------------------------------------------------------------------


def test_enqueue_labels_normalized(queue: JobQueue):
    j = queue.enqueue(
        tenant_id="t1",
        payload={"x": 1},
        labels=["env:prod", " batch:42 ", "env:prod", "team:payments"],
    )
    assert j.labels == ("batch:42", "env:prod", "team:payments")
    got = queue.get(j.id, tenant_id="t1")
    assert got is not None
    assert got.labels == j.labels


def test_list_jobs_filter_tenant_label_status(queue: JobQueue):
    a1 = queue.enqueue(
        tenant_id="ta", payload={"n": 1}, labels=["campaign:x", "tier:gold"]
    )
    a2 = queue.enqueue(
        tenant_id="ta", payload={"n": 2}, labels=["campaign:x", "tier:silver"]
    )
    a3 = queue.enqueue(tenant_id="ta", payload={"n": 3}, labels=["campaign:y"])
    queue.enqueue(tenant_id="tb", payload={"n": 9}, labels=["campaign:x"])

    # Label filter (AND): both campaign:x and tier:gold
    gold = queue.list_jobs(tenant_id="ta", labels=["campaign:x", "tier:gold"])
    assert {j.id for j in gold} == {a1.id}

    # campaign:x only
    cx = queue.list_jobs(tenant_id="ta", labels=["campaign:x"])
    assert {j.id for j in cx} == {a1.id, a2.id}

    # Status filter after cancel one
    queue.cancel(a2.id, tenant_id="ta")
    pending = queue.list_jobs(
        tenant_id="ta", labels=["campaign:x"], status=JobStatus.PENDING
    )
    assert {j.id for j in pending} == {a1.id}
    cancelled = queue.list_jobs(
        tenant_id="ta", labels=["campaign:x"], status="cancelled"
    )
    assert {j.id for j in cancelled} == {a2.id}

    # Tenant isolation: tb does not see ta jobs
    tb = queue.list_jobs(tenant_id="tb", labels=["campaign:x"])
    assert len(tb) == 1
    assert tb[0].tenant_id == "tb"
    assert a3.id not in {j.id for j in cx}


def test_cancel_matching_by_label_and_status(queue: JobQueue):
    ids = []
    for i in range(5):
        j = queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            labels=["run:abc", "kind:work"],
        )
        ids.append(j.id)
    # One other label should survive
    keep = queue.enqueue(tenant_id="t1", payload={"keep": True}, labels=["run:other"])
    # Lease one so bulk cancel covers leased too
    leased = queue.lease(tenant_id="t1", worker_id="w1", visibility_timeout_seconds=30)
    assert leased is not None

    result = queue.cancel_matching(
        tenant_id="t1",
        labels=["run:abc"],
        status=["pending", "leased"],
    )
    assert result.cancelled >= 5
    assert all(j.status == JobStatus.CANCELLED for j in result.jobs)

    for jid in ids:
        row = queue.get(jid, tenant_id="t1")
        assert row is not None and row.status == JobStatus.CANCELLED

    keep_after = queue.get(keep.id, tenant_id="t1")
    assert keep_after is not None
    assert keep_after.status in (JobStatus.PENDING, JobStatus.LEASED)


def test_cancel_matching_multi_tenant_isolation(queue: JobQueue):
    for tid in ("t-a", "t-b"):
        for i in range(3):
            queue.enqueue(
                tenant_id=tid,
                payload={"i": i},
                labels=["batch:shared-name"],
            )
    res = queue.cancel_matching(tenant_id="t-a", labels=["batch:shared-name"])
    assert res.cancelled == 3
    assert all(j.tenant_id == "t-a" for j in res.jobs)
    remaining = queue.list_jobs(tenant_id="t-b", labels=["batch:shared-name"])
    assert len(remaining) == 3
    assert all(j.status == JobStatus.PENDING for j in remaining)


def test_cancel_matching_with_workers_mid_flight(queue: JobQueue):
    """Bulk cancel frees leases; multi-tenant workers cannot complete them."""
    from job_queue import Worker

    for tid in ("mw-a", "mw-b"):
        for i in range(4):
            queue.enqueue(
                tenant_id=tid,
                payload={"kind": "slow", "i": i},
                labels=["wave:1", f"tenant:{tid}"],
            )

    started = threading.Event()
    workers = []
    threads = []
    for i in range(3):
        w = Worker(
            queue,
            worker_id=f"lbl-w-{i}",
            poll_interval_seconds=0.05,
            visibility_timeout_seconds=60,
        )

        @w.handler
        def process(job: Job, lease: LeaseContext) -> dict:
            started.set()
            # Block until lease lost (cancel) or timeout.
            for _ in range(200):
                if lease.lease_lost.is_set():
                    break
                time.sleep(0.05)
            lease.ensure_held()
            return {"ok": True}

        workers.append(w)
        th = threading.Thread(target=w.run, daemon=True)
        th.start()
        threads.append(th)

    assert started.wait(5)
    time.sleep(0.3)

    res_a = queue.cancel_matching(tenant_id="mw-a", labels=["wave:1"])
    res_b = queue.cancel_matching(tenant_id="mw-b", labels=["wave:1"])
    assert res_a.cancelled + res_b.cancelled == 8

    for tid in ("mw-a", "mw-b"):
        rows = queue.list_jobs(tenant_id=tid, labels=["wave:1"])
        assert len(rows) == 4
        assert all(r.status == JobStatus.CANCELLED for r in rows)
        assert queue.count(tenant_id=tid, status=JobStatus.LEASED) == 0

    for w in workers:
        w.stop()
    for th in threads:
        th.join(timeout=3)


def test_cancel_matching_loops_past_batch_size(queue: JobQueue):
    """Must drain all matches, not silently stop after one batch."""
    n = 1200
    for i in range(n):
        queue.enqueue(
            tenant_id="t1",
            payload={"i": i},
            labels=["sweep:big"],
        )
    # Small batch_size forces multiple loops.
    result = queue.cancel_matching(
        tenant_id="t1",
        labels=["sweep:big"],
        batch_size=100,
    )
    assert result.complete is True
    assert result.remaining_estimate == 0
    assert result.cancelled == n
    assert result.batches >= 12
    # Sample is capped; full count is in cancelled.
    assert len(result.jobs) <= 200
    leftover = queue.list_jobs(
        tenant_id="t1", labels=["sweep:big"], status="pending", limit=10
    )
    assert leftover == []
    cancelled = queue.list_jobs(
        tenant_id="t1", labels=["sweep:big"], status="cancelled", limit=50
    )
    assert len(cancelled) == 50  # limit on list


def test_cancel_matching_max_jobs_reports_incomplete(queue: JobQueue):
    for i in range(50):
        queue.enqueue(tenant_id="t1", payload={"i": i}, labels=["cap:x"])
    result = queue.cancel_matching(
        tenant_id="t1",
        labels=["cap:x"],
        batch_size=10,
        max_jobs=25,
    )
    assert result.cancelled == 25
    assert result.complete is False
    assert result.remaining_estimate == 25
    # Drain the rest.
    rest = queue.cancel_matching(tenant_id="t1", labels=["cap:x"], batch_size=20)
    assert rest.cancelled == 25
    assert rest.complete is True


def test_spawn_children_inherit_parent_labels(queue: JobQueue):
    parent = queue.enqueue(
        tenant_id="t1",
        payload={"role": "parent"},
        labels=["wave:spawn", "env:test"],
    )
    job = queue.lease(tenant_id="t1", worker_id="p1", visibility_timeout_seconds=30)
    assert job is not None
    result = queue.spawn_children(
        parent.id,
        tenant_id="t1",
        worker_id="p1",
        lease_token=job.lease_token,  # type: ignore[arg-type]
        children=[
            {"payload": {"c": 1}},
            {"payload": {"c": 2}, "labels": ["child:extra"]},
        ],
        wait=False,
    )
    kids = list(result.children)
    assert len(kids) == 2
    assert set(kids[0].labels) == {"env:test", "wave:spawn"}
    assert set(kids[1].labels) == {"child:extra", "env:test", "wave:spawn"}
    listed = queue.list_jobs(tenant_id="t1", labels=["wave:spawn"])
    # parent + 2 children
    assert len(listed) >= 3


def test_schedule_fire_copies_labels(queue: JobQueue):
    from datetime import datetime, timezone

    past = datetime(2020, 1, 1, tzinfo=timezone.utc)
    queue.create_schedule(
        tenant_id="t1",
        payload={"from": "schedule"},
        interval_seconds=60,
        start_at=past,
        labels=["sched:nightly", "env:prod"],
    )
    fires = queue.process_due_schedules(tenant_id="t1", limit=5)
    assert len(fires) >= 1
    job = fires[0].job
    assert set(job.labels) == {"env:prod", "sched:nightly"}
    found = queue.list_jobs(tenant_id="t1", labels=["sched:nightly"])
    assert any(j.id == job.id for j in found)
    # Bulk cancel schedule-fired jobs by label.
    res = queue.cancel_matching(tenant_id="t1", labels=["sched:nightly"])
    assert res.cancelled >= 1
    assert res.complete is True


def test_cancel_matching_skip_locked_empty_not_complete(queue: JobQueue):
    """Empty SKIP LOCKED page must not report complete while matches remain locked."""
    from job_queue.db import transaction

    jobs = [
        queue.enqueue(tenant_id="t1", payload={"i": i}, labels=["lock:race"])
        for i in range(3)
    ]
    ids = [j.id for j in jobs]

    # Hold FOR UPDATE on all matching rows in a concurrent transaction.
    hold_ready = threading.Event()
    release = threading.Event()
    hold_err: list[str] = []

    def _hold_locks() -> None:
        try:
            with transaction(queue._pool) as conn:  # type: ignore[attr-defined]
                rows = conn.execute(
                    """
                    SELECT id FROM jobs
                    WHERE tenant_id = %s AND labels @> %s::text[]
                    FOR UPDATE
                    """,
                    ("t1", ["lock:race"]),
                ).fetchall()
                if len(rows) != 3:
                    hold_err.append(f"locked {len(rows)} want 3")
                hold_ready.set()
                if not release.wait(10):
                    hold_err.append("release timeout")
        except Exception as exc:  # noqa: BLE001
            hold_err.append(str(exc))
            hold_ready.set()

    th = threading.Thread(target=_hold_locks, daemon=True)
    th.start()
    assert hold_ready.wait(5), hold_err

    # While locked, cancel cannot claim rows — must not claim complete.
    partial = queue.cancel_matching(
        tenant_id="t1",
        labels=["lock:race"],
        batch_size=10,
        lock_wait_seconds=0.15,
        lock_retry_interval=0.05,
    )
    assert partial.cancelled == 0
    assert partial.complete is False
    assert partial.remaining_estimate == 3

    # Release locks; full cancel should now drain everything.
    release.set()
    th.join(timeout=5)
    assert not hold_err, hold_err

    done = queue.cancel_matching(
        tenant_id="t1",
        labels=["lock:race"],
        batch_size=10,
        lock_wait_seconds=2.0,
    )
    assert done.complete is True
    assert done.cancelled == 3
    assert done.remaining_estimate == 0
    for jid in ids:
        row = queue.get(jid, tenant_id="t1")
        assert row is not None and row.status == JobStatus.CANCELLED


def test_cancel_matching_waits_for_lock_release(queue: JobQueue):
    """Sweep retries until concurrent locks release (within wait window)."""
    from job_queue.db import transaction

    j = queue.enqueue(tenant_id="t1", payload={"x": 1}, labels=["lock:wait"])
    hold_ready = threading.Event()
    release = threading.Event()

    def _hold() -> None:
        with transaction(queue._pool) as conn:  # type: ignore[attr-defined]
            conn.execute(
                "SELECT id FROM jobs WHERE id = %s FOR UPDATE",
                (j.id,),
            )
            hold_ready.set()
            release.wait(5)

    th = threading.Thread(target=_hold, daemon=True)
    th.start()
    assert hold_ready.wait(3)

    def _release_soon() -> None:
        time.sleep(0.2)
        release.set()

    threading.Thread(target=_release_soon, daemon=True).start()

    result = queue.cancel_matching(
        tenant_id="t1",
        labels=["lock:wait"],
        lock_wait_seconds=3.0,
        lock_retry_interval=0.05,
    )
    th.join(timeout=3)
    assert result.complete is True
    assert result.cancelled == 1
    after = queue.get(j.id, tenant_id="t1")
    assert after is not None and after.status == JobStatus.CANCELLED


def test_webhook_signature_verify_helper():
    from job_queue.webhooks import sign_payload, verify_webhook_signature

    body = b'{"event":"job.completed","ok":true}'
    sig = sign_payload("super-secret", body)
    assert sig.startswith("v1=")
    assert verify_webhook_signature("super-secret", body, sig) is True
    assert verify_webhook_signature("wrong", body, sig) is False
    assert verify_webhook_signature("super-secret", body, None) is False
    # Bare hex (legacy) still verifies.
    bare = sig.removeprefix("v1=")
    assert verify_webhook_signature("super-secret", body, bare) is True
    assert verify_webhook_signature("super-secret", body, f"sha256={bare}") is True


def test_webhook_signed_delivery_and_secret_rotation_mid_flight(queue: JobQueue):
    """In-flight retries must keep verifying under the pre-rotation secret."""
    import json

    from job_queue.webhooks import sign_payload, verify_webhook_signature

    capture = _WebhookCapture(fail_times=1)
    try:
        ep = queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            secret="secret-v1",
            max_attempts=5,
            backoff_base_seconds=0,
            backoff_max_seconds=1,
        )
        queue.enqueue(tenant_id="t1", payload={"n": 1})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1", result={"done": True})

        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        assert pending[0].signing_secret == "secret-v1"

        # First attempt fails (capture fails once).
        results = queue.process_webhook_deliveries(limit=5)
        assert len(results) == 1
        assert results[0].success is False
        assert len(capture.received) == 1
        first_sig = capture.received[0]["signature"]
        json.dumps(
            capture.received[0]["body"], default=str, separators=(",", ":")
        ).encode("utf-8")
        # Note: process re-serializes payload; verify using the exact POST body
        # is not available here — check header format + claim path instead.
        assert first_sig is not None and first_sig.startswith("v1=")

        # Rotate while delivery is pending retry.
        rotated = queue.rotate_webhook_secret(ep.id, tenant_id="t1", secret="secret-v2")
        assert rotated.secret == "secret-v2"
        still = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(still) == 1
        assert still[0].signing_secret == "secret-v1"

        # Claim must hand out frozen secret, not the rotated one.
        claimed, _ = queue.claim_webhook_deliveries(limit=5, claim_seconds=30.0)
        assert len(claimed) == 1
        assert claimed[0].secret == "secret-v1"
        assert claimed[0].delivery.signing_secret == "secret-v1"

        # Manually verify HMAC with frozen secret over a re-encoded payload
        # matching post_webhook separators.
        body = json.dumps(
            claimed[0].delivery.payload, default=str, separators=(",", ":")
        ).encode("utf-8")
        sig = sign_payload("secret-v1", body)
        assert verify_webhook_signature("secret-v1", body, sig)
        assert not verify_webhook_signature("secret-v2", body, sig)

        # Finalize success; new job after rotate uses v2.
        queue.finalize_webhook_delivery(claimed[0].delivery.id, status_code=200)

        queue.enqueue(tenant_id="t1", payload={"n": 2})
        job2 = queue.lease(tenant_id="t1", worker_id="w2")
        assert job2 is not None
        _ack(queue, job2, "w2")
        pending2 = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending2) == 1
        assert pending2[0].signing_secret == "secret-v2"
        claimed2, _ = queue.claim_webhook_deliveries(limit=5)
        assert len(claimed2) == 1
        assert claimed2[0].secret == "secret-v2"
        queue.finalize_webhook_delivery(claimed2[0].delivery.id, status_code=200)
    finally:
        capture.stop()


def test_webhook_process_posts_valid_signature_after_rotation(queue: JobQueue):
    """End-to-end: rotate mid-retry; second POST still verifies with old secret."""
    import json

    from job_queue.webhooks import verify_webhook_signature

    capture = _WebhookCapture(fail_times=1)
    # Patch capture to record raw body+sig for verify.

    try:
        ep = queue.register_webhook(
            tenant_id="t1",
            url=capture.url,
            events=["job.completed"],
            secret="alpha",
            max_attempts=5,
            backoff_base_seconds=0,
            backoff_max_seconds=1,
        )
        queue.enqueue(tenant_id="t1", payload={"x": 1})
        job = queue.lease(tenant_id="t1", worker_id="w1")
        assert job is not None
        _ack(queue, job, "w1")

        r1 = queue.process_webhook_deliveries(limit=5)
        assert r1[0].success is False
        assert capture.received[0]["signature"]

        queue.rotate_webhook_secret(ep.id, tenant_id="t1", secret="beta")

        # Force due for retry.
        pending = queue.list_webhook_deliveries(tenant_id="t1", status="pending")
        assert len(pending) == 1
        # process again — should succeed with signature under alpha
        r2 = queue.process_webhook_deliveries(limit=5)
        assert r2[0].success is True
        assert len(capture.received) == 2
        # Both attempts should carry a signature header.
        for rec in capture.received:
            assert rec["signature"] and rec["signature"].startswith("v1=")
            # Re-serialize as post_webhook does and check alpha still works
            # for both (frozen secret).
            body = json.dumps(rec["body"], default=str, separators=(",", ":")).encode(
                "utf-8"
            )
            assert verify_webhook_signature("alpha", body, rec["signature"]), (
                "retry must verify under pre-rotation secret"
            )
            assert not verify_webhook_signature("beta", body, rec["signature"])
    finally:
        capture.stop()


def test_job_progress_stream_multi_tenant(queue: JobQueue):
    """Live iter_job_events receives status + checkpoint across tenants."""
    from job_queue.models import JobProgressEvent

    stop = threading.Event()
    seen: list[JobProgressEvent] = []
    lock = threading.Lock()

    def _listen() -> None:
        for ev in queue.iter_job_events(
            tenant_ids=["ta", "tb"],
            include_snapshot=False,
            idle_timeout_seconds=8.0,
            heartbeat_seconds=30.0,
            stop_when=stop.is_set,
        ):
            if ev.event == "ping":
                continue
            with lock:
                seen.append(ev)
            # Stop after we see both tenants complete-ish activity.
            with lock:
                tenants = {e.tenant_id for e in seen if e.event != "ping"}
                statuses = {e.status for e in seen}
            if (
                "ta" in tenants
                and "tb" in tenants
                and ("completed" in statuses or "leased" in statuses)
            ):
                if any(e.event == "checkpointed" for e in seen) or any(
                    e.status == "completed" for e in seen
                ):
                    # Need enough events
                    if len(seen) >= 4:
                        stop.set()
                        return

    th = threading.Thread(target=_listen, daemon=True)
    th.start()
    time.sleep(0.3)  # allow LISTEN to start

    # Tenant A: stepped job with checkpoint
    ja = queue.enqueue(
        tenant_id="ta",
        payload={"kind": "steps", "steps": 2},
        labels=["stream:a"],
    )
    la = queue.lease(tenant_id="ta", worker_id="wa", visibility_timeout_seconds=30)
    assert la is not None
    queue.checkpoint(
        la.id,
        tenant_id="ta",
        worker_id="wa",
        lease_token=la.lease_token,  # type: ignore[arg-type]
        progress={"step": 1, "total": 2},
    )
    _ack(queue, la, "wa", result={"ok": True})

    # Tenant B: simple complete
    jb = queue.enqueue(tenant_id="tb", payload={"x": 1}, labels=["stream:b"])
    lb = queue.lease(tenant_id="tb", worker_id="wb", visibility_timeout_seconds=30)
    assert lb is not None
    _ack(queue, lb, "wb")

    stop.wait(6)
    stop.set()
    th.join(timeout=3)

    with lock:
        events = list(seen)
    tenants = {e.tenant_id for e in events}
    assert "ta" in tenants and "tb" in tenants, events
    assert any(e.job_id == ja.id for e in events)
    assert any(e.job_id == jb.id for e in events)
    assert any(e.event == "checkpointed" for e in events)
    assert any(e.status == "completed" for e in events)
    # Isolation: no other tenant leakage in filter
    assert all(e.tenant_id in ("ta", "tb") for e in events)


def test_job_progress_stream_job_filter_and_snapshot(queue: JobQueue):
    j1 = queue.enqueue(tenant_id="t1", payload={"n": 1}, labels=["watch"])
    j2 = queue.enqueue(tenant_id="t1", payload={"n": 2}, labels=["ignore"])

    snaps = []
    for ev in queue.iter_job_events(
        tenant_ids=["t1"],
        job_ids=[j1.id],
        include_snapshot=True,
        idle_timeout_seconds=0.5,
        heartbeat_seconds=60.0,
    ):
        if ev.event == "ping":
            continue
        if ev.snapshot:
            snaps.append(ev)
        else:
            break
    assert any(e.job_id == j1.id and e.snapshot for e in snaps)
    assert all(e.job_id == j1.id for e in snaps)
    # j2 not in snapshot filter
    assert not any(e.job_id == j2.id for e in snaps)


def test_job_progress_stream_no_missed_completion_race(queue: JobQueue):
    """LISTEN before snapshot: completion between snapshot and listen is not lost.

    Runs many iterations to stress the race window that previously left clients
    stuck on a pre-completion snapshot.
    """
    for i in range(25):
        j = queue.enqueue(
            tenant_id="t1",
            payload={"race": i},
            labels=["race:stream"],
        )
        leased = queue.lease(
            tenant_id="t1",
            worker_id=f"race-w-{i}",
            visibility_timeout_seconds=30,
        )
        assert leased is not None and leased.id == j.id

        stop = threading.Event()
        events: list = []
        lock = threading.Lock()

        def _listen(job_id=j.id, ev_list=events, st=stop) -> None:
            for ev in queue.iter_job_events(
                tenant_ids=["t1"],
                job_ids=[job_id],
                include_snapshot=True,
                idle_timeout_seconds=4.0,
                heartbeat_seconds=60.0,
                stop_when=st.is_set,
            ):
                if ev.event == "ping":
                    continue
                with lock:
                    ev_list.append(ev)
                    if any(e.status == "completed" for e in ev_list):
                        st.set()
                        return

        th = threading.Thread(target=_listen, daemon=True)
        th.start()
        # Complete as soon as possible while subscribe is snapshotting / listening.
        time.sleep(0.005)
        _ack(queue, leased, f"race-w-{i}", result={"i": i})
        assert stop.wait(4.0), (
            f"iter {i}: never saw completed; events="
            f"{[(e.event, e.status, e.snapshot) for e in events]}"
        )
        stop.set()
        th.join(timeout=2)
        with lock:
            assert any(e.status == "completed" for e in events)


def test_job_progress_stream_completion_during_snapshot_buffered(queue: JobQueue):
    """If job finishes while snapshot is loading, buffered NOTIFY still delivers."""
    j = queue.enqueue(tenant_id="t1", payload={"x": 1})
    leased = queue.lease(
        tenant_id="t1", worker_id="buf-w", visibility_timeout_seconds=30
    )
    assert leased is not None

    # Flood list_jobs work by enqueueing many siblings so snapshot takes longer.
    for n in range(80):
        queue.enqueue(tenant_id="t1", payload={"pad": n}, labels=["pad"])

    stop = threading.Event()
    seen_completed = threading.Event()
    events: list = []

    def _listen() -> None:
        for ev in queue.iter_job_events(
            tenant_ids=["t1"],
            job_ids=[j.id],
            include_snapshot=True,
            idle_timeout_seconds=5.0,
            heartbeat_seconds=60.0,
            stop_when=stop.is_set,
        ):
            if ev.event == "ping":
                continue
            events.append(ev)
            if ev.job_id == j.id and ev.status == "completed":
                seen_completed.set()
                stop.set()
                return

    th = threading.Thread(target=_listen, daemon=True)
    th.start()
    time.sleep(0.02)
    _ack(queue, leased, "buf-w", result={"done": True})
    assert seen_completed.wait(5.0), [(e.event, e.status, e.snapshot) for e in events]
    stop.set()
    th.join(timeout=2)


def test_job_progress_stream_snapshot_paginates_beyond_500(queue: JobQueue):
    """Tenant-wide subscribe must not silently drop jobs past the first list page."""
    ids = [queue.enqueue(tenant_id="t1", payload={"i": i}).id for i in range(520)]
    snaps: list = []
    for ev in queue.iter_job_events(
        tenant_ids=["t1"],
        include_snapshot=True,
        idle_timeout_seconds=0.3,
        heartbeat_seconds=60.0,
        max_snapshot_jobs=10_000,
    ):
        if ev.event == "ping":
            continue
        if ev.snapshot:
            snaps.append(ev.job_id)
    assert len(snaps) == 520
    assert set(snaps) == set(ids)


def test_job_progress_stream_snapshot_respects_max_snapshot_jobs(queue: JobQueue):
    """Memory guard: tenant-wide snapshots stop after max_snapshot_jobs."""
    for i in range(30):
        queue.enqueue(tenant_id="t1", payload={"i": i})
    snaps: list = []
    for ev in queue.iter_job_events(
        tenant_ids=["t1"],
        include_snapshot=True,
        idle_timeout_seconds=0.3,
        heartbeat_seconds=60.0,
        max_snapshot_jobs=12,
    ):
        if ev.event == "ping":
            continue
        if ev.snapshot:
            snaps.append(ev.job_id)
    assert len(snaps) == 12
