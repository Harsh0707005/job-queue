"""Spill large payloads/results to blob storage; keep small ones inline."""

from __future__ import annotations

import json
import os
import time
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, WebhookEvent
from job_queue.blob_store import FilesystemBlobStore
from job_queue.crypto import is_blob_pointer, is_envelope
from job_queue.db import transaction

from tests.helpers_queue import _WebhookCapture, _ack


THRESHOLD = 256
BIG = "S" * 800
SMALL = {"n": 1, "kind": "tiny"}


@pytest.fixture
def blob_queue(queue: JobQueue, tmp_path):
    queue._blob_store = FilesystemBlobStore(tmp_path / "blobs")
    queue.payload_blob_threshold_bytes = THRESHOLD
    return queue


def _stored(queue: JobQueue, job_id, *, col: str = "payload") -> dict:
    with queue._pool.connection() as conn:
        row = conn.execute(
            f"SELECT {col} FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
    assert row is not None
    val = row[col]
    if isinstance(val, str):
        val = json.loads(val)
    return val


def _blob_rows(queue: JobQueue, tenant_id: str) -> list[dict]:
    with queue._pool.connection() as conn:
        return list(
            conn.execute(
                """
                SELECT id, job_id, kind, status, nbytes, sha256
                FROM job_payload_blobs
                WHERE tenant_id = %s
                ORDER BY created_at ASC, id ASC
                """,
                (tenant_id,),
            ).fetchall()
        )


def _pg_has(queue: JobQueue, needle: str) -> bool:
    with queue._pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT payload::text AS t FROM jobs
            UNION ALL
            SELECT result::text FROM jobs WHERE result IS NOT NULL
            UNION ALL
            SELECT payload::text FROM webhook_deliveries
            """
        ).fetchall()
    blob = "\n".join(r["t"] or "" for r in rows)
    return needle in blob


def test_small_payload_stays_inline(blob_queue: JobQueue):
    job = blob_queue.enqueue(tenant_id="blob-small", payload=dict(SMALL))
    stored = _stored(blob_queue, job.id)
    assert is_envelope(stored)
    assert not is_blob_pointer(stored)
    assert isinstance(stored.get("c"), str)
    assert _blob_rows(blob_queue, "blob-small") == []
    leased = blob_queue.lease(tenant_id="blob-small", worker_id="w")
    assert leased is not None
    assert leased.payload == SMALL


def test_large_payload_spills_and_lease_is_plaintext(blob_queue: JobQueue):
    secret = "tenant-secret-spill-1"
    payload = {"secret": secret, "blob": BIG}
    job = blob_queue.enqueue(tenant_id="blob-big", payload=payload)
    assert job.payload == payload
    stored = _stored(blob_queue, job.id)
    assert is_blob_pointer(stored)
    assert "c" not in stored or not isinstance(stored.get("c"), str)
    dumped = json.dumps(stored)
    assert secret not in dumped
    assert BIG not in dumped
    assert not _pg_has(blob_queue, secret)
    rows = _blob_rows(blob_queue, "blob-big")
    assert len(rows) == 1
    assert rows[0]["kind"] == "payload"
    assert rows[0]["status"] == "committed"
    assert blob_queue._blob_store.exists(rows[0]["id"])
    with blob_queue._pool.connection() as conn:
        size = conn.execute(
            "SELECT pg_column_size(payload) AS n FROM jobs WHERE id = %s",
            (job.id,),
        ).fetchone()["n"]
    assert int(size) < 2000
    leased = blob_queue.lease(tenant_id="blob-big", worker_id="w1")
    assert leased is not None
    assert leased.payload == payload
    assert leased.payload["secret"] == secret
    got = blob_queue.get(job.id, tenant_id="blob-big")
    assert got is not None and got.payload == payload


def test_ack_spills_large_result_and_orphans_nothing_inline(blob_queue: JobQueue):
    job = blob_queue.enqueue(
        tenant_id="blob-res", payload={"secret": "pay", "blob": BIG}
    )
    leased = blob_queue.lease(tenant_id="blob-res", worker_id="w")
    assert leased is not None
    result = {"out": "result-secret", "blob": BIG}
    done = _ack(blob_queue, leased, "w", result=result)
    assert done.result == result
    stored = _stored(blob_queue, job.id, col="result")
    assert is_blob_pointer(stored)
    assert "result-secret" not in json.dumps(stored)
    assert not _pg_has(blob_queue, "result-secret")
    kinds = {r["kind"] for r in _blob_rows(blob_queue, "blob-res")}
    assert "payload" in kinds
    assert "result" in kinds
    got = blob_queue.get(job.id, tenant_id="blob-res")
    assert got is not None
    assert got.result == result


def test_rotate_rewrites_spilled_ciphertext(blob_queue: JobQueue):
    secret = "rotate-blob-secret"
    job = blob_queue.enqueue(
        tenant_id="blob-rot", payload={"secret": secret, "blob": BIG}
    )
    before = _stored(blob_queue, job.id)
    old_blob = before["blob_id"]
    rot = blob_queue.rotate_tenant_payload_key("blob-rot")
    for _ in range(10):
        rot = blob_queue.advance_payload_key_rotation("blob-rot", batch_size=4)
        if rot.status == "complete":
            break
    assert rot.status == "complete"
    after = _stored(blob_queue, job.id)
    assert is_blob_pointer(after)
    assert after["blob_id"] != old_blob
    rows = {str(r["id"]): r for r in _blob_rows(blob_queue, "blob-rot")}
    assert rows[old_blob]["status"] == "orphan"
    assert rows[after["blob_id"]]["status"] == "committed"
    leased = blob_queue.lease(tenant_id="blob-rot", worker_id="w")
    assert leased is not None
    assert leased.payload["secret"] == secret
    gc = blob_queue.process_payload_blobs(limit=50)
    assert gc["deleted"] >= 1
    assert not blob_queue._blob_store.exists(old_blob)


def test_live_rotation_spilled_pending_leased_webhook_and_export(
    blob_queue: JobQueue,
):
    """Old and new keys coexist; spilled pointers stay openable; list stays redacted."""
    from job_queue.crypto import is_envelope
    from tests.helpers_queue import _WebhookCapture

    capture = _WebhookCapture()
    try:
        blob_queue.register_webhook(
            tenant_id="blob-rot-live",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook",
        )
        held = blob_queue.enqueue(
            tenant_id="blob-rot-live",
            payload={"secret": "lease-secret", "blob": BIG, "who": "leased"},
        )
        leased = blob_queue.lease(tenant_id="blob-rot-live", worker_id="holder")
        assert leased is not None
        assert leased.id == held.id
        assert leased.payload["secret"] == "lease-secret"
        done = blob_queue.enqueue(
            tenant_id="blob-rot-live",
            payload={"secret": "done-secret", "blob": BIG, "who": "done"},
        )
        dlease = blob_queue.lease(tenant_id="blob-rot-live", worker_id="finisher")
        assert dlease is not None and dlease.id == done.id
        _ack(blob_queue, dlease, "finisher", result={"out": "done-secret", "blob": BIG})
        pending = blob_queue.enqueue(
            tenant_id="blob-rot-live",
            payload={"secret": "pend-secret", "blob": BIG, "who": "pending"},
        )

        listed = blob_queue.list_jobs(tenant_id="blob-rot-live")
        # get/list decrypt locally; HTTP redaction is covered in encrypt tests.
        # Postgres must not hold the secrets.
        with blob_queue._pool.connection() as conn:
            txt = "\n".join(
                r["t"]
                for r in conn.execute(
                    """
                    SELECT payload::text AS t FROM jobs WHERE tenant_id = %s
                    UNION ALL
                    SELECT result::text FROM jobs
                    WHERE tenant_id = %s AND result IS NOT NULL
                    """,
                    ("blob-rot-live", "blob-rot-live"),
                ).fetchall()
            )
        for secret in ("pend-secret", "lease-secret", "done-secret"):
            assert secret not in txt

        rot = blob_queue.rotate_tenant_payload_key("blob-rot-live")
        assert rot.status == "reencrypting"
        fresh = blob_queue.enqueue(
            tenant_id="blob-rot-live",
            payload={"secret": "new-secret", "blob": BIG, "who": "fresh"},
        )
        stored_fresh = _stored(blob_queue, fresh.id)
        assert is_blob_pointer(stored_fresh)
        with blob_queue._pool.connection() as conn:
            kid = conn.execute(
                "SELECT payload_key_id FROM jobs WHERE id = %s",
                (fresh.id,),
            ).fetchone()["payload_key_id"]
        assert kid == rot.to_key_id

        # Mid-rotation: old pending still leases, held lease still acks,
        # export sees ciphertext only.
        still = blob_queue.get(leased.id, tenant_id="blob-rot-live")
        assert still is not None and still.payload["secret"] == "lease-secret"
        snap = blob_queue.export_tenant("blob-rot-live")
        assert snap.status == "ready"
        assert snap.counts.get("payload_blobs", 0) >= 1

        for _ in range(12):
            rot = blob_queue.advance_payload_key_rotation(
                "blob-rot-live", batch_size=4
            )
            if rot.status == "complete":
                break
        assert rot.status == "complete"

        gc = blob_queue.process_payload_blobs(limit=50)
        assert gc["deleted"] >= 0
        for job_id in (pending.id, leased.id, done.id, fresh.id):
            stored = _stored(blob_queue, job_id)
            with blob_queue._pool.connection() as conn:
                kid = conn.execute(
                    "SELECT payload_key_id FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()["payload_key_id"]
            assert kid == rot.to_key_id
            assert is_envelope(stored)
            if is_blob_pointer(stored):
                assert blob_queue._blob_store.exists(stored["blob_id"])
                blob_queue._hydrate_envelope(stored)
            got = blob_queue.get(job_id, tenant_id="blob-rot-live")
            assert got is not None
            assert "secret" in got.payload

        _ack(
            blob_queue,
            leased,
            "holder",
            result={"out": "lease-secret", "blob": BIG},
        )
        leftover = blob_queue.lease(tenant_id="blob-rot-live", worker_id="w2")
        assert leftover is not None
        assert leftover.payload["secret"] in {"pend-secret", "new-secret"}
        _ack(blob_queue, leftover, "w2")
        last = blob_queue.lease(tenant_id="blob-rot-live", worker_id="w3")
        if last is not None:
            _ack(blob_queue, last, "w3")

        blob_queue.process_webhook_deliveries(limit=20)
        delivered = blob_queue.list_webhook_deliveries(
            tenant_id="blob-rot-live", status="succeeded"
        )
        assert delivered
        assert delivered[0].payload["payload"]["secret"] in {
            "done-secret",
            "lease-secret",
            "pend-secret",
            "new-secret",
        }

        rst = blob_queue.restore_tenant(
            "blob-rot-live-dst",
            snapshot_id=snap.id,
            batch_size=20,
            run_until_complete=True,
        )
        assert rst.status == "complete"
        dst = blob_queue.get(
            blob_queue.list_jobs(tenant_id="blob-rot-live-dst")[0].id,
            tenant_id="blob-rot-live-dst",
        )
        assert dst is not None
        assert "secret" in dst.payload
    finally:
        capture.stop()


def test_export_restore_onto_empty_blob_store(blob_queue: JobQueue, tmp_path):
    """Fresh dest fleet: snapshot carries ciphertext; dest rebuilds files."""
    from job_queue.blob_store import FilesystemBlobStore
    from tests.helpers_queue import _WebhookCapture

    capture = _WebhookCapture()
    try:
        blob_queue.register_webhook(
            tenant_id="blob-fresh-src",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook",
        )
        secret = "fresh-fleet-secret"
        src = blob_queue.enqueue(
            tenant_id="blob-fresh-src",
            payload={"secret": secret, "blob": BIG, "n": 1},
            queue_name="email",
        )
        pending = blob_queue.enqueue(
            tenant_id="blob-fresh-src",
            payload={"secret": secret, "blob": BIG, "n": 2},
            queue_name="email",
        )
        leased = blob_queue.lease(
            tenant_id="blob-fresh-src", worker_id="w", queue_name="email"
        )
        assert leased is not None
        _ack(blob_queue, leased, "w", result={"out": secret, "blob": BIG})
        snap = blob_queue.export_tenant("blob-fresh-src")
        assert snap.status == "ready"
        assert snap.counts.get("payload_blobs", 0) >= 1

        # Simulate a new fleet with an empty blob volume.
        blob_queue._blob_store = FilesystemBlobStore(tmp_path / "fresh-dest")
        rst = blob_queue.restore_tenant(
            "blob-fresh-dst",
            snapshot_id=snap.id,
            batch_size=20,
            run_until_complete=True,
        )
        assert rst.status == "complete"
        dst = blob_queue.list_jobs(tenant_id="blob-fresh-dst")
        assert len(dst) == 2
        completed = [j for j in dst if j.status == JobStatus.COMPLETED]
        assert completed
        got = blob_queue.get(completed[0].id, tenant_id="blob-fresh-dst")
        assert got is not None
        assert got.payload == {"secret": secret, "blob": BIG, "n": 1}
        assert got.result == {"out": secret, "blob": BIG}
        stored = _stored(blob_queue, got.id)
        assert is_blob_pointer(stored)
        assert blob_queue._blob_store.exists(stored["blob_id"])
        assert secret not in json.dumps(stored)

        dest_leased = blob_queue.lease(
            tenant_id="blob-fresh-dst", worker_id="dw", queue_name="email"
        )
        assert dest_leased is not None
        assert dest_leased.id != src.id
        assert dest_leased.id != pending.id
        assert dest_leased.payload["secret"] == secret
        _ack(blob_queue, dest_leased, "dw")

        rot = blob_queue.rotate_tenant_payload_key("blob-fresh-dst")
        for _ in range(10):
            rot = blob_queue.advance_payload_key_rotation(
                "blob-fresh-dst", batch_size=8
            )
            if rot.status == "complete":
                break
        assert rot.status == "complete"
        after = blob_queue.get(got.id, tenant_id="blob-fresh-dst")
        assert after is not None and after.payload["secret"] == secret

        deliveries = blob_queue.list_webhook_deliveries(tenant_id="blob-fresh-dst")
        assert deliveries
        assert deliveries[0].payload["payload"]["secret"] == secret
    finally:
        capture.stop()


def test_export_restore_keeps_spilled_plaintext(blob_queue: JobQueue):
    secret = "export-blob-secret"
    src = blob_queue.enqueue(
        tenant_id="blob-src",
        payload={"secret": secret, "blob": BIG, "n": 3},
        queue_name="email",
    )
    snap = blob_queue.export_tenant("blob-src")
    assert snap.counts.get("payload_blobs", 0) >= 1
    rst = blob_queue.restore_tenant(
        "blob-dst", snapshot_id=snap.id, batch_size=20, run_until_complete=True
    )
    assert rst.status == "complete"
    leased = blob_queue.lease(tenant_id="blob-dst", worker_id="w", queue_name="email")
    assert leased is not None
    assert leased.id != src.id
    assert leased.payload == {"secret": secret, "blob": BIG, "n": 3}
    stored = _stored(blob_queue, leased.id)
    assert is_blob_pointer(stored)
    assert secret not in json.dumps(stored)
    assert not _pg_has(blob_queue, secret)
    src_job = blob_queue.get(src.id, tenant_id="blob-src")
    assert src_job is not None and src_job.payload["secret"] == secret


def test_webhook_delivery_spills_and_delivers_plaintext(blob_queue: JobQueue):
    capture = _WebhookCapture()
    try:
        blob_queue.register_webhook(
            tenant_id="blob-wh",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook-secret",
        )
        secret = "webhook-blob-secret"
        blob_queue.enqueue(
            tenant_id="blob-wh", payload={"secret": secret, "blob": BIG}
        )
        job = blob_queue.lease(tenant_id="blob-wh", worker_id="w")
        assert job is not None
        assert job.payload["secret"] == secret
        _ack(blob_queue, job, "w", result={"out": secret, "blob": BIG})
        pending = blob_queue.list_webhook_deliveries(
            tenant_id="blob-wh", status="pending"
        )
        assert len(pending) == 1
        assert pending[0].payload["payload"]["secret"] == secret
        assert pending[0].payload["result"]["out"] == secret
        with blob_queue._pool.connection() as conn:
            raw = conn.execute(
                "SELECT payload FROM webhook_deliveries WHERE tenant_id = %s",
                ("blob-wh",),
            ).fetchone()["payload"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        assert is_blob_pointer(raw)
        assert secret not in json.dumps(raw)
        results = blob_queue.process_webhook_deliveries(limit=10)
        assert len(results) == 1 and results[0].success is True
        assert capture.received
        body = capture.received[0]["body"]
        assert body["payload"]["secret"] == secret
        assert body["result"]["out"] == secret
    finally:
        capture.stop()


def test_crash_after_file_write_leaves_no_plaintext_then_gc(blob_queue: JobQueue):
    secret = "crash-blob-secret"
    store = blob_queue._blob_store
    leftover_id = uuid4()
    store.write(leftover_id, ("cipher-" + secret + "-" + BIG).encode())
    path = store.path_for(leftover_id)
    past = time.time() - 120
    os.utime(path, (past, past))
    assert store.exists(leftover_id)
    with blob_queue._pool.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM job_payload_blobs WHERE id = %s",
            (leftover_id,),
        ).fetchone()["n"]
    assert int(n) == 0
    with blob_queue._pool.connection() as conn:
        txts = conn.execute("SELECT payload::text AS t FROM jobs").fetchall()
    assert not any(secret in (r["t"] or "") for r in txts)
    gc = blob_queue.process_payload_blobs(limit=50)
    assert gc["unreferenced"] >= 1
    assert not store.exists(leftover_id)


def test_txn_rollback_after_spill_orphans_only_the_file(blob_queue: JobQueue):
    secret = "rollback-blob-secret"
    env = {
        "_jq_enc": 1,
        "kid": str(uuid4()),
        "n": "AAAAAAAAAAAA",
        "c": ("cipher-" + secret + "-" + BIG),
    }
    spilled = None
    try:
        with transaction(blob_queue._pool) as conn:
            spilled = blob_queue._spill_envelope(
                conn,
                env,
                tenant_id="blob-rb",
                job_id=None,
                kind="payload",
            )
            raise RuntimeError("boom mid-spill")
    except RuntimeError:
        pass
    assert spilled is not None
    bid = spilled["blob_id"]
    assert blob_queue._blob_store.exists(bid)
    with blob_queue._pool.connection() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM job_payload_blobs WHERE id = %s",
            (bid,),
        ).fetchone()["n"]
    assert int(n) == 0
    path = blob_queue._blob_store.path_for(bid)
    past = time.time() - 120
    os.utime(path, (past, past))
    gc = blob_queue.process_payload_blobs(limit=20)
    assert gc["unreferenced"] >= 1
    assert not blob_queue._blob_store.exists(bid)


def test_orphan_catalog_gc_deletes_file(blob_queue: JobQueue):
    job = blob_queue.enqueue(
        tenant_id="blob-gc", payload={"secret": "x", "blob": BIG}
    )
    stored = _stored(blob_queue, job.id)
    bid = stored["blob_id"]
    with blob_queue._pool.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = %s", (job.id,))
        conn.execute(
            "UPDATE job_payload_blobs SET status = 'orphan' WHERE id = %s",
            (bid,),
        )
        conn.commit()
    gc = blob_queue.process_payload_blobs(limit=10)
    assert gc["deleted"] >= 1
    assert not blob_queue._blob_store.exists(bid)


def test_missing_blob_dead_letters_and_does_not_block_sibling_lease(
    blob_queue: JobQueue,
):
    """A vanished spill must fail that job closed, not poison the tenant."""
    broken = blob_queue.enqueue(
        tenant_id="blob-miss", payload={"secret": "gone", "blob": BIG, "who": "broken"}
    )
    healthy = blob_queue.enqueue(
        tenant_id="blob-miss", payload={"secret": "ok", "who": "healthy"}
    )
    stored = _stored(blob_queue, broken.id)
    assert is_blob_pointer(stored)
    blob_queue._blob_store.delete(stored["blob_id"])

    # Repeating lease must not raise; the unreadable job is skipped.
    leased = None
    for _ in range(4):
        got = blob_queue.lease(tenant_id="blob-miss", worker_id="w1")
        if got is None:
            continue
        assert got.id != broken.id
        leased = got
        break
    assert leased is not None
    assert leased.id == healthy.id
    assert leased.payload == {"secret": "ok", "who": "healthy"}

    dead = blob_queue.get(broken.id, tenant_id="blob-miss")
    assert dead is not None
    assert dead.status == JobStatus.DEAD
    assert dead.last_error is not None
    assert "missing" in dead.last_error.lower()

    listed = blob_queue.list_jobs(tenant_id="blob-miss")
    assert {j.id for j in listed} == {broken.id, healthy.id}

    again = blob_queue.lease(tenant_id="blob-miss", worker_id="w2")
    assert again is None
    still = blob_queue.get(broken.id, tenant_id="blob-miss")
    assert still is not None and still.status == JobStatus.DEAD


def test_restore_unreadable_delivery_fails_closed_not_posted(
    blob_queue: JobQueue, tmp_path
):
    """A dest delivery whose blob cannot be opened must not go out as success."""
    from job_queue.blob_store import FilesystemBlobStore

    capture = _WebhookCapture()
    try:
        blob_queue.register_webhook(
            tenant_id="blob-wh-rst-src",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook",
        )
        blob_queue.enqueue(
            tenant_id="blob-wh-rst-src",
            payload={"secret": "wh-restore-secret", "blob": BIG},
        )
        job = blob_queue.lease(tenant_id="blob-wh-rst-src", worker_id="w")
        assert job is not None
        _ack(blob_queue, job, "w", result={"out": "wh-restore-secret", "blob": BIG})
        with blob_queue._pool.connection() as conn:
            raw = conn.execute(
                "SELECT payload FROM webhook_deliveries WHERE tenant_id = %s",
                ("blob-wh-rst-src",),
            ).fetchone()["payload"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        assert is_blob_pointer(raw)
        # File gone before export: snapshot cannot carry ciphertext.
        blob_queue._blob_store.delete(raw["blob_id"])
        snap = blob_queue.export_tenant("blob-wh-rst-src")
        blob_queue._blob_store = FilesystemBlobStore(tmp_path / "fresh-wh")
        rst = blob_queue.restore_tenant(
            "blob-wh-rst-dst",
            snapshot_id=snap.id,
            batch_size=20,
            run_until_complete=True,
        )
        assert rst.status == "complete"
        dest = blob_queue.list_webhook_deliveries(tenant_id="blob-wh-rst-dst")
        assert dest
        assert dest[0].status == "dead"
        assert dest[0].last_error
        assert dest[0].last_error.startswith("restore:")
        before = list(capture.received)
        blob_queue.process_webhook_deliveries(limit=20)
        assert capture.received == before
        assert (
            blob_queue.list_webhook_deliveries(
                tenant_id="blob-wh-rst-dst", status="succeeded"
            )
            == []
        )
        assert (
            blob_queue.list_webhook_deliveries(
                tenant_id="blob-wh-rst-dst", status="pending"
            )
            == []
        )
    finally:
        capture.stop()


def test_missing_delivery_blob_does_not_break_webhook_claim(blob_queue: JobQueue):
    capture = _WebhookCapture()
    try:
        blob_queue.register_webhook(
            tenant_id="blob-wh-miss",
            url=capture.url,
            events=[WebhookEvent.JOB_COMPLETED.value],
            secret="hook",
        )
        blob_queue.enqueue(
            tenant_id="blob-wh-miss",
            payload={"secret": "gone", "blob": BIG},
        )
        job = blob_queue.lease(tenant_id="blob-wh-miss", worker_id="w")
        assert job is not None
        _ack(blob_queue, job, "w", result={"out": "gone", "blob": BIG})
        pending = blob_queue.list_webhook_deliveries(
            tenant_id="blob-wh-miss", status="pending"
        )
        assert pending
        with blob_queue._pool.connection() as conn:
            raw = conn.execute(
                "SELECT payload FROM webhook_deliveries WHERE tenant_id = %s",
                ("blob-wh-miss",),
            ).fetchone()["payload"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        assert is_blob_pointer(raw)
        blob_queue._blob_store.delete(raw["blob_id"])
        # Must not raise; unreadable delivery is fail-closed.
        blob_queue.process_webhook_deliveries(limit=10)
        dead = blob_queue.list_webhook_deliveries(
            tenant_id="blob-wh-miss", status="dead"
        )
        assert dead
        assert blob_queue.process_webhook_deliveries(limit=10) == []
    finally:
        capture.stop()


def test_blob_store_retries_until_peer_write_is_visible(tmp_path):
    import threading

    from job_queue.blob_store import FilesystemBlobStore

    root = tmp_path / "shared"
    writer = FilesystemBlobStore(root)
    reader = FilesystemBlobStore(root, read_retries=20, read_retry_seconds=0.01)
    bid = uuid4()

    def _write() -> None:
        time.sleep(0.04)
        writer.write(bid, b"cipher-from-other-node")

    t = threading.Thread(target=_write)
    t.start()
    assert reader.read(bid) == b"cipher-from-other-node"
    t.join()


def test_shared_store_enqueue_on_one_queue_lease_rotate_export_on_peer(
    queue: JobQueue, postgres_url: str, tmp_path
):
    """Two coordinators, one blob root: spill on A, hydrate on B."""
    root = tmp_path / "fleet-blobs"
    q1 = queue
    q1._blob_store = FilesystemBlobStore(root)
    q1.payload_blob_threshold_bytes = THRESHOLD
    q2 = JobQueue.connect(
        postgres_url, min_size=1, max_size=4, require_worker_registration=False
    )
    try:
        q2._blob_store = FilesystemBlobStore(root)
        q2.payload_blob_threshold_bytes = THRESHOLD
        capture = _WebhookCapture()
        try:
            q1.register_webhook(
                tenant_id="blob-ha",
                url=capture.url,
                events=[WebhookEvent.JOB_COMPLETED.value],
                secret="hook",
            )
            secret = "cross-coord-secret"
            job = q1.enqueue(
                tenant_id="blob-ha",
                payload={"secret": secret, "blob": BIG, "who": "a"},
            )
            stored = _stored(q1, job.id)
            assert is_blob_pointer(stored)
            assert q2._blob_store.exists(stored["blob_id"])
            assert q2.get(job.id, tenant_id="blob-ha").payload["secret"] == secret

            leased = q2.lease(tenant_id="blob-ha", worker_id="peer")
            assert leased is not None
            assert leased.id == job.id
            assert leased.payload == {"secret": secret, "blob": BIG, "who": "a"}
            _ack(q2, leased, "peer", result={"out": secret, "blob": BIG})

            pending = q1.enqueue(
                tenant_id="blob-ha",
                payload={"secret": secret, "blob": BIG, "who": "pending"},
            )
            rot = q1.rotate_tenant_payload_key("blob-ha")
            for _ in range(12):
                rot = q2.advance_payload_key_rotation("blob-ha", batch_size=8)
                if rot.status == "complete":
                    break
            assert rot.status == "complete"
            after = _stored(q2, pending.id)
            assert is_blob_pointer(after)
            assert after["blob_id"] != stored["blob_id"]
            assert q1._blob_store.exists(after["blob_id"])
            still = q2.get(pending.id, tenant_id="blob-ha")
            assert still is not None and still.payload["secret"] == secret

            snap = q1.export_tenant("blob-ha")
            assert snap.status == "ready"
            assert snap.counts.get("payload_blobs", 0) >= 1
            rst = q2.restore_tenant(
                "blob-ha-dst",
                snapshot_id=snap.id,
                batch_size=20,
                run_until_complete=True,
            )
            assert rst.status == "complete"
            dest = q1.lease(tenant_id="blob-ha-dst", worker_id="dst")
            assert dest is not None
            assert dest.payload["secret"] == secret
            assert dest.id != job.id
            dest_stored = _stored(q2, dest.id)
            assert is_blob_pointer(dest_stored)
            assert q1._blob_store.exists(dest_stored["blob_id"])

            q2.process_webhook_deliveries(limit=20)
            delivered = q1.list_webhook_deliveries(
                tenant_id="blob-ha", status="succeeded"
            )
            assert delivered
            assert delivered[0].payload["payload"]["secret"] == secret
            assert not _pg_has(q1, secret)
        finally:
            capture.stop()
    finally:
        q2.close()


def test_missing_blob_is_skipped_on_fair_lease(blob_queue: JobQueue):
    broken = blob_queue.enqueue(
        tenant_id="blob-fair-a", payload={"secret": "gone", "blob": BIG}
    )
    healthy = blob_queue.enqueue(
        tenant_id="blob-fair-b", payload={"secret": "ok", "n": 1}
    )
    stored = _stored(blob_queue, broken.id)
    blob_queue._blob_store.delete(stored["blob_id"])

    leased = blob_queue.lease(worker_id="fair-w")
    assert leased is not None
    assert leased.id == healthy.id
    assert leased.payload["secret"] == "ok"

    dead = blob_queue.get(broken.id, tenant_id="blob-fair-a")
    assert dead is not None
    assert dead.status == JobStatus.DEAD

    assert blob_queue.lease(worker_id="fair-w") is None
