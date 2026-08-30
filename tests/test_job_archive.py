"""Per-tenant archival of finished jobs and results."""

from __future__ import annotations

import json
import os
import time
from uuid import uuid4

import pytest

from job_queue import JobQueue, JobStatus, ReplayError
from job_queue.blob_store import FilesystemBlobStore
from job_queue.db import transaction
from job_queue.queue.tenants_archive import ARCHIVE_STUB

from tests.helpers_queue import _ack, _drive_to_dead, _nack


@pytest.fixture
def blob_queue(queue: JobQueue, tmp_path):
    queue._blob_store = FilesystemBlobStore(tmp_path / "blobs")
    queue.payload_blob_threshold_bytes = 256
    return queue


def _complete(queue: JobQueue, tenant: str, payload: dict, result, *, worker="w"):
    job = queue.enqueue(tenant_id=tenant, payload=payload)
    leased = queue.lease(tenant_id=tenant, worker_id=worker)
    assert leased is not None and leased.id == job.id
    return _ack(queue, leased, worker, result=result)


def _stored(queue: JobQueue, job_id, *, col: str = "payload") -> dict | None:
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


def _row(queue: JobQueue, job_id) -> dict:
    with queue._pool.connection() as conn:
        row = conn.execute(
            """
            SELECT payload, result, checkpoint, archived_at, archive_blob_id,
                   status
            FROM jobs WHERE id = %s
            """,
            (job_id,),
        ).fetchone()
    assert row is not None
    return dict(row)


def test_no_policy_keeps_finished_rows_live(blob_queue: JobQueue):
    done = _complete(blob_queue, "arch-off", {"n": 1}, {"ok": True})
    out = blob_queue.process_job_archives(limit=50)
    assert out.archived == 0
    row = _row(blob_queue, done.id)
    assert row["archived_at"] is None
    assert row["archive_blob_id"] is None
    got = blob_queue.get(done.id, tenant_id="arch-off")
    assert got is not None
    assert got.result == {"ok": True}
    assert got.payload == {"n": 1}


def test_age_zero_archives_finished_and_rehydrates(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-age", after_seconds=0)
    done = _complete(
        blob_queue,
        "arch-age",
        {"secret": "age-payload", "n": 2},
        {"out": "age-result", "ok": True},
    )
    leased = blob_queue.lease(tenant_id="arch-age", worker_id="w2")
    assert leased is None
    out = blob_queue.process_job_archives(limit=20)
    assert out.archived == 1
    row = _row(blob_queue, done.id)
    assert row["archived_at"] is not None
    assert row["archive_blob_id"] is not None
    assert row["payload"] == ARCHIVE_STUB
    assert row["result"] is None
    got = blob_queue.get(done.id, tenant_id="arch-age")
    assert got is not None
    assert got.is_archived
    assert got.payload == {"secret": "age-payload", "n": 2}
    assert got.result == {"out": "age-result", "ok": True}
    assert blob_queue.get_result(done.id, tenant_id="arch-age") == {
        "out": "age-result",
        "ok": True,
    }
    listed = blob_queue.list_jobs(tenant_id="arch-age", status=JobStatus.COMPLETED)
    assert len(listed) == 1
    assert listed[0].payload["secret"] == "age-payload"
    assert listed[0].result["out"] == "age-result"


def test_pending_and_leased_are_not_archived(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-live", after_seconds=0)
    pending = blob_queue.enqueue(tenant_id="arch-live", payload={"p": 1})
    leased_src = blob_queue.enqueue(tenant_id="arch-live", payload={"p": 2})
    leased = blob_queue.lease(tenant_id="arch-live", worker_id="holder")
    assert leased is not None and leased.id == pending.id
    out = blob_queue.process_job_archives(limit=20)
    assert out.archived == 0
    assert _row(blob_queue, pending.id)["archived_at"] is None
    assert _row(blob_queue, leased_src.id)["archived_at"] is None


def test_failed_dead_cancelled_are_archived(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-term", after_seconds=0)
    dead_job = blob_queue.enqueue(
        tenant_id="arch-term", payload={"k": "dead"}, max_attempts=1
    )
    leased = blob_queue.lease(tenant_id="arch-term", worker_id="w")
    assert leased is not None
    dead = _nack(blob_queue, leased, "w", error="boom", requeue=True)
    assert dead.status == JobStatus.DEAD
    cancel_job = blob_queue.enqueue(tenant_id="arch-term", payload={"k": "cancel"})
    blob_queue.cancel(cancel_job.id, tenant_id="arch-term")
    out = blob_queue.process_job_archives(limit=20)
    assert out.archived == 2
    for jid, want in ((dead_job.id, "dead"), (cancel_job.id, "cancel")):
        got = blob_queue.get(jid, tenant_id="arch-term")
        assert got is not None
        assert got.is_archived
        assert got.payload == {"k": want}


def test_size_policy_archives_oldest_until_under_cap(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-size", max_live_bytes=1)
    first = _complete(blob_queue, "arch-size", {"n": 1}, {"pad": "A" * 400})
    second = _complete(blob_queue, "arch-size", {"n": 2}, {"pad": "B" * 400})
    out = blob_queue.process_job_archives(limit=10)
    assert out.archived >= 1
    assert _row(blob_queue, first.id)["archived_at"] is not None
    # Cap is 1 byte so both finished rows should be stubbed.
    assert out.archived == 2
    assert _row(blob_queue, second.id)["archived_at"] is not None
    assert blob_queue.get(first.id, tenant_id="arch-size").result["pad"].startswith("A")
    assert blob_queue.get(second.id, tenant_id="arch-size").result["pad"].startswith("B")


def test_other_tenant_is_not_archived(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-a", after_seconds=0)
    a = _complete(blob_queue, "arch-a", {"t": "a"}, {"ok": 1})
    b = _complete(blob_queue, "arch-b", {"t": "b"}, {"ok": 2})
    out = blob_queue.process_job_archives(limit=20)
    assert out.archived == 1
    assert blob_queue.get(a.id, tenant_id="arch-a").is_archived
    assert not blob_queue.get(b.id, tenant_id="arch-b").is_archived


def test_already_archived_is_skipped(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-once", after_seconds=0)
    done = _complete(blob_queue, "arch-once", {"n": 1}, {"ok": True})
    assert blob_queue.process_job_archives(limit=10).archived == 1
    first = _row(blob_queue, done.id)["archive_blob_id"]
    assert blob_queue.process_job_archives(limit=10).archived == 0
    assert _row(blob_queue, done.id)["archive_blob_id"] == first


def test_missing_archive_blob_fails_closed(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-miss", after_seconds=0)
    done = _complete(blob_queue, "arch-miss", {"secret": "gone"}, {"out": 1})
    blob_queue.process_job_archives(limit=5)
    bid = _row(blob_queue, done.id)["archive_blob_id"]
    blob_queue._blob_store.delete(bid)
    got = blob_queue.get(done.id, tenant_id="arch-miss")
    assert got is not None
    assert got.payload == ARCHIVE_STUB
    assert got.result is None
    listed = blob_queue.list_jobs(tenant_id="arch-miss")
    assert listed[0].payload == ARCHIVE_STUB


def test_export_restore_rehydrates_archived_jobs(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-src", after_seconds=0)
    src = _complete(
        blob_queue,
        "arch-src",
        {"secret": "export-me", "n": 9},
        {"out": "restored-result"},
    )
    blob_queue.process_job_archives(limit=5)
    assert blob_queue.get(src.id, tenant_id="arch-src").is_archived
    snap = blob_queue.export_tenant("arch-src")
    rst = blob_queue.restore_tenant(
        "arch-dst", snapshot_id=snap.id, run_until_complete=True
    )
    assert rst.status == "complete"
    dest = blob_queue.list_jobs(tenant_id="arch-dst")
    assert len(dest) == 1
    got = dest[0]
    assert got.is_archived
    assert got.payload == {"secret": "export-me", "n": 9}
    assert got.result == {"out": "restored-result"}
    settings = blob_queue.get_tenant_job_archive("arch-dst")
    assert settings.archive_after_seconds == 0


def test_offboard_wipes_archive_blobs(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-gone", after_seconds=0)
    keep = _complete(blob_queue, "arch-keep", {"k": 1}, {"ok": True})
    gone = _complete(blob_queue, "arch-gone", {"g": 1}, {"ok": True})
    blob_queue.process_job_archives(limit=10)
    bid = _row(blob_queue, gone.id)["archive_blob_id"]
    assert blob_queue._blob_store.exists(bid)
    result = blob_queue.offboard_tenant("arch-gone")
    assert result.empty or not result.offboarding
    blob_queue.process_offboard_finalizes(limit=10)
    assert blob_queue.tenant_is_empty("arch-gone")
    assert not blob_queue._blob_store.exists(bid)
    assert blob_queue.get(keep.id, tenant_id="arch-keep") is not None


def test_purge_still_deletes_archived_stubs(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-purge", after_seconds=0)
    blob_queue.set_tenant_job_retention("arch-purge", completed_seconds=1)
    done = _complete(blob_queue, "arch-purge", {"n": 1}, {"ok": True})
    blob_queue.process_job_archives(limit=5)
    assert blob_queue.get(done.id, tenant_id="arch-purge").is_archived
    time.sleep(1.2)
    purged = blob_queue.purge_expired_jobs(limit=20)
    assert purged.completed_deleted == 1
    assert blob_queue.get(done.id, tenant_id="arch-purge") is None


def test_crash_after_file_write_leaves_job_live_then_retries(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-crash", after_seconds=0)
    done = _complete(blob_queue, "arch-crash", {"n": 1}, {"ok": True})
    leftover = uuid4()
    blob_queue._blob_store.write(leftover, b'{"v":1,"payload":{}}')
    path = blob_queue._blob_store.path_for(leftover)
    past = time.time() - 120
    os.utime(path, (past, past))
    with transaction(blob_queue._pool) as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM job_payload_blobs WHERE id = %s",
            (leftover,),
        ).fetchone()["n"]
    assert int(n) == 0
    assert _row(blob_queue, done.id)["archived_at"] is None
    out = blob_queue.process_job_archives(limit=5)
    assert out.archived == 1
    got = blob_queue.get(done.id, tenant_id="arch-crash")
    assert got is not None and got.is_archived
    assert got.result == {"ok": True}
    gc = blob_queue.process_payload_blobs(limit=20)
    assert gc["unreferenced"] >= 1
    assert not blob_queue._blob_store.exists(leftover)


def test_rotate_rewrites_archived_ciphertext(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-rot", after_seconds=0)
    done = _complete(
        blob_queue, "arch-rot", {"secret": "rotate-me"}, {"out": "still-here"}
    )
    blob_queue.process_job_archives(limit=5)
    old = _row(blob_queue, done.id)["archive_blob_id"]
    rot = blob_queue.rotate_tenant_payload_key("arch-rot")
    for _ in range(8):
        rot = blob_queue.advance_payload_key_rotation("arch-rot", batch_size=8)
        if rot.status == "complete":
            break
    assert rot.status == "complete"
    new = _row(blob_queue, done.id)["archive_blob_id"]
    assert new != old
    got = blob_queue.get(done.id, tenant_id="arch-rot")
    assert got.payload == {"secret": "rotate-me"}
    assert got.result == {"out": "still-here"}


def test_checkpoint_survives_archive(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-ckpt", after_seconds=0)
    job = blob_queue.enqueue(tenant_id="arch-ckpt", payload={"n": 1})
    leased = blob_queue.lease(tenant_id="arch-ckpt", worker_id="w")
    assert leased is not None
    blob_queue.checkpoint(
        leased.id,
        tenant_id="arch-ckpt",
        worker_id="w",
        lease_token=leased.lease_token,
        progress={"step": 3},
    )
    _ack(blob_queue, leased, "w", result={"done": True})
    blob_queue.process_job_archives(limit=5)
    got = blob_queue.get(job.id, tenant_id="arch-ckpt")
    assert got.checkpoint == {"step": 3}
    assert got.result == {"done": True}


def test_clear_turns_archival_off(blob_queue: JobQueue):
    blob_queue.set_tenant_job_archive("arch-clear", after_seconds=0)
    blob_queue.set_tenant_job_archive("arch-clear", clear=True)
    settings = blob_queue.get_tenant_job_archive("arch-clear")
    assert settings.after_inherits
    assert settings.archive_after_seconds is None
    _complete(blob_queue, "arch-clear", {"n": 1}, {"ok": True})
    assert blob_queue.process_job_archives(limit=5).archived == 0


def test_replay_archived_dead_job_shows_new_result(blob_queue: JobQueue):
    """Full replay path: dead → archive → replay → lease → ack → get."""
    tenant = "arch-replay"
    blob_queue.set_tenant_job_archive(tenant, after_seconds=0)
    dead = _drive_to_dead(
        blob_queue,
        tenant_id=tenant,
        payload={"task": "retry-me", "secret": "orig-payload"},
        max_attempts=1,
    )
    assert dead.status == JobStatus.DEAD
    assert blob_queue.process_job_archives(limit=5).archived == 1
    archived = blob_queue.get(dead.id, tenant_id=tenant)
    assert archived is not None
    assert archived.is_archived
    assert archived.payload == {"task": "retry-me", "secret": "orig-payload"}
    old_archive = _row(blob_queue, dead.id)["archive_blob_id"]
    assert old_archive is not None
    assert _row(blob_queue, dead.id)["payload"] == ARCHIVE_STUB

    listed = blob_queue.list_dead_letters(tenant_id=tenant)
    assert len(listed) == 1
    assert listed[0].payload["secret"] == "orig-payload"

    replayed = blob_queue.replay_dead_letter(dead.id, tenant_id=tenant)
    assert replayed.status == JobStatus.PENDING
    assert not replayed.is_archived
    assert replayed.payload == {"task": "retry-me", "secret": "orig-payload"}
    assert replayed.result is None
    live = _row(blob_queue, dead.id)
    assert live["archived_at"] is None
    assert live["archive_blob_id"] is None
    assert live["payload"] != ARCHIVE_STUB
    assert live["result"] is None

    got = blob_queue.get(dead.id, tenant_id=tenant)
    assert got is not None
    assert not got.is_archived
    assert got.payload["secret"] == "orig-payload"
    assert got.result is None

    leased = blob_queue.lease(tenant_id=tenant, worker_id="rescuer")
    assert leased is not None and leased.id == dead.id
    assert leased.payload == {"task": "retry-me", "secret": "orig-payload"}
    done = _ack(
        blob_queue,
        leased,
        "rescuer",
        result={"out": "new-result", "ok": True},
    )
    assert done.status == JobStatus.COMPLETED
    assert done.result == {"out": "new-result", "ok": True}

    after = blob_queue.get(dead.id, tenant_id=tenant)
    assert after is not None
    assert after.status == JobStatus.COMPLETED
    assert not after.is_archived
    assert after.payload["secret"] == "orig-payload"
    assert after.result == {"out": "new-result", "ok": True}
    assert blob_queue.get_result(dead.id, tenant_id=tenant) == {
        "out": "new-result",
        "ok": True,
    }
    listed_done = blob_queue.list_jobs(tenant_id=tenant, status=JobStatus.COMPLETED)
    assert listed_done[0].result == {"out": "new-result", "ok": True}

    # Old snapshot must not come back after a later archive pass.
    assert blob_queue.process_job_archives(limit=5).archived == 1
    rearchived = blob_queue.get(dead.id, tenant_id=tenant)
    assert rearchived is not None
    assert rearchived.is_archived
    assert rearchived.result == {"out": "new-result", "ok": True}
    new_archive = _row(blob_queue, dead.id)["archive_blob_id"]
    assert new_archive is not None
    assert new_archive != old_archive


def test_replay_graph_unarchives_dead_job(blob_queue: JobQueue):
    tenant = "arch-replay-graph"
    blob_queue.set_tenant_job_archive(tenant, after_seconds=0)
    dead = _drive_to_dead(
        blob_queue,
        tenant_id=tenant,
        payload={"graph": True, "n": 1},
        max_attempts=1,
    )
    blob_queue.process_job_archives(limit=5)
    out = blob_queue.replay_dead_letter_graph(tenant, job_ids=[dead.id])
    assert out.jobs_replayed == 1
    live = _row(blob_queue, dead.id)
    assert live["archive_blob_id"] is None
    assert live["status"] == JobStatus.PENDING.value
    leased = blob_queue.lease(tenant_id=tenant, worker_id="g")
    assert leased is not None
    _ack(blob_queue, leased, "g", result={"graph": "done"})
    got = blob_queue.get(dead.id, tenant_id=tenant)
    assert got is not None
    assert got.result == {"graph": "done"}
    assert not got.is_archived


def test_replay_archived_dead_job_missing_blob_fails_closed(blob_queue: JobQueue):
    tenant = "arch-replay-miss"
    blob_queue.set_tenant_job_archive(tenant, after_seconds=0)
    dead = _drive_to_dead(
        blob_queue, tenant_id=tenant, payload={"n": 1}, max_attempts=1
    )
    blob_queue.process_job_archives(limit=5)
    bid = _row(blob_queue, dead.id)["archive_blob_id"]
    blob_queue._blob_store.delete(bid)
    with pytest.raises(ReplayError, match="archive snapshot is unreadable"):
        blob_queue.replay_dead_letter(dead.id, tenant_id=tenant)
    still = _row(blob_queue, dead.id)
    assert still["status"] == JobStatus.DEAD.value
    assert still["archive_blob_id"] == bid


def test_replay_restores_checkpoint_from_archive(blob_queue: JobQueue):
    tenant = "arch-replay-ckpt"
    blob_queue.set_tenant_job_archive(tenant, after_seconds=0)
    job = blob_queue.enqueue(tenant_id=tenant, payload={"n": 1}, max_attempts=1)
    leased = blob_queue.lease(tenant_id=tenant, worker_id="w")
    assert leased is not None
    blob_queue.checkpoint(
        leased.id,
        tenant_id=tenant,
        worker_id="w",
        lease_token=leased.lease_token,
        progress={"step": 7},
    )
    _nack(blob_queue, leased, "w", error="boom", requeue=True)
    blob_queue.process_job_archives(limit=5)
    replayed = blob_queue.replay_dead_letter(job.id, tenant_id=tenant)
    assert replayed.checkpoint == {"step": 7}
    assert _row(blob_queue, job.id)["checkpoint"] == {"step": 7}
