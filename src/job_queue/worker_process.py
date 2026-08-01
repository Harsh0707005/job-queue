"""Worker job processing: process, release helpers, call handler."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from job_queue.models import Job
from job_queue.queue import (
    ApprovalRequested,
    ChildrenAwaited,
    LeaseError,
)
from job_queue.lease_context import LeaseContext, _ActiveJob

logger = logging.getLogger(__name__)


class WorkerProcessMixin:
    def _process(self, job: Job) -> None:
        assert self._handler is not None
        assert job.lease_token is not None

        lease_lost = threading.Event()
        stop_hb = threading.Event()
        active = _ActiveJob(job=job, lease_lost=lease_lost, stop_hb=stop_hb)
        with self._lease_lock:
            self._held.setdefault(job.id, job)
            self._active[job.id] = active
        ctx = LeaseContext(
            job=job,
            queue=self.queue,
            worker_id=self.worker_id,
            lease_lost=lease_lost,
        )
        interval = self._heartbeat_interval_for(job)

        hb_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job, stop_hb, lease_lost, interval),
            name=f"heartbeat-{job.id}",
            daemon=True,
        )
        hb_thread.start()
        try:
            if lease_lost.is_set():
                raise LeaseError(f"job {job.id} lease lost before handler start")

            handler_result = self._call_handler(job, ctx)

            # If the lease was reclaimed mid-flight, do not ack or nack.
            if lease_lost.is_set():
                raise LeaseError(
                    f"job {job.id} lease lost during processing; "
                    "refusing to ack (another worker may own it)"
                )

            self.queue.ack(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,
                result=handler_result,
            )
            logger.info("acked job %s", job.id)
        except ApprovalRequested as exc:
            # Job intentionally parked; do not nack.
            logger.info("job %s awaiting approval", exc.job.id)
        except ChildrenAwaited as exc:
            # Parent parked waiting for subtasks; lease already cleared.
            logger.info(
                "job %s awaiting %d children",
                exc.job.id,
                len(exc.children),
            )
        except LeaseError:
            # Lease no longer ours — do not nack; job is free or held elsewhere.
            logger.warning("lease error for job %s; not nacking", job.id)
            raise
        except Exception as exc:
            logger.exception("job %s failed: %s", job.id, exc)
            if lease_lost.is_set():
                # Another worker may already own it.
                raise
            try:
                self.queue.nack(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,
                    error=str(exc),
                    requeue=True,
                )
            except LeaseError:
                logger.warning("could not nack job %s (lease lost)", job.id)
            raise
        finally:
            stop_hb.set()
            hb_thread.join(timeout=interval + 1)
            with self._lease_lock:
                still_held = job.id in self._held
                self._active.pop(job.id, None)
                self._held.pop(job.id, None)
            # If cancel was signalled without an explicit release, free the lease.
            # Parked (approval/children) clears the token first so release is a no-op.
            if lease_lost.is_set() and still_held:
                self._release_job(job, reason="lease_lost_cleanup")

    def _release_job(self, job: Job, *, reason: str) -> bool:
        if job.lease_token is None:
            return False
        try:
            self.queue.release(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,
                reason=reason,
            )
            logger.info("released job %s reason=%s", job.id, reason)
            return True
        except LeaseError:
            logger.warning("could not release job %s (lease lost)", job.id)
            return False

    def _release_held_except_active(self, *, reason: str) -> int:
        """Release leased batch jobs that are not currently running."""
        with self._lease_lock:
            active_ids = set(self._active.keys())
            to_release = [
                j for jid, j in list(self._held.items()) if jid not in active_ids
            ]
            for j in to_release:
                self._held.pop(j.id, None)
        released = 0
        for job in to_release:
            if self._release_job(job, reason=reason):
                released += 1
        if released:
            logger.info(
                "worker %s released %d queued batch job(s) reason=%s",
                self.worker_id,
                released,
                reason,
            )
        return released

    def _release_all_held(self, *, reason: str) -> int:
        with self._lease_lock:
            to_release = list(self._held.values())
            self._held.clear()
        released = 0
        for job in to_release:
            if self._release_job(job, reason=reason):
                released += 1
        return released

    def _force_release_active(self, *, reason: str) -> int:
        with self._lease_lock:
            actives = list(self._active.values())
        released = 0
        for active in actives:
            # Stop heartbeat and free the lease *before* signalling lease_lost.
            # If we flip lease_lost first, the handler can race into
            # ``lease_lost_cleanup`` and release the row, making this path
            # return False and under-count ``DrainResult.released``.
            active.stop_hb.set()
            if self._release_job(active.job, reason=reason):
                released += 1
            active.lease_lost.set()
            with self._lease_lock:
                self._held.pop(active.job.id, None)
                self._active.pop(active.job.id, None)
        # Wait briefly for handlers to unwind after lease_lost.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with self._lease_lock:
                if not self._active:
                    break
            time.sleep(0.05)
        return released

    def _call_handler(self, job: Job, ctx: LeaseContext) -> Any:
        """Run the handler; return value is stored as the job result on ack."""
        assert self._handler is not None
        # Support both handler(job) and handler(job, lease).
        try:
            import inspect

            sig = inspect.signature(self._handler)
            nparams = len(sig.parameters)
        except (TypeError, ValueError):
            nparams = 1

        if nparams >= 2:
            return self._handler(job, ctx)
        return self._handler(job)
