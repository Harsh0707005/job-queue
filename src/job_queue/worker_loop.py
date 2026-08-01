"""Worker run loop: init, drain, run, batch, heartbeats."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from job_queue.models import DrainResult, Job
from job_queue.queue import (
    JobQueue,
    LeaseError,
)
from job_queue.lease_context import _ActiveJob

logger = logging.getLogger(__name__)

Handler = Callable[..., Any]


class WorkerLoopMixin:
    def __init__(
        self,
        queue: JobQueue,
        *,
        tenant_id: str | None = None,
        worker_id: str | None = None,
        queue_name: str = "default",
        heartbeat_interval_seconds: float | None = None,
        poll_interval_seconds: float = 0.5,
        visibility_timeout_seconds: int | None = None,
        batch_size: int = 1,
        drain_timeout_seconds: float = 60.0,
    ) -> None:
        self.queue = queue
        self.tenant_id = tenant_id  # None → fair lease across tenants
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self.queue_name = queue_name
        # Default heartbeat is filled in per-job from visibility timeout / 3.
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.batch_size = batch_size
        self.drain_timeout_seconds = float(drain_timeout_seconds)
        self._handler: Handler | None = None
        self._stop = threading.Event()
        self._draining = threading.Event()
        # Currently executing handler (heartbeating).
        self._active: dict[Any, _ActiveJob] = {}
        # Leased jobs not yet finished — includes the full batch queue so
        # unprocessed batch members are not left locked across drain.
        self._held: dict[Any, Job] = {}
        self._lease_lock = threading.Lock()

    def handler(self, fn: Handler) -> Handler:
        """Decorator to register the job processing function.

        The handler may accept ``(job)`` or ``(job, lease: LeaseContext)``.
        """
        self._handler = fn
        return fn

    def stop(self) -> None:
        """Stop the run loop. Prefer :meth:`drain` for deploy shutdown."""
        self._draining.set()
        self._stop.set()
        # Free leased-but-not-running batch jobs immediately.
        self._release_held_except_active(reason="stop")

    def drain(self, timeout_seconds: float | None = None) -> DrainResult:
        """Graceful shutdown: stop leasing, finish in-flight, release on timeout.

        1. Stop claiming new jobs.
        2. **Immediately release** leased batch jobs that are not yet running
           (so they are not locked until the current job finishes).
        3. Keep heartbeating the active job(s) until *timeout_seconds*.
        4. If still running at the deadline, cancel +
           :meth:`~job_queue.queue.JobQueue.release` so another worker can
           reclaim without waiting for visibility timeout.
        """
        timeout = (
            self.drain_timeout_seconds
            if timeout_seconds is None
            else float(timeout_seconds)
        )
        if timeout < 0:
            raise ValueError("timeout_seconds must be >= 0")
        self._draining.set()
        with self._lease_lock:
            in_flight = len(self._held)
            active_n = len(self._active)
        # Critical: free the rest of the batch now, not after the active job.
        released = self._release_held_except_active(reason="drain")
        started = time.monotonic()
        deadline = started + timeout
        while True:
            with self._lease_lock:
                n = len(self._active)
            if n == 0:
                # Safety: any straggler held leases.
                released += self._release_all_held(reason="drain")
                self._stop.set()
                waited = time.monotonic() - started
                logger.info(
                    "worker %s drain complete in %.2fs "
                    "(held_at_start=%d active=%d released=%d)",
                    self.worker_id,
                    waited,
                    in_flight,
                    active_n,
                    released,
                )
                return DrainResult(
                    drained=True,
                    timed_out=False,
                    in_flight_at_start=in_flight,
                    released=released,
                    wait_seconds=waited,
                )
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        released += self._force_release_active(reason="drain_timeout")
        released += self._release_all_held(reason="drain_timeout")
        self._stop.set()
        waited = time.monotonic() - started
        logger.warning(
            "worker %s drain timed out after %.2fs; released=%d",
            self.worker_id,
            waited,
            released,
        )
        return DrainResult(
            drained=False,
            timed_out=True,
            in_flight_at_start=in_flight,
            released=released,
            wait_seconds=waited,
        )

    def run_once(self) -> Job | None:
        """Lease and process at most one job. Returns the job, or None."""
        jobs = self.run_batch_once(limit=1)
        return jobs[0] if jobs else None

    def run_batch_once(self, *, limit: int | None = None) -> list[Job]:
        """Lease up to *limit* (default ``batch_size``) jobs and process them.

        Jobs in the batch are processed sequentially with independent heartbeats.
        Returns the list of jobs that were leased (may be shorter than *limit*).
        Unprocessed jobs (if drain/stop mid-batch) are released immediately.
        """
        if self._handler is None:
            raise RuntimeError("no handler registered; use @worker.handler")
        if self._draining.is_set() or self._stop.is_set():
            return []

        take = self.batch_size if limit is None else limit
        if take < 1:
            raise ValueError("limit must be >= 1")

        jobs = self.queue.lease_batch(
            tenant_id=self.tenant_id,
            worker_id=self.worker_id,
            queue_name=self.queue_name,
            visibility_timeout_seconds=self.visibility_timeout_seconds,
            limit=take,
        )
        # Track the whole batch immediately so drain can free unprocessed rows.
        with self._lease_lock:
            for job in jobs:
                self._held[job.id] = job
        if self._draining.is_set() or self._stop.is_set():
            self._release_all_held(reason="drain")
            return jobs
        for job in jobs:
            if self._draining.is_set() or self._stop.is_set():
                self._release_held_except_active(reason="drain")
                break
            try:
                self._process(job)
            except LeaseError:
                # Propagate so run_once / callers can detect cancel, reclaim,
                # or max-runtime. Release any not-yet-processed batch mates.
                logger.warning("lease lost for job %s", job.id)
                self._release_held_except_active(reason="batch_abort_lease")
                raise
            except Exception:
                # Handler already nacked inside _process; still surface the
                # original error (tests and operators rely on this).
                logger.exception("processing failed for job %s", job.id)
                self._release_held_except_active(reason="batch_abort_error")
                raise
        return jobs

    def run(self, *, max_jobs: int | None = None, stop_when_idle: bool = False) -> int:
        """Run the worker loop. Returns the number of jobs processed.

        Handler failures and lease losses are logged and the loop continues
        (so a single bad job cannot kill a long-running worker). Call
        :meth:`run_once` / :meth:`run_batch_once` when you want errors to
        propagate to the caller.
        """
        processed = 0
        while not self._stop.is_set() and not self._draining.is_set():
            remaining = None
            if max_jobs is not None:
                remaining = max_jobs - processed
                if remaining <= 0:
                    break
            take = (
                self.batch_size
                if remaining is None
                else min(self.batch_size, remaining)
            )
            try:
                jobs = self.run_batch_once(limit=take)
            except LeaseError:
                logger.warning(
                    "worker %s lease error in run loop; continuing",
                    self.worker_id,
                )
                if self._draining.is_set() or self._stop.is_set():
                    break
                continue
            except Exception:
                logger.exception(
                    "worker %s job error in run loop; continuing",
                    self.worker_id,
                )
                if self._draining.is_set() or self._stop.is_set():
                    break
                continue
            if not jobs:
                if stop_when_idle or self._draining.is_set() or self._stop.is_set():
                    break
                self._stop.wait(self.poll_interval_seconds)
                continue
            processed += len(jobs)
            if max_jobs is not None and processed >= max_jobs:
                break
        return processed

    def _heartbeat_interval_for(self, job: Job) -> float:
        if self.heartbeat_interval_seconds is not None:
            return self.heartbeat_interval_seconds
        # Keep well under visibility timeout so a healthy worker never expires.
        return max(0.2, job.visibility_timeout_seconds / 3.0)

    def _heartbeat_loop(
        self,
        job: Job,
        stop: threading.Event,
        lease_lost: threading.Event,
        interval: float,
    ) -> None:
        assert job.lease_token is not None
        # Immediate first heartbeat so short VTs stay alive.
        while True:
            if stop.is_set():
                return
            try:
                self.queue.heartbeat(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,
                )
                logger.debug("heartbeat job %s", job.id)
            except LeaseError:
                logger.warning("heartbeat failed for job %s (lease lost)", job.id)
                lease_lost.set()
                return
            except Exception:
                logger.exception("heartbeat error for job %s", job.id)
                # Transient error: keep trying until stop or lease error.
            if stop.wait(interval):
                return
