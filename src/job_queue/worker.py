"""Worker helper that leases jobs and heartbeats while processing.

Double-run protection
---------------------
Visibility-timeout reclaim is how dead workers free jobs. That means a
*zombie* worker (still running after its lease expired) can overlap with a
new holder. We prevent incorrect completion and shrink the overlap window:

1. Each lease mints a unique ``lease_token`` (fencing token).
2. Heartbeat / ack / nack require that token; after reclaim they fail.
3. The worker heartbeats on a short interval and sets a cancel flag as soon
   as the lease is lost so the handler can abort before side effects.
4. Ack only runs if the lease is still held after the handler returns.
5. If the lease was lost, we do **not** nack (another worker may already own
   the job) — we raise ``LeaseError`` instead of re-queueing a duplicate.

Handlers that perform external side effects should call
``lease_lost.is_set()`` / ``ctx.ensure_held()`` before committing them, or
use idempotent writes keyed by ``job.id``.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from job_queue.models import Job
from job_queue.queue import JobQueue, LeaseError

logger = logging.getLogger(__name__)

Handler = Callable[..., Any]


@dataclass(slots=True)
class LeaseContext:
    """Live lease handle passed to handlers that want cooperative cancel."""

    job: Job
    queue: JobQueue
    worker_id: str
    lease_lost: threading.Event

    def ensure_held(self) -> Job:
        """Raise ``LeaseError`` if this worker no longer owns the job."""
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        return self.queue.ensure_lease(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
        )


class Worker:
    """Poll a queue, process jobs, and keep leases alive via heartbeat.

    Example::

        queue = JobQueue.connect(dsn)
        worker = Worker(queue, tenant_id="acme", worker_id="w1")

        @worker.handler
        def process(job: Job, lease: LeaseContext) -> None:
            lease.ensure_held()
            do_work(job.payload)

        worker.run_once()
    """

    def __init__(
        self,
        queue: JobQueue,
        *,
        tenant_id: str,
        worker_id: str | None = None,
        queue_name: str = "default",
        heartbeat_interval_seconds: float | None = None,
        poll_interval_seconds: float = 0.5,
        visibility_timeout_seconds: int | None = None,
    ) -> None:
        self.queue = queue
        self.tenant_id = tenant_id
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self.queue_name = queue_name
        # Default heartbeat is filled in per-job from visibility timeout / 3.
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self._handler: Handler | None = None
        self._stop = threading.Event()

    def handler(self, fn: Handler) -> Handler:
        """Decorator to register the job processing function.

        The handler may accept ``(job)`` or ``(job, lease: LeaseContext)``.
        """
        self._handler = fn
        return fn

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> Job | None:
        """Lease and process at most one job. Returns the job, or None."""
        if self._handler is None:
            raise RuntimeError("no handler registered; use @worker.handler")

        job = self.queue.lease(
            tenant_id=self.tenant_id,
            worker_id=self.worker_id,
            queue_name=self.queue_name,
            visibility_timeout_seconds=self.visibility_timeout_seconds,
        )
        if job is None:
            return None

        self._process(job)
        return job

    def run(self, *, max_jobs: int | None = None, stop_when_idle: bool = False) -> int:
        """Run the worker loop. Returns the number of jobs processed."""
        processed = 0
        while not self._stop.is_set():
            job = self.run_once()
            if job is None:
                if stop_when_idle:
                    break
                self._stop.wait(self.poll_interval_seconds)
                continue
            processed += 1
            if max_jobs is not None and processed >= max_jobs:
                break
        return processed

    def _heartbeat_interval_for(self, job: Job) -> float:
        if self.heartbeat_interval_seconds is not None:
            return self.heartbeat_interval_seconds
        # Keep well under visibility timeout so a healthy worker never expires.
        return max(0.2, job.visibility_timeout_seconds / 3.0)

    def _process(self, job: Job) -> None:
        assert self._handler is not None
        assert job.lease_token is not None

        lease_lost = threading.Event()
        stop_hb = threading.Event()
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
