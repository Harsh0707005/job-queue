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
from typing import Any  # noqa: I001 — keep Any with callables

from job_queue.models import DrainResult, Job, JobSideEffect
from job_queue.queue import ApprovalRequested, JobQueue, LeaseError

logger = logging.getLogger(__name__)

Handler = Callable[..., Any]


@dataclass(slots=True)
class _ActiveJob:
    job: Job
    lease_lost: threading.Event
    stop_hb: threading.Event


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

    def checkpoint(self, progress: Any) -> Job:
        """Persist durable progress so a reclaim can resume.

        Raises ``LeaseError`` if the lease was lost. Updates ``self.job`` with
        the row that includes the new ``checkpoint`` / ``checkpointed_at``.
        """
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        updated = self.queue.checkpoint(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
            progress=progress,
        )
        self.job = updated
        return updated

    def request_approval(
        self,
        *,
        message: str | None = None,
        timeout_seconds: float | int = 3600,
        progress: Any = None,
    ) -> Job:
        """Park the job for tenant approval and raise :class:`ApprovalRequested`.

        Releases the lease so concurrency is freed. Callers/handlers should not
        ack or nack after this — the exception signals a clean stop.
        """
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        updated = self.queue.request_approval(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
            message=message,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )
        self.job = updated
        self.lease_lost.set()
        raise ApprovalRequested(updated)

    def side_effect_done(self, effect_key: str) -> bool:
        """True if this effect already completed (skip re-run after reclaim)."""
        return self.queue.side_effect_done(
            self.job.id,
            tenant_id=self.job.tenant_id,
            effect_key=effect_key,
        )

    def get_side_effect(self, effect_key: str) -> JobSideEffect | None:
        return self.queue.get_side_effect(
            self.job.id,
            tenant_id=self.job.tenant_id,
            effect_key=effect_key,
        )

    def begin_side_effect(
        self,
        effect_key: str,
        *,
        idempotency_key: str | None = None,
    ) -> JobSideEffect:
        """Record intent before the external call; returns stable idempotency key."""
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        return self.queue.begin_side_effect(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
            effect_key=effect_key,
            idempotency_key=idempotency_key,
        )

    def complete_side_effect(
        self,
        effect_key: str,
        *,
        result: Any = None,
    ) -> JobSideEffect:
        """Record that *effect_key* finished successfully under this lease."""
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        return self.queue.complete_side_effect(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
            effect_key=effect_key,
            result=result,
        )

    def run_side_effect_once(
        self,
        effect_key: str,
        fn: Callable[..., Any],
    ) -> Any:
        """Two-phase once-only: begin → run → complete.

        1. ``begin_side_effect`` writes ``started`` + stable ``idempotency_key``
        2. If already ``completed``, return stored result (no re-run)
        3. Call *fn*; if *fn* accepts one argument, pass the idempotency key
           so the external system can dedupe on reclaim after a crash between
           the call and ``complete_side_effect``
        4. ``complete_side_effect`` marks success
        """
        import inspect

        self.ensure_held()
        begun = self.begin_side_effect(effect_key)
        if begun.status == "completed":
            return begun.result
        self.ensure_held()
        idem = begun.idempotency_key
        try:
            sig = inspect.signature(fn)
            accepts_key = len(sig.parameters) >= 1
        except (TypeError, ValueError):
            accepts_key = False
        out = fn(idem) if accepts_key else fn()
        completed = self.complete_side_effect(effect_key, result=out)
        return completed.result if completed.result is not None else out


class Worker:
    """Poll a queue, process jobs, and keep leases alive via heartbeat.

    Graceful drain (deploy / shutdown)::

        worker.drain(timeout_seconds=60)
        # stops new leases, finishes in-flight with heartbeats, then
        # releases any still-running jobs so others can reclaim immediately

    Example::

        queue = JobQueue.connect(dsn)
        worker = Worker(queue, tenant_id="acme", worker_id="w1")
        # Or omit tenant_id for fair multi-tenant leasing:
        # worker = Worker(queue, worker_id="shared-1")

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
        self._active: dict[Any, _ActiveJob] = {}
        self._active_lock = threading.Lock()

    def handler(self, fn: Handler) -> Handler:
        """Decorator to register the job processing function.

        The handler may accept ``(job)`` or ``(job, lease: LeaseContext)``.
        """
        self._handler = fn
        return fn

    def stop(self) -> None:
        """Stop the run loop (does not wait for in-flight work). Prefer :meth:`drain`."""
        self._stop.set()
        self._draining.set()

    def drain(self, timeout_seconds: float | None = None) -> DrainResult:
        """Graceful shutdown: stop leasing, finish in-flight, release on timeout.

        1. Stop claiming new jobs.
        2. Keep heartbeating and let handlers finish until *timeout_seconds*.
        3. If still in-flight at the deadline, signal cancel and
           :meth:`~job_queue.queue.JobQueue.release` each remaining job so
           another worker can reclaim without waiting for visibility timeout.
        """
        timeout = (
            self.drain_timeout_seconds
            if timeout_seconds is None
            else float(timeout_seconds)
        )
        if timeout < 0:
            raise ValueError("timeout_seconds must be >= 0")
        self._draining.set()
        with self._active_lock:
            in_flight = len(self._active)
        started = time.monotonic()
        deadline = started + timeout
        while True:
            with self._active_lock:
                n = len(self._active)
            if n == 0:
                self._stop.set()
                waited = time.monotonic() - started
                logger.info(
                    "worker %s drain complete in %.2fs (in_flight_at_start=%d)",
                    self.worker_id,
                    waited,
                    in_flight,
                )
                return DrainResult(
                    drained=True,
                    timed_out=False,
                    in_flight_at_start=in_flight,
                    released=0,
                    wait_seconds=waited,
                )
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        released = self._force_release_active(reason="drain_timeout")
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
        for i, job in enumerate(jobs):
            if self._draining.is_set() or self._stop.is_set():
                # Release this and all remaining unprocessed leases.
                for leftover in jobs[i:]:
                    self._release_job(leftover, reason="drain")
                break
            self._process(job)
        return jobs

    def run(self, *, max_jobs: int | None = None, stop_when_idle: bool = False) -> int:
        """Run the worker loop. Returns the number of jobs processed."""
        processed = 0
        while not self._stop.is_set() and not self._draining.is_set():
            remaining = None
            if max_jobs is not None:
                remaining = max_jobs - processed
                if remaining <= 0:
                    break
            take = self.batch_size if remaining is None else min(self.batch_size, remaining)
            jobs = self.run_batch_once(limit=take)
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

    def _process(self, job: Job) -> None:
        assert self._handler is not None
        assert job.lease_token is not None

        lease_lost = threading.Event()
        stop_hb = threading.Event()
        active = _ActiveJob(job=job, lease_lost=lease_lost, stop_hb=stop_hb)
        with self._active_lock:
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
            with self._active_lock:
                self._active.pop(job.id, None)

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

    def _force_release_active(self, *, reason: str) -> int:
        with self._active_lock:
            actives = list(self._active.values())
        released = 0
        for active in actives:
            active.lease_lost.set()
            active.stop_hb.set()
            if self._release_job(active.job, reason=reason):
                released += 1
        # Wait briefly for handlers to unwind after lease_lost.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with self._active_lock:
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
