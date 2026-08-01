"""LeaseContext and active-job tracking for Worker."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any  # noqa: I001 — keep Any with callables

from job_queue.models import DrainResult, Job, JobSideEffect, SpawnChildrenResult
from job_queue.queue import (
    ApprovalRequested,
    ChildrenAwaited,
    JobQueue,
    LeaseError,
)

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

    def spawn_children(
        self,
        children: list[dict[str, Any]],
        *,
        timeout_seconds: float | int | None = None,
        progress: Any = None,
        wait: bool = True,
    ) -> Any:
        """Enqueue subtasks; when *wait* is True park parent and raise
        :class:`ChildrenAwaited` so the worker frees the lease.

        *children* is a list of dicts with at least ``payload``. See
        :meth:`JobQueue.spawn_children`.
        """
        if self.lease_lost.is_set():
            raise LeaseError(f"job {self.job.id} lease lost (cancel signalled)")
        assert self.job.lease_token is not None
        result: SpawnChildrenResult = self.queue.spawn_children(
            self.job.id,
            tenant_id=self.job.tenant_id,
            worker_id=self.worker_id,
            lease_token=self.job.lease_token,
            children=children,
            timeout_seconds=timeout_seconds,
            progress=progress,
            wait=wait,
        )
        self.job = result.parent
        if wait:
            self.lease_lost.set()
            raise ChildrenAwaited(result.parent, result.children)
        return result

    def list_children(self) -> list[Job]:
        """List subtasks spawned by the current job."""
        return self.queue.list_children(
            self.job.id, tenant_id=self.job.tenant_id
        )

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
