"""RemoteWorkerAgent loop: init, drain, run, process, release helpers."""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import BulkCancelResult, DrainResult, Job, SpawnChildrenResult
from job_queue.queue import ApprovalRequested, ChildrenAwaited, LeaseError


from job_queue.remote.client import CoordinatorClient, _ActiveRemoteJob

logger = logging.getLogger(__name__)


class RemoteWorkerLoopMixin:
    def __init__(
        self,
        client: CoordinatorClient,
        *,
        worker_id: str | None = None,
        tenant_id: str | None = None,
        poll_interval_seconds: float = 0.5,
        heartbeat_interval_seconds: float = 2.0,
        visibility_timeout_seconds: int = 30,
        batch_size: int = 1,
        drain_timeout_seconds: float = 60.0,
    ) -> None:
        self.client = client
        self.worker_id = worker_id or f"remote-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.tenant_id = tenant_id  # None => fair multi-tenant
        self.poll_interval_seconds = poll_interval_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.batch_size = batch_size
        self.drain_timeout_seconds = float(drain_timeout_seconds)
        self._stop = threading.Event()
        self._draining = threading.Event()
        self._active: dict[Any, _ActiveRemoteJob] = {}
        self._held: dict[Any, Job] = {}
        self._lease_lock = threading.Lock()
        self.processed = 0

    def stop(self) -> None:
        """Stop the run loop without waiting. Prefer :meth:`drain` for deploys."""
        self._draining.set()
        self._stop.set()
        self._release_held_except_active(reason="stop")

    def drain(self, timeout_seconds: float | None = None) -> DrainResult:
        """Stop leasing, finish active work, release batch leftovers immediately.

        Unprocessed jobs from the current lease batch are released as soon as
        drain starts (not only after the running job finishes).
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
        released = self._release_held_except_active(reason="drain")
        started = time.monotonic()
        deadline = started + timeout
        while True:
            with self._lease_lock:
                n = len(self._active)
            if n == 0:
                released += self._release_all_held(reason="drain")
                self._stop.set()
                waited = time.monotonic() - started
                logger.info(
                    "remote worker %s drain complete in %.2fs "
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
            "remote worker %s drain timed out after %.2fs; released=%d",
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

    def run(self) -> int:
        logger.info(
            "remote worker %s starting (tenant=%s fair=%s batch_size=%d "
            "drain_timeout=%.1fs)",
            self.worker_id,
            self.tenant_id,
            self.tenant_id is None,
            self.batch_size,
            self.drain_timeout_seconds,
        )
        while not self._stop.is_set() and not self._draining.is_set():
            try:
                jobs = self.client.lease_batch(
                    worker_id=self.worker_id,
                    limit=self.batch_size,
                    tenant_id=self.tenant_id,
                    visibility_timeout_seconds=self.visibility_timeout_seconds,
                )
            except Exception:
                logger.exception("lease error; backing off")
                self._stop.wait(min(5.0, self.poll_interval_seconds * 4))
                continue

            if not jobs:
                self._stop.wait(self.poll_interval_seconds)
                continue

            logger.debug(
                "worker %s leased batch of %d", self.worker_id, len(jobs)
            )
            with self._lease_lock:
                for job in jobs:
                    self._held[job.id] = job
            if self._draining.is_set() or self._stop.is_set():
                self._release_all_held(reason="drain")
                continue
            for job in jobs:
                if self._draining.is_set() or self._stop.is_set():
                    self._release_held_except_active(reason="drain")
                    break
                try:
                    self._process(job)
                    self.processed += 1
                except ApprovalRequested as exc:
                    logger.info("job %s awaiting approval", exc.job.id)
                except ChildrenAwaited as exc:
                    logger.info(
                        "job %s awaiting %d children",
                        exc.job.id,
                        len(exc.children),
                    )
                except LeaseError:
                    logger.warning("lease lost for job %s", job.id)
                except Exception:
                    logger.exception("processing failed for job %s", job.id)
        logger.info(
            "remote worker %s stopped after %d jobs",
            self.worker_id,
            self.processed,
        )
        return self.processed

    def _process(self, job: Job) -> None:
        assert job.lease_token is not None
        lease_lost = threading.Event()
        stop_hb = threading.Event()
        active = _ActiveRemoteJob(job, lease_lost, stop_hb)
        with self._lease_lock:
            self._held.setdefault(job.id, job)
            self._active[job.id] = active

        def heartbeat_loop() -> None:
            while not stop_hb.is_set():
                try:
                    self.client.heartbeat(
                        job.id,
                        tenant_id=job.tenant_id,
                        worker_id=self.worker_id,
                        lease_token=job.lease_token,  # type: ignore[arg-type]
                    )
                except LeaseError:
                    lease_lost.set()
                    return
                except Exception:
                    logger.exception("heartbeat error job=%s", job.id)
                if stop_hb.wait(self.heartbeat_interval_seconds):
                    return

        hb = threading.Thread(
            target=heartbeat_loop,
            name=f"hb-{job.id}",
            daemon=True,
        )
        hb.start()
        try:
            result = self._do_work(job, lease_lost)
            if lease_lost.is_set():
                raise LeaseError(f"lease lost during processing of {job.id}")
            self.client.ack(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,
                result=result,
            )
            logger.info("acked job %s tenant=%s", job.id, job.tenant_id)
        except ApprovalRequested:
            raise
        except ChildrenAwaited:
            raise
        except LeaseError:
            raise
        except Exception as exc:
            if not lease_lost.is_set():
                try:
                    self.client.nack(
                        job.id,
                        tenant_id=job.tenant_id,
                        worker_id=self.worker_id,
                        lease_token=job.lease_token,
                        error=str(exc),
                        requeue=True,
                    )
                except LeaseError:
                    pass
            raise
        finally:
            stop_hb.set()
            hb.join(timeout=self.heartbeat_interval_seconds + 1)
            with self._lease_lock:
                still_held = job.id in self._held
                self._active.pop(job.id, None)
                self._held.pop(job.id, None)
            if lease_lost.is_set() and still_held:
                self._release_job(job, reason="lease_lost_cleanup")

    def _release_job(self, job: Job, *, reason: str) -> bool:
        if job.lease_token is None:
            return False
        try:
            self.client.release(
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
        except Exception:
            logger.exception("release failed for job %s", job.id)
            return False

    def _release_held_except_active(self, *, reason: str) -> int:
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
                "remote worker %s released %d queued batch job(s) reason=%s",
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
            active.lease_lost.set()
            active.stop_hb.set()
            if self._release_job(active.job, reason=reason):
                released += 1
            with self._lease_lock:
                self._held.pop(active.job.id, None)
                self._active.pop(active.job.id, None)
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with self._lease_lock:
                if not self._active:
                    break
            time.sleep(0.05)
        return released
