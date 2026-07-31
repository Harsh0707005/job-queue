"""worker_agent."""
from __future__ import annotations

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


class RemoteWorkerAgent:
    """Remote worker: leases/heartbeats/acks only via coordinator HTTP.

    Graceful drain on deploy/shutdown::

        agent.drain(timeout_seconds=60)
        # no new leases; finish in-flight with heartbeats; on timeout
        # release remaining jobs for immediate reclaim by others
    """

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

    def _do_work(self, job: Job, lease_lost: threading.Event) -> dict[str, Any]:
        """Run payload work; supports stepped checkpointed jobs and plain sleep.

        Payload ``steps`` (or ``kind: "steps"``) runs a multi-step loop, writing
        ``checkpoint = {"step": N}`` after each finished step so a reclaim
        resumes from *N* instead of 0. ``step_ms`` is sleep per step.
        Legacy ``sleep_ms`` still supported for cancel/long-job soaks.
        """
        payload = job.payload if isinstance(job.payload, dict) else {}
        kind = payload.get("kind")
        # Spawn subtasks and park until they complete (join).
        if (
            kind == "spawn"
            or isinstance(payload.get("spawn_children"), list)
            or isinstance(payload.get("spawn"), list)
        ):
            return self._do_spawn_work(job, lease_lost, payload)

        # Once-only external side effects (charge/email style).
        if kind == "side_effects" or payload.get("effects"):
            return self._do_side_effects(job, lease_lost, payload)
        # Force failure (dead-letter / retry soaks).
        if kind in ("fail", "error") or payload.get("force_fail"):
            try:
                ms = int(payload.get("ms") or payload.get("sleep_ms") or 20)
            except (TypeError, ValueError):
                ms = 20
            if ms > 0:
                time.sleep(min(ms, 5000) / 1000.0)
            raise RuntimeError(
                str(payload.get("error") or "forced failure")
            )

        steps_raw = payload.get("steps")
        use_steps = steps_raw is not None or kind == "steps"

        if use_steps:
            try:
                total = int(steps_raw if steps_raw is not None else 1)
            except (TypeError, ValueError):
                total = 1
            if total < 1:
                total = 1
            try:
                step_ms = int(payload.get("step_ms") or 200)
            except (TypeError, ValueError):
                step_ms = 200
            start = 0
            cp = job.checkpoint
            if isinstance(cp, dict) and cp.get("step") is not None:
                try:
                    start = max(0, int(cp["step"]))
                except (TypeError, ValueError):
                    start = 0
            if start > total:
                start = total
            logger.info(
                "job %s steps=%d start=%d resumed=%s worker=%s",
                job.id,
                total,
                start,
                start > 0,
                self.worker_id,
            )
            approval_after = payload.get("approval_after_step")
            try:
                approval_after_i = (
                    int(approval_after) if approval_after is not None else None
                )
            except (TypeError, ValueError):
                approval_after_i = None
            already_approved = job.approval_outcome == "approved"
            try:
                approval_timeout = float(
                    payload.get("approval_timeout_seconds") or 3600
                )
            except (TypeError, ValueError):
                approval_timeout = 3600.0

            for step in range(start, total):
                if lease_lost.is_set():
                    raise LeaseError(
                        f"job {job.id} lease lost during step {step}/{total}"
                    )
                # Approval gate: park after finishing steps up to the gate.
                if (
                    approval_after_i is not None
                    and not already_approved
                    and step == approval_after_i
                ):
                    try:
                        parked = self.client.request_approval(
                            job.id,
                            tenant_id=job.tenant_id,
                            worker_id=self.worker_id,
                            lease_token=job.lease_token,  # type: ignore[arg-type]
                            message=str(
                                payload.get("approval_message")
                                or f"approval required after step {approval_after_i}"
                            ),
                            timeout_seconds=approval_timeout,
                            progress={
                                "step": step,
                                "total": total,
                                "approval_gate": approval_after_i,
                            },
                        )
                    except LeaseError:
                        lease_lost.set()
                        raise
                    raise ApprovalRequested(parked)

                # Simulate unit of work for this step.
                deadline = time.time() + (max(0, step_ms) / 1000.0)
                while time.time() < deadline:
                    if lease_lost.is_set():
                        raise LeaseError(
                            f"job {job.id} lease lost during step {step}/{total}"
                        )
                    time.sleep(min(0.05, max(0.005, deadline - time.time())))
                # Durable progress: next worker resumes from step+1.
                try:
                    self.client.checkpoint(
                        job.id,
                        tenant_id=job.tenant_id,
                        worker_id=self.worker_id,
                        lease_token=job.lease_token,  # type: ignore[arg-type]
                        progress={"step": step + 1, "total": total},
                    )
                except LeaseError:
                    lease_lost.set()
                    raise
                logger.info(
                    "job %s checkpoint step=%d/%d worker=%s",
                    job.id,
                    step + 1,
                    total,
                    self.worker_id,
                )
            return {
                "ok": True,
                "worker_id": self.worker_id,
                "tenant_id": job.tenant_id,
                "host": socket.gethostname(),
                "started_step": start,
                "steps": total,
                "resumed": start > 0,
                "approved_resume": already_approved and start > 0,
                "schedule_id": str(job.schedule_id) if job.schedule_id else None,
            }

        # Optional payload sleep for cancel/long-job soaks; cooperative cancel.
        sleep_ms = 0
        try:
            sleep_ms = int(
                payload.get("sleep_ms")
                or payload.get("ms")
                or 0
            )
        except (TypeError, ValueError):
            sleep_ms = 0
        if sleep_ms > 0:
            deadline = time.time() + (sleep_ms / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(
                        f"job {job.id} cancelled/lease lost during work"
                    )
                time.sleep(min(0.1, max(0.01, deadline - time.time())))
        else:
            time.sleep(0.02)
        return {
            "ok": True,
            "worker_id": self.worker_id,
            "tenant_id": job.tenant_id,
            "schedule_id": str(job.schedule_id) if job.schedule_id else None,
            "host": socket.gethostname(),
        }

    def _do_spawn_work(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Fan-out subtasks, park parent, resume after join.

        Payload::
            {
              "kind": "spawn",
              "spawn_children": [{"payload": {"ms": 100}}, ...],
              "after_ms": 50,
              "timeout_seconds": 60  # optional join deadline
            }

        On first lease: spawn + park (raises ChildrenAwaited).
        After children complete: parent is pending with
        ``await_children_outcome=joined`` / checkpoint phase children_joined;
        this path finishes ``after_ms`` work and acks.
        """
        already_joined = (
            job.await_children_outcome == "joined"
            or (
                isinstance(job.checkpoint, dict)
                and job.checkpoint.get("phase") == "children_joined"
            )
        )
        if already_joined:
            after_ms = 0
            try:
                after_ms = int(payload.get("after_ms") or payload.get("ms") or 20)
            except (TypeError, ValueError):
                after_ms = 20
            if after_ms > 0:
                deadline = time.time() + (after_ms / 1000.0)
                while time.time() < deadline:
                    if lease_lost.is_set():
                        raise LeaseError(
                            f"job {job.id} lease lost after children joined"
                        )
                    time.sleep(min(0.05, max(0.005, deadline - time.time())))
            child_ids = list(job.waiting_on) or []
            if isinstance(job.checkpoint, dict):
                raw_ids = job.checkpoint.get("waiting_on") or []
                if raw_ids and not child_ids:
                    child_ids = [UUID(str(x)) for x in raw_ids]
            return {
                "ok": True,
                "joined": True,
                "child_ids": [str(x) for x in child_ids],
                "worker_id": self.worker_id,
                "tenant_id": job.tenant_id,
                "host": socket.gethostname(),
            }

        # Build child specs from payload.
        raw_children = (
            payload.get("spawn_children")
            or payload.get("spawn")
            or payload.get("children")
            or []
        )
        if not isinstance(raw_children, list) or not raw_children:
            raise ValueError("spawn job requires non-empty spawn_children list")
        children: list[dict[str, Any]] = []
        for i, item in enumerate(raw_children):
            if isinstance(item, dict) and "payload" in item:
                children.append(item)
            elif isinstance(item, dict):
                # Treat whole dict as child payload.
                children.append({"payload": item})
            else:
                children.append({"payload": {"value": item, "i": i}})

        timeout = payload.get("timeout_seconds")
        try:
            timeout_f = float(timeout) if timeout is not None else None
        except (TypeError, ValueError):
            timeout_f = None

        pre_ms = 0
        try:
            pre_ms = int(payload.get("pre_ms") or 0)
        except (TypeError, ValueError):
            pre_ms = 0
        if pre_ms > 0:
            deadline = time.time() + (pre_ms / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(f"job {job.id} lease lost before spawn")
                time.sleep(min(0.05, max(0.005, deadline - time.time())))

        if lease_lost.is_set():
            raise LeaseError(f"job {job.id} lease lost before spawn")
        try:
            result = self.client.spawn_children(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
                children=children,
                timeout_seconds=timeout_f,
                progress={"phase": "awaiting_children", "pre_done": True},
                wait=True,
            )
        except LeaseError:
            lease_lost.set()
            raise
        raise ChildrenAwaited(result.parent, result.children)

    def _do_side_effects(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Two-phase once-only effects; reclaim reuses started idempotency keys."""
        raw = payload.get("effects") or ["effect-0"]
        if isinstance(raw, str):
            effects = [raw]
        else:
            effects = [str(x) for x in raw]
        try:
            effect_ms = int(payload.get("effect_ms") or 80)
        except (TypeError, ValueError):
            effect_ms = 80
        sink = (
            os.environ.get("EFFECT_SINK_URL")
            or os.environ.get("SIDE_EFFECT_SINK_URL")
            or "http://effect-sink:8091/effect"
        ).rstrip("/")
        if not sink.endswith("/effect"):
            # allow base URL
            if sink.endswith("/"):
                sink = sink + "effect"
            else:
                sink = sink + "/effect"

        ran: list[str] = []
        skipped: list[str] = []
        resumed: list[str] = []
        for key in effects:
            if lease_lost.is_set():
                raise LeaseError(f"job {job.id} lease lost during effect {key}")
            try:
                begun = self.client.begin_side_effect(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    effect_key=key,
                )
            except LeaseError:
                lease_lost.set()
                raise
            except Exception:
                logger.exception("begin_side_effect failed job=%s key=%s", job.id, key)
                raise

            status = str(begun.get("status") or "")
            if status == "completed":
                skipped.append(key)
                logger.info(
                    "job %s skip effect %s (already completed) worker=%s",
                    job.id,
                    key,
                    self.worker_id,
                )
                continue
            if status == "started" and begun.get("started_by") not in (
                None,
                self.worker_id,
            ):
                resumed.append(key)
                logger.info(
                    "job %s resume effect %s (prior start by %s) idem=%s worker=%s",
                    job.id,
                    key,
                    begun.get("started_by"),
                    begun.get("idempotency_key"),
                    self.worker_id,
                )

            # Simulate non-trivial work; external system uses stable key.
            deadline = time.time() + (max(0, effect_ms) / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(
                        f"job {job.id} lease lost during effect {key}"
                    )
                time.sleep(min(0.05, max(0.005, deadline - time.time())))

            idem = str(begun.get("idempotency_key") or f"{job.id}:{key}")
            body = json.dumps(
                {
                    "job_id": str(job.id),
                    "tenant_id": job.tenant_id,
                    "effect_key": key,
                    "worker_id": self.worker_id,
                    "idempotency_key": idem,
                }
            ).encode()
            req = urllib.request.Request(
                sink,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": idem,
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    _ = resp.read()
            except Exception:
                logger.exception(
                    "effect sink POST failed job=%s key=%s", job.id, key
                )
                raise

            try:
                self.client.complete_side_effect(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    effect_key=key,
                    result={
                        "sink": "ok",
                        "worker_id": self.worker_id,
                        "idempotency_key": idem,
                    },
                )
            except LeaseError:
                lease_lost.set()
                # External call may have succeeded; started row remains so the
                # next worker retries with the same idempotency_key.
                raise
            ran.append(key)
            logger.info(
                "job %s completed effect %s worker=%s",
                job.id,
                key,
                self.worker_id,
            )

        return {
            "ok": True,
            "worker_id": self.worker_id,
            "tenant_id": job.tenant_id,
            "host": socket.gethostname(),
            "effects_ran": ran,
            "effects_skipped": skipped,
            "effects_resumed": resumed,
            "effects_total": len(effects),
        }


