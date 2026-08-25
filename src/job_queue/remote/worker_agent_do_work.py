"""RemoteWorkerAgent._do_work."""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any

from job_queue.models import Job
from job_queue.queue import ApprovalRequested, LeaseError

logger = logging.getLogger(__name__)


class RemoteWorkerDoWorkMixin:
    def _do_work(self, job: Job, lease_lost: threading.Event) -> dict[str, Any]:
        """Run payload work; supports stepped checkpointed jobs and plain sleep.

        Payload ``steps`` (or ``kind: "steps"``) runs a multi-step loop, writing
        ``checkpoint = {"step": N}`` after each finished step so a reclaim
        resumes from *N* instead of 0. ``step_ms`` is sleep per step.
        Legacy ``sleep_ms`` still supported for cancel/long-job soaks.
        """
        payload = job.payload if isinstance(job.payload, dict) else {}
        kind = payload.get("kind")
        # Multi-step saga forward / compensate step.
        if kind == "workflow.step":
            return self._do_workflow_step(job, lease_lost, payload)

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
            raise RuntimeError(str(payload.get("error") or "forced failure"))

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
            sleep_ms = int(payload.get("sleep_ms") or payload.get("ms") or 0)
        except (TypeError, ValueError):
            sleep_ms = 0
        if sleep_ms > 0:
            deadline = time.time() + (sleep_ms / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(f"job {job.id} cancelled/lease lost during work")
                time.sleep(min(0.1, max(0.01, deadline - time.time())))
        else:
            time.sleep(0.02)
        out = {
            "ok": True,
            "worker_id": self.worker_id,
            "tenant_id": job.tenant_id,
            "schedule_id": str(job.schedule_id) if job.schedule_id else None,
            "host": socket.gethostname(),
        }
        # Soak / test hook: echo a declared result (can be large so the
        # coordinator spills the ack body out of Postgres).
        declared = payload.get("ack_result")
        if isinstance(declared, dict):
            out.update(declared)
        return out
