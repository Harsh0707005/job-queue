"""RemoteWorkerAgent workflow step handler (forward + compensate)."""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from job_queue.models import Job
from job_queue.queue import LeaseError

logger = logging.getLogger(__name__)


class RemoteWorkerWorkflowWorkMixin:
    def _do_workflow_step(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute one workflow.step job (forward or compensate) once.

        Uses side-effect begin/complete + stable idempotency_key so reclaim
        does not double-apply external ops. force_fail in payload nacks the step.
        """
        if payload.get("force_fail") or payload.get("fail"):
            try:
                ms = int(payload.get("ms") or 30)
            except (TypeError, ValueError):
                ms = 30
            if ms > 0:
                time.sleep(min(ms, 2000) / 1000.0)
            raise RuntimeError(
                str(payload.get("error") or "workflow step forced failure")
            )

        direction = str(payload.get("direction") or "forward")
        step = payload.get("step") if isinstance(payload.get("step"), dict) else {}
        step_payload = (
            step.get("payload") if isinstance(step.get("payload"), dict) else {}
        )
        effect_key = f"wf:{payload.get('step_run_id')}:{direction}"
        idem = str(payload.get("idempotency_key") or f"{job.id}:{effect_key}")

        if lease_lost.is_set():
            raise LeaseError(f"job {job.id} lease lost before workflow step")

        try:
            begun = self.client.begin_side_effect(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
                effect_key=effect_key,
                idempotency_key=idem,
            )
        except LeaseError:
            lease_lost.set()
            raise

        status = str(begun.get("status") or "")
        if status == "completed":
            return {
                "workflow": True,
                "direction": direction,
                "skipped": True,
                "effect_key": effect_key,
                "result": begun.get("result"),
            }

        try:
            effect_ms = int(
                step_payload.get("ms")
                or payload.get("ms")
                or step_payload.get("effect_ms")
                or 50
            )
        except (TypeError, ValueError):
            effect_ms = 50
        deadline = time.time() + (max(0, effect_ms) / 1000.0)
        while time.time() < deadline:
            if lease_lost.is_set():
                raise LeaseError(f"job {job.id} lease lost during workflow step")
            time.sleep(min(0.05, max(0.005, deadline - time.time())))

        sink = (
            os.environ.get("EFFECT_SINK_URL")
            or os.environ.get("SIDE_EFFECT_SINK_URL")
            or "http://effect-sink:8091/effect"
        ).rstrip("/")
        if not sink.endswith("/effect"):
            sink = sink.rstrip("/") + "/effect"

        body = json.dumps(
            {
                "job_id": str(job.id),
                "tenant_id": job.tenant_id,
                "effect_key": effect_key,
                "worker_id": self.worker_id,
                "idempotency_key": idem,
                "direction": direction,
                "step_name": payload.get("step_name"),
                "step_index": payload.get("step_index"),
                "op": step_payload.get("op") or direction,
                "workflow_run_id": payload.get("workflow_run_id"),
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
                resp.read()
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            # Fleet may not always have sink; treat as soft success for non-op tests
            # only if explicitly allowed — otherwise fail the step.
            if os.environ.get("WORKFLOW_ALLOW_SINK_FAILURE") == "1":
                logger.warning(
                    "workflow step sink failed job=%s: %s (allowed)", job.id, exc
                )
            else:
                raise RuntimeError(f"workflow effect sink failed: {exc}") from exc

        if lease_lost.is_set():
            raise LeaseError(f"job {job.id} lease lost after workflow effect")

        completed = self.client.complete_side_effect(
            job.id,
            tenant_id=job.tenant_id,
            worker_id=self.worker_id,
            lease_token=job.lease_token,  # type: ignore[arg-type]
            effect_key=effect_key,
            result={
                "direction": direction,
                "step_name": payload.get("step_name"),
                "op": step_payload.get("op") or direction,
            },
        )
        return {
            "workflow": True,
            "direction": direction,
            "step_name": payload.get("step_name"),
            "step_index": payload.get("step_index"),
            "effect_key": effect_key,
            "effect": completed,
        }
