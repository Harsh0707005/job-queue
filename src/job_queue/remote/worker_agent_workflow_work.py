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


def activity_runner_failure(remote: dict[str, Any] | None) -> str | None:
    """Error text when the runner finished unsuccessfully.

    ``done=true`` is not enough: a runner can return ``status=failed``
    after finishing. That must fail the step, not complete it.
    """
    if not remote:
        return None
    status = str(remote.get("status") or "").strip().lower()
    if status in ("failed", "timed_out", "error"):
        return str(remote.get("error") or f"activity {status}")
    if remote.get("error") and not remote.get("done"):
        return str(remote["error"])
    return None


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
        direction = str(payload.get("direction") or "forward")
        step = payload.get("step") if isinstance(payload.get("step"), dict) else {}
        step_payload = (
            step.get("payload") if isinstance(step.get("payload"), dict) else {}
        )
        item = step_payload.get("item")
        fail_item = step_payload.get("fail_item")
        if (
            payload.get("force_fail")
            or payload.get("fail")
            or (fail_item is not None and item == fail_item)
        ):
            try:
                ms = int(payload.get("ms") or step_payload.get("ms") or 30)
            except (TypeError, ValueError):
                ms = 30
            if ms > 0:
                time.sleep(min(ms, 2000) / 1000.0)
            raise RuntimeError(
                str(payload.get("error") or "workflow step forced failure")
            )
        effect_key = f"wf:{payload.get('step_run_id')}:{direction}"
        idem = str(payload.get("idempotency_key") or f"{job.id}:{effect_key}")

        if lease_lost.is_set():
            raise LeaseError(f"job {job.id} lease lost before workflow step")

        if str(step.get("kind") or "") == "activity":
            return self._do_workflow_activity(
                job, lease_lost, payload, step, step_payload
            )

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

        result: dict[str, Any] = {
            "direction": direction,
            "step_name": payload.get("step_name"),
            "op": step_payload.get("op") or direction,
        }
        # Test / soak producers can declare the ack result (items list for
        # dynamic map fan-out) on the step payload.
        declared = step_payload.get("result")
        if isinstance(declared, dict):
            result.update(declared)
        if isinstance(step_payload.get("items"), list) and "items" not in result:
            result["items"] = list(step_payload["items"])
        completed = self.client.complete_side_effect(
            job.id,
            tenant_id=job.tenant_id,
            worker_id=self.worker_id,
            lease_token=job.lease_token,  # type: ignore[arg-type]
            effect_key=effect_key,
            result=result,
        )
        return {
            "workflow": True,
            "direction": direction,
            "step_name": payload.get("step_name"),
            "step_index": payload.get("step_index"),
            "effect_key": effect_key,
            "effect": completed,
            **{k: result[k] for k in ("items",) if k in result},
        }

    def _do_workflow_activity(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
        step: dict[str, Any],
        step_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Start an external activity and heartbeat until it finishes.

        A missed heartbeat or start-to-close timeout is raised as a step
        failure so the saga compensates. Reclaim of a still-running
        activity resumes the wait without a second start POST.
        """
        started = self.client.start_workflow_activity(
            job.id,
            tenant_id=job.tenant_id,
            worker_id=self.worker_id,
            lease_token=job.lease_token,  # type: ignore[arg-type]
        )
        act = started.get("activity") or started
        status = str(act.get("status") or "")
        if status == "completed":
            return {
                "workflow": True,
                "activity": True,
                "direction": payload.get("direction"),
                "result": act.get("result"),
            }
        if status in ("failed", "timed_out"):
            raise RuntimeError(
                str(act.get("error") or f"activity {status}")
            )

        url = self._safe_activity_base_url(
            str(step.get("url") or os.environ.get("ACTIVITY_URL") or "")
        )
        if url:
            self._post_activity_start(job, payload, step, step_payload, act, url)

        hb_s = float(act.get("heartbeat_timeout_seconds") or 2.0)
        interval = max(0.05, min(0.5, hb_s / 3.0))
        while True:
            if lease_lost.is_set():
                raise LeaseError(f"job {job.id} lease lost during activity")
            hb = self.client.heartbeat_workflow_activity(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
            )
            act = hb.get("activity") or hb
            status = str(act.get("status") or "")
            if status == "completed":
                return {
                    "workflow": True,
                    "activity": True,
                    "direction": payload.get("direction"),
                    "result": act.get("result"),
                }
            if status in ("failed", "timed_out"):
                raise RuntimeError(
                    str(act.get("error") or f"activity {status}")
                )
            if url:
                remote = self._poll_activity_runner(url, str(act.get("id") or ""))
                if remote is not None:
                    fail_msg = activity_runner_failure(remote)
                    if fail_msg:
                        self._fail_activity_from_runner(job, fail_msg)
                        raise RuntimeError(fail_msg)
                    if remote.get("done"):
                        result = remote.get("result")
                        if not isinstance(result, dict):
                            result = {
                                "activity": step.get("activity") or step.get("name"),
                                "op": step_payload.get("op") or "activity",
                            }
                        done = self.client.complete_workflow_activity(
                            job.id,
                            tenant_id=job.tenant_id,
                            worker_id=self.worker_id,
                            lease_token=job.lease_token,  # type: ignore[arg-type]
                            result=result,
                        )
                        fin = done.get("activity") or done
                        return {
                            "workflow": True,
                            "activity": True,
                            "direction": payload.get("direction"),
                            "result": fin.get("result") or result,
                        }
            time.sleep(interval)

    def _fail_activity_from_runner(self, job: Job, error: str) -> None:
        fail = getattr(self.client, "fail_workflow_activity", None)
        if fail is None:
            return
        try:
            fail(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
                error=error,
            )
        except LeaseError:
            pass

    @staticmethod
    def _safe_activity_base_url(raw: str) -> str:
        url = str(raw or "").strip().rstrip("/")
        if not url:
            return ""
        from job_queue.queue.workflows_activities import validate_activity_url

        try:
            return validate_activity_url(url, resolve_dns=True).rstrip("/")
        except ValueError as exc:
            raise RuntimeError(f"activity url blocked: {exc}") from exc

    @staticmethod
    def _activity_http_open(req: urllib.request.Request, timeout: float):
        """Open *req* only after SSRF checks; never follow redirects."""
        from job_queue.queue.workflows_activities import validate_activity_url

        validate_activity_url(req.full_url, resolve_dns=True)

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):  # noqa: ANN002
                raise urllib.error.URLError("activity redirects are not allowed")

        opener = urllib.request.build_opener(_NoRedirect)
        return opener.open(req, timeout=timeout)

    def _post_activity_start(
        self,
        job: Job,
        payload: dict[str, Any],
        step: dict[str, Any],
        step_payload: dict[str, Any],
        act: dict[str, Any],
        url: str,
    ) -> None:
        start_url = url if url.endswith("/start") else url + "/start"
        start_url = self._safe_activity_base_url(start_url)
        body = json.dumps(
            {
                "activity_id": act.get("id"),
                "activity": step.get("activity") or act.get("name"),
                "job_id": str(job.id),
                "tenant_id": job.tenant_id,
                "payload": step_payload,
                "timeout_seconds": act.get("timeout_seconds"),
                "heartbeat_timeout_seconds": act.get("heartbeat_timeout_seconds"),
                "workflow_run_id": payload.get("workflow_run_id"),
                "step_name": payload.get("step_name"),
            }
        ).encode()
        req = urllib.request.Request(
            start_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Idempotency-Key": str(act.get("id") or job.id),
            },
            method="POST",
        )
        try:
            with self._activity_http_open(req, 10) as resp:
                resp.read()
        except RuntimeError:
            raise
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            if os.environ.get("WORKFLOW_ALLOW_SINK_FAILURE") == "1":
                logger.warning(
                    "activity start failed job=%s: %s (allowed)", job.id, exc
                )
                return
            raise RuntimeError(f"activity start failed: {exc}") from exc

    def _poll_activity_runner(
        self, url: str, activity_id: str
    ) -> dict[str, Any] | None:
        if not activity_id:
            return None
        base = url[: -len("/start")] if url.endswith("/start") else url
        status_url = f"{base}/status?id={activity_id}"
        try:
            status_url = self._safe_activity_base_url(status_url)
            req = urllib.request.Request(status_url, method="GET")
            with self._activity_http_open(req, 5) as resp:
                raw = resp.read()
            return json.loads(raw.decode() or "{}")
        except RuntimeError:
            raise
        except Exception:  # noqa: BLE001
            return None
