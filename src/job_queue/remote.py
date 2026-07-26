"""HTTP client and remote node agents for the distributed fleet."""

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
from job_queue.models import Job
from job_queue.queue import ApprovalRequested, LeaseError

logger = logging.getLogger(__name__)


class CoordinatorClient:
    """HTTP client for one or more fleet coordinators (failover / RR)."""

    def __init__(
        self,
        base_url: str | list[str],
        *,
        timeout: float = 30.0,
    ) -> None:
        if isinstance(base_url, str):
            urls = [u.strip() for u in base_url.split(",") if u.strip()]
        else:
            urls = list(base_url)
        if not urls:
            raise ValueError("at least one coordinator URL is required")
        self.base_urls = [u.rstrip("/") for u in urls]
        self.timeout = timeout
        self._rr = 0
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return self.base_urls[0]

    def _next_order(self) -> list[str]:
        with self._lock:
            n = len(self.base_urls)
            start = self._rr % n
            self._rr += 1
        return self.base_urls[start:] + self.base_urls[:start]

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        allow_204: bool = False,
    ) -> dict[str, Any] | None:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"

        last_err: Exception | None = None
        for base in self._next_order():
            url = f"{base}{path}"
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    code = int(resp.getcode())
                    raw = resp.read()
                    if code == 204 or (allow_204 and not raw):
                        return None
                    if not raw:
                        return {}
                    return json.loads(raw.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read() if exc.fp else b""
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {"error": raw.decode("utf-8", errors="replace") or str(exc)}
                if exc.code == 409 and payload.get("code") == "lease_error":
                    raise LeaseError(payload.get("error") or "lease error") from exc
                if exc.code == 409 and payload.get("code") == "approval_error":
                    from job_queue.queue import ApprovalError

                    raise ApprovalError(
                        payload.get("error") or "approval error"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "cancel_error":
                    from job_queue.queue import CancelError

                    raise CancelError(
                        payload.get("error") or "cancel error"
                    ) from exc
                if int(exc.code) == 404:
                    return {"error": payload.get("error") or "not found", "_http": 404}
                # 5xx → try next coordinator; 4xx is definitive.
                if 500 <= int(exc.code) < 600:
                    last_err = RuntimeError(
                        f"coordinator {method} {path} -> {exc.code}: {payload}"
                    )
                    continue
                raise RuntimeError(
                    f"coordinator {method} {path} -> {exc.code}: {payload}"
                ) from exc
            except urllib.error.URLError as exc:
                last_err = RuntimeError(f"coordinator {base} unreachable: {exc}")
                continue
        raise RuntimeError(f"all coordinators failed for {method} {path}: {last_err}")

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health") or {}

    def wait_healthy(self, *, timeout: float = 120.0, interval: float = 1.0) -> None:
        deadline = time.time() + timeout
        last: Exception | None = None
        while time.time() < deadline:
            try:
                h = self.health()
                if h.get("ok"):
                    return
            except Exception as exc:  # noqa: BLE001
                last = exc
            time.sleep(interval)
        raise RuntimeError(f"coordinator not healthy within {timeout}s: {last}")

    def lease(
        self,
        *,
        worker_id: str,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> Job | None:
        jobs = self.lease_batch(
            worker_id=worker_id,
            limit=1,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        return jobs[0] if jobs else None

    def lease_batch(
        self,
        *,
        worker_id: str,
        limit: int = 1,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> list[Job]:
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "queue_name": queue_name,
            "limit": limit,
        }
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        if visibility_timeout_seconds is not None:
            body["visibility_timeout_seconds"] = visibility_timeout_seconds
        last_err: Exception | None = None
        for base in self._next_order():
            url = f"{base}/v1/lease"
            data = json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    if int(resp.getcode()) == 204:
                        return []
                    raw = resp.read()
                    if not raw:
                        return []
                    payload = json.loads(raw.decode("utf-8"))
                    jobs_raw = payload.get("jobs")
                    if jobs_raw is None and payload.get("job") is not None:
                        jobs_raw = [payload["job"]]
                    return [job_from_dict(j) for j in (jobs_raw or [])]
            except urllib.error.HTTPError as exc:
                if 500 <= int(exc.code) < 600:
                    last_err = exc
                    continue
                raw = exc.read() if exc.fp else b""
                raise RuntimeError(
                    f"lease failed {exc.code}: {raw.decode('utf-8', errors='replace')}"
                ) from exc
            except urllib.error.URLError as exc:
                last_err = exc
                continue
        raise RuntimeError(f"lease failed on all coordinators: {last_err}")

    def heartbeat(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        extend_by_seconds: int | None = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
        }
        if extend_by_seconds is not None:
            body["extend_by_seconds"] = extend_by_seconds
        payload = self._request("POST", f"/v1/jobs/{job_id}/heartbeat", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def ack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        result: Any = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
        }
        if result is not None:
            body["result"] = result
        payload = self._request("POST", f"/v1/jobs/{job_id}/ack", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def nack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        error: str | None = None,
        requeue: bool = True,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "requeue": requeue,
        }
        if error is not None:
            body["error"] = error
        payload = self._request("POST", f"/v1/jobs/{job_id}/nack", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def checkpoint(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        progress: Any,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "progress": progress,
        }
        payload = self._request("POST", f"/v1/jobs/{job_id}/checkpoint", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def request_approval(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        message: str | None = None,
        timeout_seconds: float | int = 3600,
        progress: Any = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "timeout_seconds": timeout_seconds,
        }
        if message is not None:
            body["message"] = message
        if progress is not None:
            body["progress"] = progress
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/request-approval", body
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def approve(self, job_id: UUID | str, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/approve",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def reject(
        self,
        job_id: UUID | str,
        *,
        tenant_id: str,
        reason: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if reason is not None:
            body["reason"] = reason
        payload = self._request("POST", f"/v1/jobs/{job_id}/reject", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def process_approval_timeouts(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> list[Job]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/approvals/process-timeouts", body)
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def get_job(self, job_id: UUID | str, *, tenant_id: str) -> Job | None:
        payload = self._request(
            "GET",
            f"/v1/jobs/{job_id}?tenant_id={tenant_id}",
        )
        if payload is None:
            return None
        if payload.get("_http") == 404 or (
            payload.get("error") and "job" not in payload
        ):
            return None
        return job_from_dict(payload["job"])

    def process_due_schedules(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> int:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/schedules/process-due", body)
        assert payload is not None
        return int(payload.get("count") or 0)

    def claim_webhooks(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "limit": limit,
            "http_timeout_seconds": http_timeout_seconds,
        }
        if claim_seconds is not None:
            body["claim_seconds"] = claim_seconds
        payload = self._request("POST", "/v1/webhooks/claim", body)
        assert payload is not None
        return payload

    def finalize_webhook(
        self,
        delivery_id: str | UUID,
        *,
        status_code: int | None = None,
        error: str | None = None,
        endpoint_missing: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"endpoint_missing": endpoint_missing}
        if status_code is not None:
            body["status_code"] = status_code
        if error is not None:
            body["error"] = error
        payload = self._request(
            "POST", f"/v1/webhooks/{delivery_id}/finalize", body
        )
        assert payload is not None
        return payload

    def cancel_job(self, job_id: str | UUID, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/cancel",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def pause_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/pause", {})
        assert payload is not None
        return payload

    def resume_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/resume", {})
        assert payload is not None
        return payload

    def offboard_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/offboard", {})
        assert payload is not None
        return payload

    def merge_tenant(
        self, source_tenant_id: str, target_tenant_id: str
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{source_tenant_id}/merge",
            {"target_tenant_id": target_tenant_id},
        )
        assert payload is not None
        return payload

    def create_schedule(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/schedules", kwargs)
        assert payload is not None
        return payload["schedule"]

    def register_webhook(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/webhooks", kwargs)
        assert payload is not None
        return payload["endpoint"]

    def pause_schedule(self, schedule_id: str, *, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/pause",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return payload["schedule"]

    def backlog(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/backlog")
        assert payload is not None
        return payload

    def completed_by_schedule(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/completed-by-schedule")
        assert payload is not None
        return payload

    def webhook_stats(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/webhook-stats")
        assert payload is not None
        return payload


class RemoteWorkerAgent:
    """Remote worker: leases/heartbeats/acks only via coordinator HTTP."""

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
        self._stop = threading.Event()
        self.processed = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        logger.info(
            "remote worker %s starting (tenant=%s fair=%s batch_size=%d)",
            self.worker_id,
            self.tenant_id,
            self.tenant_id is None,
            self.batch_size,
        )
        while not self._stop.is_set():
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
            for job in jobs:
                if self._stop.is_set():
                    break
                try:
                    self._process(job)
                    self.processed += 1
                except ApprovalRequested as exc:
                    logger.info("job %s awaiting approval", exc.job.id)
                except LeaseError:
                    logger.warning("lease lost for job %s", job.id)
                except Exception:
                    logger.exception("processing failed for job %s", job.id)
        logger.info("remote worker %s stopped after %d jobs", self.worker_id, self.processed)
        return self.processed

    def _process(self, job: Job) -> None:
        assert job.lease_token is not None
        lease_lost = threading.Event()
        stop_hb = threading.Event()

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

    def _do_work(self, job: Job, lease_lost: threading.Event) -> dict[str, Any]:
        """Run payload work; supports stepped checkpointed jobs and plain sleep.

        Payload ``steps`` (or ``kind: "steps"``) runs a multi-step loop, writing
        ``checkpoint = {"step": N}`` after each finished step so a reclaim
        resumes from *N* instead of 0. ``step_ms`` is sleep per step.
        Legacy ``sleep_ms`` still supported for cancel/long-job soaks.
        """
        payload = job.payload if isinstance(job.payload, dict) else {}
        steps_raw = payload.get("steps")
        kind = payload.get("kind")
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
            sleep_ms = int(payload.get("sleep_ms") or 0)
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


class ScheduleNode:
    """Polls coordinator process-due endpoint."""

    def __init__(
        self,
        client: CoordinatorClient,
        *,
        poll_interval_seconds: float = 0.5,
        batch_limit: int = 100,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self._stop = threading.Event()
        self.fires = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        logger.info("schedule node starting")
        while not self._stop.is_set():
            try:
                n = self.client.process_due_schedules(limit=self.batch_limit)
                if n:
                    self.fires += n
                    logger.info("fired %d schedule(s) (total=%d)", n, self.fires)
            except Exception:
                logger.exception("schedule process-due failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.fires


class WebhookNode:
    """Webhook agent: claim via coordinator, POST itself, then finalize.

    Coordinators never perform outbound HTTP — only durable claim/finalize.
    """

    def __init__(
        self,
        client: CoordinatorClient,
        *,
        poll_interval_seconds: float = 0.5,
        batch_limit: int = 50,
        http_timeout_seconds: float = 10.0,
    ) -> None:
        self.client = client
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self.http_timeout_seconds = http_timeout_seconds
        self._stop = threading.Event()
        self.delivered = 0

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> int:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from job_queue.webhooks import post_webhook

        payload = self.client.claim_webhooks(
            limit=self.batch_limit,
            http_timeout_seconds=self.http_timeout_seconds,
        )
        claimed = list(payload.get("claimed") or [])
        n = 0

        def _one(item: dict[str, Any]) -> tuple[bool, bool]:
            """Return (handled, success)."""
            delivery = item["delivery"]
            did = delivery["id"]
            if not item.get("endpoint_enabled") or not item.get("url"):
                self.client.finalize_webhook(did, endpoint_missing=True)
                return True, False
            code, detail = post_webhook(
                item["url"],
                delivery.get("payload") or {},
                secret=item.get("secret"),
                timeout_seconds=self.http_timeout_seconds,
                idempotency_key=str(did),
            )
            self.client.finalize_webhook(
                did,
                status_code=code,
                error=None if 200 <= code < 300 else detail,
            )
            return True, 200 <= code < 300

        # Parallel POST+finalize within the claimed batch (I/O bound).
        # Cap concurrency: too many simultaneous finalize calls thrash the
        # coordinator DB pool and leave stuck delivering rows until reclaim.
        # Default sequential: high parallelism often thrash finalize + reclaim.
        max_par = int(os.environ.get("WEBHOOK_PARALLEL_POST", "1") or "1")
        if max_par < 1:
            max_par = 1
        workers = min(max_par, max(1, len(claimed)))
        if workers == 1 or len(claimed) <= 1:
            for item in claimed:
                handled, ok = _one(item)
                if handled:
                    n += 1
                if ok:
                    self.delivered += 1
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = [pool.submit(_one, item) for item in claimed]
                for fut in as_completed(futs):
                    handled, ok = fut.result()
                    if handled:
                        n += 1
                    if ok:
                        self.delivered += 1

        auto = payload.get("auto_finalized") or []
        n += len(auto)
        for r in auto:
            if r.get("success"):
                self.delivered += 1
        return n

    def run(self) -> int:
        logger.info(
            "webhook agent starting (claim+POST+finalize; coordinator does not POST)"
        )
        while not self._stop.is_set():
            try:
                n = self.run_once()
                if n:
                    logger.info(
                        "handled %d webhook claim(s) (delivered_ok=%d)",
                        n,
                        self.delivered,
                    )
            except Exception:
                logger.exception("webhook agent cycle failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.delivered


# ---------------------------------------------------------------------------
# Console entry points
# ---------------------------------------------------------------------------


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _coord_url() -> str:
    # COORDINATOR_URLS takes precedence (comma-separated for multi-coordinator).
    multi = _env("COORDINATOR_URLS")
    if multi:
        return multi
    return (
        _env("COORDINATOR_URL")
        or _env("JOB_QUEUE_COORDINATOR_URL")
        or "http://127.0.0.1:8080"
    )


def worker_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-worker-agent")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--worker-id", default=_env("WORKER_ID"))
    p.add_argument("--tenant-id", default=_env("TENANT_ID"))  # empty => fair
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    p.add_argument(
        "--visibility-timeout",
        type=int,
        default=int(_env("VISIBILITY_TIMEOUT", "30") or "30"),
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(_env("BATCH_SIZE", "1") or "1"),
        help="Jobs to lease per pull (default 1; use >1 for batch pulls)",
    )
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    tenant = args.tenant_id or None
    if tenant == "":
        tenant = None
    agent = RemoteWorkerAgent(
        client,
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        tenant_id=tenant,
        poll_interval_seconds=args.poll,
        visibility_timeout_seconds=args.visibility_timeout,
    )
    try:
        agent.run()
    except KeyboardInterrupt:
        agent.stop()
    return 0


def scheduler_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-scheduler-node")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = ScheduleNode(client, poll_interval_seconds=args.poll)
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


def webhook_main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=_env("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-webhook-node")
    p.add_argument("--coordinator", default=_coord_url())
    p.add_argument("--poll", type=float, default=float(_env("POLL_INTERVAL", "0.5") or "0.5"))
    p.add_argument(
        "--batch-limit",
        type=int,
        default=int(_env("BATCH_LIMIT", "50") or "50"),
        help="Deliveries to claim per pull",
    )
    p.add_argument(
        "--http-timeout",
        type=float,
        default=float(_env("HTTP_TIMEOUT", "10") or "10"),
    )
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = WebhookNode(
        client,
        poll_interval_seconds=args.poll,
        batch_limit=args.batch_limit,
        http_timeout_seconds=args.http_timeout,
    )
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(worker_main())
