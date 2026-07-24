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
from job_queue.queue import LeaseError

logger = logging.getLogger(__name__)


class CoordinatorClient:
    """HTTP client for the fleet coordinator."""

    def __init__(self, base_url: str, *, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        allow_204: bool = False,
    ) -> dict[str, Any] | None:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"
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
            raise RuntimeError(
                f"coordinator {method} {path} -> {exc.code}: {payload}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"coordinator unreachable: {exc}") from exc

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
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "queue_name": queue_name,
        }
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        if visibility_timeout_seconds is not None:
            body["visibility_timeout_seconds"] = visibility_timeout_seconds
        # 204 => empty
        url = f"{self.base_url}/v1/lease"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if int(resp.getcode()) == 204:
                    return None
                raw = resp.read()
                if not raw:
                    return None
                payload = json.loads(raw.decode("utf-8"))
                return job_from_dict(payload["job"])
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            raise RuntimeError(
                f"lease failed {exc.code}: {raw.decode('utf-8', errors='replace')}"
            ) from exc

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

    def process_due_schedules(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> int:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/schedules/process-due", body)
        assert payload is not None
        return int(payload.get("count") or 0)

    def process_webhooks(
        self, *, limit: int = 50, http_timeout_seconds: float = 10.0
    ) -> int:
        payload = self._request(
            "POST",
            "/v1/webhooks/process",
            {"limit": limit, "http_timeout_seconds": http_timeout_seconds},
        )
        assert payload is not None
        return int(payload.get("count") or 0)

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
    ) -> None:
        self.client = client
        self.worker_id = worker_id or f"remote-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.tenant_id = tenant_id  # None => fair multi-tenant
        self.poll_interval_seconds = poll_interval_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self._stop = threading.Event()
        self.processed = 0

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> int:
        logger.info(
            "remote worker %s starting (tenant=%s fair=%s)",
            self.worker_id,
            self.tenant_id,
            self.tenant_id is None,
        )
        while not self._stop.is_set():
            try:
                job = self.client.lease(
                    worker_id=self.worker_id,
                    tenant_id=self.tenant_id,
                    visibility_timeout_seconds=self.visibility_timeout_seconds,
                )
            except Exception:
                logger.exception("lease error; backing off")
                self._stop.wait(min(5.0, self.poll_interval_seconds * 4))
                continue

            if job is None:
                self._stop.wait(self.poll_interval_seconds)
                continue

            try:
                self._process(job)
                self.processed += 1
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
            # Fast deterministic work for fleet soak.
            time.sleep(0.02)
            result = {
                "ok": True,
                "worker_id": self.worker_id,
                "tenant_id": job.tenant_id,
                "schedule_id": str(job.schedule_id) if job.schedule_id else None,
                "host": socket.gethostname(),
            }
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
    """Polls coordinator webhook process endpoint."""

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

    def run(self) -> int:
        logger.info("webhook node starting")
        while not self._stop.is_set():
            try:
                n = self.client.process_webhooks(
                    limit=self.batch_limit,
                    http_timeout_seconds=self.http_timeout_seconds,
                )
                if n:
                    self.delivered += n
                    logger.info("processed %d webhook(s) (total=%d)", n, self.delivered)
            except Exception:
                logger.exception("webhook process failed")
            self._stop.wait(self.poll_interval_seconds)
        return self.delivered


# ---------------------------------------------------------------------------
# Console entry points
# ---------------------------------------------------------------------------


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _coord_url() -> str:
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
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    tenant = args.tenant_id or None
    if tenant == "":
        tenant = None
    agent = RemoteWorkerAgent(
        client,
        worker_id=args.worker_id,
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
    args = p.parse_args(argv)
    client = CoordinatorClient(args.coordinator)
    client.wait_healthy(timeout=float(_env("READY_TIMEOUT", "120") or "120"))
    node = WebhookNode(client, poll_interval_seconds=args.poll)
    try:
        node.run()
    except KeyboardInterrupt:
        node.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(worker_main())
