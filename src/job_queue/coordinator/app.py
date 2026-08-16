"""Coordinator HTTP service."""

from __future__ import annotations

import json
import logging
import os
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

from job_queue.codec import (
    job_progress_event_to_dict,
)
from job_queue.queue import (
    JobQueue,
)

from job_queue.coordinator.auth import (
    _BodyTooLarge,
    _MAX_REQUEST_BODY_BYTES,
    _UUID_RE,
    _extract_request_token,
)
from job_queue.coordinator.routing import RoutingMixin
from job_queue.coordinator.jobs import JobsApiMixin
from job_queue.coordinator.webhooks import WebhooksApiMixin
from job_queue.coordinator.schedules import SchedulesApiMixin
from job_queue.coordinator.blackouts import BlackoutsApiMixin
from job_queue.coordinator.tenants import TenantsApiMixin
from job_queue.coordinator.audit import AuditApiMixin
from job_queue.coordinator.admin import AdminApiMixin
from job_queue.coordinator.workflow import WorkflowApiMixin
from job_queue.coordinator.metrics_api import MetricsApiMixin
from job_queue.coordinator.alerts_api import AlertsApiMixin
from job_queue.coordinator.workflows_api import WorkflowsApiMixin
from job_queue.coordinator.partitions_api import PartitionsApiMixin

logger = logging.getLogger(__name__)


class Coordinator(
    RoutingMixin,
    JobsApiMixin,
    WebhooksApiMixin,
    SchedulesApiMixin,
    BlackoutsApiMixin,
    TenantsApiMixin,
    AuditApiMixin,
    AdminApiMixin,
    WorkflowApiMixin,
    MetricsApiMixin,
    AlertsApiMixin,
    WorkflowsApiMixin,
    PartitionsApiMixin,
):
    """Thin HTTP façade over JobQueue (handlers split by domain)."""

    pass


def _make_handler(coordinator: Coordinator) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("http: " + format, *args)

        def _read_json(self) -> dict[str, Any] | None:
            raw_len = self.headers.get("Content-Length") or "0"
            try:
                length = int(raw_len)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid Content-Length") from exc
            if length < 0:
                raise ValueError("invalid Content-Length")
            if length <= 0:
                return None
            if length > _MAX_REQUEST_BODY_BYTES:
                raise _BodyTooLarge(
                    f"body too large ({length} > {_MAX_REQUEST_BODY_BYTES})"
                )
            raw = self.rfile.read(length)
            if not raw:
                return None
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid json: {exc}") from exc
            if data is None:
                return {}
            if not isinstance(data, dict):
                raise ValueError("json body must be an object")
            return data

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            if status == 204:
                self.send_response(204)
                self.end_headers()
                return
            # Metrics multi-format export: raw CSV / Parquet body.
            if (
                isinstance(payload, dict)
                and payload.get("format") in ("csv", "parquet")
                and isinstance(payload.get("body"), (bytes, bytearray))
            ):
                raw = bytes(payload["body"])
                ctype = str(
                    payload.get("content_type")
                    or (
                        "text/csv; charset=utf-8"
                        if payload["format"] == "csv"
                        else "application/vnd.apache.parquet"
                    )
                )
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header(
                    "Content-Disposition",
                    f'attachment; filename="metrics.{payload["format"]}"',
                )
                self.end_headers()
                self.wfile.write(raw)
                return
            # JSON metrics export wraps data under {"format","data",...}
            if (
                isinstance(payload, dict)
                and payload.get("format") == "json"
                and "data" in payload
                and "body" not in payload
            ):
                payload = payload["data"]
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _stream_job_events(self, path: str, query: dict[str, list[str]]) -> None:
            """SSE stream of live job progress (no client polling)."""
            # /v1/jobs/{id}/events?tenant_id=
            # /v1/stream/job-events?tenant_id=&tenant_id=&job_id=&label=
            try:
                tenants = list(query.get("tenant_id") or [])
                job_ids: list[UUID] = []
                labels = list(query.get("label") or query.get("labels") or [])
                idle_raw = (query.get("idle_timeout_seconds") or [None])[0]
                idle = float(idle_raw) if idle_raw not in (None, "") else None
                include_snap = (query.get("snapshot") or ["1"])[0] not in (
                    "0",
                    "false",
                    "no",
                )
                max_snap_raw = (query.get("max_snapshot_jobs") or [None])[0]
                max_snapshot_jobs: int | None
                if max_snap_raw in (None, ""):
                    max_snapshot_jobs = None  # use iter_job_events default
                else:
                    max_snapshot_jobs = int(max_snap_raw)

                if path.endswith("/events") and path.startswith("/v1/jobs/"):
                    parts = path.strip("/").split("/")
                    # v1 jobs {id} events
                    if len(parts) != 4 or not _UUID_RE.match(parts[2]):
                        self._send(400, {"error": "invalid job events path"})
                        return
                    if not tenants or not tenants[0]:
                        self._send(400, {"error": "tenant_id query param required"})
                        return
                    job_ids = [UUID(parts[2])]
                    tenants = [tenants[0]]
                elif path == "/v1/stream/job-events":
                    if not tenants:
                        self._send(
                            400,
                            {
                                "error": "tenant_id query param required (repeat for multi-tenant)"
                            },
                        )
                        return
                    for jid in query.get("job_id") or []:
                        if not _UUID_RE.match(jid):
                            self._send(400, {"error": f"invalid job_id {jid}"})
                            return
                        job_ids.append(UUID(jid))
                else:
                    self._send(404, {"error": "not found"})
                    return

                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()

                def _write(ev_name: str, data: dict[str, Any]) -> None:
                    raw = json.dumps(data, default=str, separators=(",", ":"))
                    chunk = f"event: {ev_name}\ndata: {raw}\n\n".encode("utf-8")
                    self.wfile.write(chunk)
                    self.wfile.flush()

                _write(
                    "ready",
                    {
                        "tenants": tenants,
                        "job_ids": [str(j) for j in job_ids],
                        "labels": labels,
                    },
                )
                for ev in coordinator.queue.iter_job_events(
                    tenant_ids=tenants,
                    job_ids=job_ids or None,
                    labels=labels or None,
                    include_snapshot=include_snap,
                    idle_timeout_seconds=idle,
                    heartbeat_seconds=15.0,
                    **(
                        {"max_snapshot_jobs": max_snapshot_jobs}
                        if max_snapshot_jobs is not None
                        else {}
                    ),
                ):
                    if ev.event == "ping":
                        # SSE comment keepalive
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        continue
                    name = "snapshot" if ev.snapshot else "progress"
                    _write(name, job_progress_event_to_dict(ev))
            except (BrokenPipeError, ConnectionResetError):
                return
            except ValueError as exc:
                try:
                    self._send(400, {"error": str(exc)})
                except Exception:  # noqa: BLE001
                    return
            except Exception as exc:  # noqa: BLE001
                logger.exception("sse stream error: %s", exc)
                try:
                    if not self.wfile.closed:
                        err = json.dumps({"error": str(exc)})
                        self.wfile.write(
                            f"event: error\ndata: {err}\n\n".encode("utf-8")
                        )
                        self.wfile.flush()
                except Exception:  # noqa: BLE001
                    return

        def _authorized(self, path: str) -> bool:
            """When ``coordinator.api_token`` is set, require matching credential.

            ``GET /health`` stays open for load balancers / probes.
            """
            token = coordinator.api_token
            if not token:
                return True
            if path == "/health":
                return True
            provided = _extract_request_token(self.headers)
            if provided is None:
                return False
            # Constant-time compare for shared secret. Length mismatch must
            # never raise (would turn auth failure into a 500).
            import hmac as _hmac

            try:
                return bool(_hmac.compare_digest(provided, token))
            except (TypeError, ValueError):
                return False

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            # keep trailing structure for /v1/jobs/.../ack
            if self.path.split("?", 1)[0].endswith("/") and path != "/":
                path = self.path.split("?", 1)[0].rstrip("/")
            query = parse_qs(parsed.query)
            try:
                if not self._authorized(path):
                    self._send(401, {"error": "unauthorized", "code": "auth_required"})
                    return
                # Live progress streams (SSE) — not JSON response bodies.
                if method == "GET" and (
                    path == "/v1/stream/job-events"
                    or (path.startswith("/v1/jobs/") and path.endswith("/events"))
                ):
                    self._stream_job_events(path, query)
                    return
                body = self._read_json() if method in ("POST", "PUT", "PATCH") else None
                status, payload = coordinator.handle(method, path, body, query)
                self._send(status, payload)
            except _BodyTooLarge as exc:
                self._send(413, {"error": str(exc)})
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                logger.exception("coordinator error: %s", exc)
                # Never leak stack frames to clients unless explicitly debugging.
                payload: dict[str, Any] = {
                    "error": "internal server error",
                    "code": "internal_error",
                }
                if os.environ.get("JOB_QUEUE_DEBUG", "").lower() in (
                    "1",
                    "true",
                    "yes",
                ):
                    payload["error"] = str(exc)
                    payload["trace"] = traceback.format_exc().splitlines()[-5:]
                self._send(500, payload)

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT")

        def do_DELETE(self) -> None:  # noqa: N802
            self._dispatch("DELETE")

    return Handler


def serve_coordinator(
    dsn: str,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    min_size: int = 2,
    max_size: int = 20,
) -> None:
    """Bind ThreadingHTTPServer and serve forever."""
    queue = JobQueue.connect(dsn, min_size=min_size, max_size=max_size)
    coordinator = Coordinator(queue)
    handler = _make_handler(coordinator)
    server = ThreadingHTTPServer((host, port), handler)
    logger.info("coordinator listening on http://%s:%s", host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        queue.close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-coordinator")
    p.add_argument(
        "--dsn",
        default=os.environ.get("JOB_QUEUE_DSN") or os.environ.get("DATABASE_URL"),
        help="Postgres DSN",
    )
    p.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    args = p.parse_args(argv)
    if not args.dsn:
        print("JOB_QUEUE_DSN / --dsn required", file=__import__("sys").stderr)
        return 2
    dsn = args.dsn.replace("postgresql+psycopg://", "postgresql://", 1).replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
    serve_coordinator(dsn, host=args.host, port=args.port)
    return 0
