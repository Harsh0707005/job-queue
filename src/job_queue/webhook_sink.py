"""Local webhook sink used by the distributed fleet soak.

Records every POST, tracks unique delivery ids, and flags duplicates
(same delivery_id received more than once).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logger = logging.getLogger(__name__)


class WebhookSinkState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total_posts = 0
        self.by_delivery: dict[str, int] = {}
        self.by_tenant: dict[str, int] = {}
        self.by_job: dict[str, int] = {}
        self.duplicate_posts = 0

    def record(self, payload: dict[str, Any], headers: Any) -> None:
        delivery_id = str(
            payload.get("delivery_id")
            or headers.get("Idempotency-Key")
            or headers.get("X-Job-Queue-Delivery-Id")
            or f"unknown-{self.total_posts}"
        )
        tenant = str(payload.get("tenant_id") or "?")
        job_id = str(payload.get("job_id") or "?")
        with self._lock:
            self.total_posts += 1
            prev = self.by_delivery.get(delivery_id, 0)
            self.by_delivery[delivery_id] = prev + 1
            if prev >= 1:
                self.duplicate_posts += 1
            self.by_tenant[tenant] = self.by_tenant.get(tenant, 0) + 1
            self.by_job[job_id] = self.by_job.get(job_id, 0) + 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            multi = {
                k: v for k, v in self.by_delivery.items() if v > 1
            }
            return {
                "total_posts": self.total_posts,
                "unique_deliveries": len(self.by_delivery),
                # HTTP retries after claim reclaim (same delivery_id) are expected
                # under chaos; they are not logical double-deliveries.
                "retry_posts": self.duplicate_posts,
                "deliveries_with_multiple_posts": len(multi),
                "multi_post_sample": dict(list(multi.items())[:20]),
                "by_tenant": dict(self.by_tenant),
                "unique_jobs": len(self.by_job),
            }


def serve_sink(*, host: str = "0.0.0.0", port: int = 8090) -> None:
    state = WebhookSinkState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("sink: " + format, *args)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            state.record(payload, self.headers)
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] in ("/health", "/"):
                payload = {"ok": True, "role": "webhook-sink"}
            elif self.path.startswith("/stats"):
                payload = state.snapshot()
            else:
                self.send_response(404)
                self.end_headers()
                return
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer((host, port), Handler)
    logger.info("webhook sink on http://%s:%s", host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-webhook-sink")
    p.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8090")))
    args = p.parse_args(argv)
    serve_sink(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
