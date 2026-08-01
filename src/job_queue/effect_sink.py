"""HTTP sink for once-only side-effect load tests.

Accepts POSTs with Idempotency-Key / job_id+effect_key. Counts unique
logical effects vs total HTTP posts (retries after reclaim).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logger = logging.getLogger(__name__)


class EffectSinkState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total_posts = 0
        self.by_key: dict[str, int] = {}
        self.by_job: dict[str, set[str]] = {}
        self.first_worker: dict[str, str] = {}
        self.workers_seen: dict[str, set[str]] = {}

    def record(self, payload: dict[str, Any], headers: Any) -> dict[str, Any]:
        job_id = str(payload.get("job_id") or "?")
        effect_key = str(payload.get("effect_key") or "?")
        worker = str(payload.get("worker_id") or "?")
        idem = str(
            payload.get("idempotency_key")
            or headers.get("Idempotency-Key")
            or f"{job_id}:{effect_key}"
        )
        with self._lock:
            self.total_posts += 1
            prev = self.by_key.get(idem, 0)
            self.by_key[idem] = prev + 1
            self.by_job.setdefault(job_id, set()).add(effect_key)
            self.workers_seen.setdefault(idem, set()).add(worker)
            if prev == 0:
                self.first_worker[idem] = worker
                accepted = True
            else:
                accepted = False
            return {
                "accepted": accepted,
                "idempotency_key": idem,
                "attempts": prev + 1,
            }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            multi = {k: v for k, v in self.by_key.items() if v > 1}
            multi_workers = {
                k: sorted(v) for k, v in self.workers_seen.items() if len(v) > 1
            }
            return {
                "total_posts": self.total_posts,
                "unique_effects": len(self.by_key),
                "retry_posts": self.total_posts - len(self.by_key),
                "effects_with_retries": len(multi),
                "effects_seen_by_multiple_workers": len(multi_workers),
                "multi_worker_sample": dict(list(multi_workers.items())[:15]),
                "unique_jobs": len(self.by_job),
                "effects_per_job_sample": {
                    j: sorted(e) for j, e in list(self.by_job.items())[:10]
                },
            }


def serve_sink(*, host: str = "0.0.0.0", port: int = 8091) -> None:
    state = EffectSinkState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("effect-sink: " + format, *args)

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            raw = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] in ("/health", "/"):
                self._send(200, {"ok": True, "role": "effect-sink"})
                return
            if self.path.split("?", 1)[0] in ("/stats", "/v1/stats"):
                self._send(200, state.snapshot())
                return
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {}
            if self.path.split("?", 1)[0] not in ("/effect", "/v1/effect", "/"):
                self._send(404, {"error": "not found"})
                return
            out = state.record(
                payload if isinstance(payload, dict) else {}, self.headers
            )
            # Always 200 so retries are safe; accepted=false means duplicate key.
            self._send(200, out)

    httpd = ThreadingHTTPServer((host, port), Handler)
    logger.info("effect sink listening on %s:%s", host, port)
    httpd.serve_forever()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8091"))
    serve_sink(host=host, port=port)


if __name__ == "__main__":
    main()
