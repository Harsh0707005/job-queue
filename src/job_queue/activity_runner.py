"""In-process activity runner used by the workflow-activity fleet soak.

Workers POST /start. The runner sleeps ``work_ms`` (or payload.work_ms)
then marks the activity done. GET /status?id= reports completion. The
worker keeps heartbeating the coordinator while it polls this runner.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


class ActivityRunnerState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.by_id: dict[str, dict[str, Any]] = {}
        self.starts = 0
        self.completes = 0

    def start(self, payload: dict[str, Any], headers: Any) -> dict[str, Any]:
        aid = str(payload.get("activity_id") or headers.get("Idempotency-Key") or "")
        if not aid:
            raise ValueError("activity_id is required")
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        try:
            work_ms = int(
                inner.get("work_ms")
                or payload.get("work_ms")
                or os.environ.get("DEFAULT_WORK_MS")
                or 400
            )
        except (TypeError, ValueError):
            work_ms = 400
        fail = bool(inner.get("fail") or payload.get("fail"))
        with self._lock:
            existing = self.by_id.get(aid)
            if existing is not None:
                return {"accepted": False, "activity_id": aid, "status": existing["status"]}
            rec = {
                "activity_id": aid,
                "status": "running",
                "done": False,
                "result": None,
                "error": None,
                "started_at": time.time(),
                "work_ms": work_ms,
                "fail": fail,
            }
            self.by_id[aid] = rec
            self.starts += 1
        threading.Thread(
            target=self._run, args=(aid, work_ms, fail), daemon=True
        ).start()
        return {"accepted": True, "activity_id": aid, "status": "running"}

    def _run(self, aid: str, work_ms: int, fail: bool) -> None:
        time.sleep(max(0, work_ms) / 1000.0)
        with self._lock:
            rec = self.by_id.get(aid)
            if rec is None or rec["status"] != "running":
                return
            rec["done"] = True
            if fail:
                rec["status"] = "failed"
                rec["error"] = "activity runner forced fail"
            else:
                rec["status"] = "completed"
                rec["result"] = {"ok": True, "activity_id": aid, "work_ms": work_ms}
            self.completes += 1

    def status(self, aid: str) -> dict[str, Any] | None:
        with self._lock:
            rec = self.by_id.get(aid)
            if rec is None:
                return None
            return {
                "activity_id": aid,
                "status": rec["status"],
                "done": bool(rec["done"]),
                "result": rec.get("result"),
                "error": rec.get("error"),
            }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            running = sum(1 for r in self.by_id.values() if r["status"] == "running")
            return {
                "starts": self.starts,
                "completes": self.completes,
                "unique": len(self.by_id),
                "running": running,
            }


def serve_runner(*, host: str = "0.0.0.0", port: int = 8092) -> None:
    state = ActivityRunnerState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("activity-runner: " + format, *args)

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            raw = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in ("/health", "/"):
                self._send(200, {"ok": True, **state.snapshot()})
                return
            if parsed.path == "/status":
                aid = (parse_qs(parsed.query).get("id") or [""])[0]
                rec = state.status(aid)
                if rec is None:
                    self._send(404, {"error": "unknown activity"})
                    return
                self._send(200, rec)
                return
            if parsed.path == "/stats":
                self._send(200, state.snapshot())
                return
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path.rstrip("/") != "/start":
                self._send(404, {"error": "not found"})
                return
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send(400, {"error": "invalid json"})
                return
            try:
                out = state.start(payload, self.headers)
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
                return
            self._send(200, out)

    httpd = ThreadingHTTPServer((host, port), Handler)
    logger.info("activity runner listening on %s:%s", host, port)
    httpd.serve_forever()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8092"))
    serve_runner(host=host, port=port)


if __name__ == "__main__":
    main()
