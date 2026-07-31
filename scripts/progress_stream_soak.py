#!/usr/bin/env python3
"""Fleet load: multi-tenant SSE job progress (no client polling).

Opens long-lived SSE streams to dual coordinators for several tenants,
enqueues stepped jobs, and asserts live leased/checkpoint/completed events
arrive without GET polling. Measures event throughput under worker load.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("progress_stream_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "30") or "30")
STEPS = int(os.environ.get("STEPS", "4") or "4")
STEP_MS = int(os.environ.get("STEP_MS", "80") or "80")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120") or "120")
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDS)
        _rr += 1
        bases = COORDS[start:] + COORDS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


class StreamStats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events = 0
        self.by_event: dict[str, int] = {}
        self.by_tenant: dict[str, int] = {}
        self.by_status: dict[str, int] = {}
        self.job_ids: set[str] = set()
        self.completed_jobs: set[str] = set()
        self.checkpointed_jobs: set[str] = set()
        self.errors: list[str] = []

    def on_event(self, data: dict[str, Any]) -> None:
        with self.lock:
            self.events += 1
            ev = str(data.get("event") or "?")
            tid = str(data.get("tenant_id") or "?")
            st = str(data.get("status") or "?")
            jid = str(data.get("job_id") or "")
            self.by_event[ev] = self.by_event.get(ev, 0) + 1
            self.by_tenant[tid] = self.by_tenant.get(tid, 0) + 1
            self.by_status[st] = self.by_status.get(st, 0) + 1
            if jid:
                self.job_ids.add(jid)
            if st == "completed" and jid:
                self.completed_jobs.add(jid)
            if ev == "checkpointed" and jid:
                self.checkpointed_jobs.add(jid)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "events": self.events,
                "by_event": dict(self.by_event),
                "by_tenant": dict(self.by_tenant),
                "by_status": dict(self.by_status),
                "unique_jobs": len(self.job_ids),
                "completed_jobs": len(self.completed_jobs),
                "checkpointed_jobs": len(self.checkpointed_jobs),
                "errors": list(self.errors),
            }


def sse_reader(
    base: str,
    *,
    tenants: list[str],
    stats: StreamStats,
    stop: threading.Event,
) -> None:
    q = []
    for t in tenants:
        q.append(("tenant_id", t))
    q.append(("snapshot", "1"))
    url = base + "/v1/stream/job-events?" + urlencode(q)
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    try:
        with urllib.request.urlopen(req, timeout=MAX_WALL + 30) as resp:
            event_name = "message"
            data_lines: list[str] = []
            while not stop.is_set():
                line = resp.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                if text.startswith(":"):
                    continue
                if text.startswith("event:"):
                    event_name = text[6:].strip()
                elif text.startswith("data:"):
                    data_lines.append(text[5:].lstrip())
                elif text == "":
                    if data_lines:
                        raw = "\n".join(data_lines)
                        data_lines = []
                        if event_name in ("progress", "snapshot"):
                            try:
                                stats.on_event(json.loads(raw))
                            except json.JSONDecodeError as exc:
                                with stats.lock:
                                    stats.errors.append(str(exc))
                        event_name = "message"
    except Exception as exc:  # noqa: BLE001
        if not stop.is_set():
            with stats.lock:
                stats.errors.append(f"{base}: {exc}")


def main() -> int:
    logger.info(
        "progress stream soak tenants=%d jobs/tenant=%d steps=%d",
        TENANTS,
        JOBS_PER_TENANT,
        STEPS,
    )
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")

    tenants = [f"prog-{i:02d}" for i in range(TENANTS)]
    stats = StreamStats()
    stop = threading.Event()
    threads: list[threading.Thread] = []
    # One multi-tenant stream per coordinator (dual-coord fan-in via NOTIFY).
    for base in COORDS:
        th = threading.Thread(
            target=sse_reader,
            args=(base,),
            kwargs={"tenants": tenants, "stats": stats, "stop": stop},
            daemon=True,
        )
        th.start()
        threads.append(th)
    time.sleep(0.5)

    expected = TENANTS * JOBS_PER_TENANT
    for tid in tenants:
        for j in range(JOBS_PER_TENANT):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {
                        "kind": "steps",
                        "steps": STEPS,
                        "step_ms": STEP_MS,
                    },
                    "labels": ["stream:soak", f"tenant:{tid}"],
                    "priority": 0,
                    "max_attempts": 3,
                    "visibility_timeout_seconds": 60,
                },
            )
    logger.info("seeded %d stepped jobs", expected)

    t0 = time.monotonic()
    while time.monotonic() - t0 < MAX_WALL:
        s = stats.snapshot()
        logger.info("stream stats=%s", s)
        if (
            s["completed_jobs"] >= expected * 0.9
            and s["checkpointed_jobs"] >= expected * 0.5
            and len(s["by_tenant"]) >= TENANTS
        ):
            break
        time.sleep(1.0)

    stop.set()
    for th in threads:
        th.join(timeout=2)

    s = stats.snapshot()
    logger.info("final stream stats=%s elapsed=%.1fs", s, time.monotonic() - t0)
    if s["errors"]:
        logger.error("stream errors: %s", s["errors"][:5])
        return 1
    if s["completed_jobs"] < expected * 0.85:
        logger.error(
            "too few completed events: %d want ~%d",
            s["completed_jobs"],
            expected,
        )
        return 1
    if s["checkpointed_jobs"] < expected * 0.4:
        logger.error("too few checkpoint events: %s", s)
        return 1
    if len(s["by_tenant"]) < TENANTS:
        logger.error("missing tenants in stream: %s", s["by_tenant"])
        return 1
    # Dual streams may double-count; unique jobs is the truth.
    if s["unique_jobs"] < expected * 0.85:
        logger.error("too few unique jobs in stream: %s", s)
        return 1

    logger.info(
        "PROGRESS STREAM SOAK PASSED events=%d unique_jobs=%d completed=%d checkpoints=%d",
        s["events"],
        s["unique_jobs"],
        s["completed_jobs"],
        s["checkpointed_jobs"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
