#!/usr/bin/env python3
"""Fleet soak: move a live workflow-tick schedule across tenants.

Dual coordinators + eight workers + effect-sink:

  * Each source pair fires a multi-step tick (reserve → wait_signal → ship).
  * Move while the first step is live or the wait is parked.
  * Dest sends the signal; dest workers finish the run and seal the generation.
  * Source can no longer see the run or send the signal.
  * Other tenants keep completing their own ticks through the moves.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.schedule-move-workflow.yml up --build \
    --abort-on-container-exit --exit-code-from schedule-move-wf-soak \
    schedule-move-wf-soak
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schedule_move_workflow_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
PAIRS = int(os.environ.get("PAIRS", "2") or "2")
OTHERS = int(os.environ.get("OTHERS", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return int(resp.status), json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            code, h = http_json("GET", "/health", timeout=5)
            if code == 200 and h.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def future() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()


def create_sched(tid: str, name: str, **extra: Any) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "interval_seconds": 3600,
        "payload": {"who": name},
        "start_at": future(),
        "max_attempts": 1,
        "visibility_timeout_seconds": 8,
    }
    body.update(extra)
    code, out = http_json("POST", "/v1/schedules", body)
    if code not in (200, 201):
        raise RuntimeError(f"create {tid}/{name} -> {code}: {out}")
    sid = str((out.get("schedule") or {}).get("id") or "")
    if not sid:
        raise RuntimeError(f"no schedule id: {out}")
    return sid


def fire(tid: str, sid: str) -> dict[str, Any]:
    code, out = http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})
    if code not in (200, 201):
        raise RuntimeError(f"fire {tid} {sid} -> {code}: {out}")
    return out


def get_run(tid: str, rid: str) -> dict[str, Any]:
    code, out = http_json(
        "GET", f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return out.get("run") or {}


def wf_body(signal: str) -> dict[str, Any]:
    return {
        "name": "tick-wf",
        "steps": [
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve", "ms": 120},
                "compensate": {
                    "kind": "effect",
                    "payload": {"op": "release", "ms": 20},
                },
            },
            {
                "name": "gate",
                "kind": "wait_signal",
                "signal": signal,
                "timeout_seconds": 90,
            },
            {
                "name": "ship",
                "kind": "effect",
                "payload": {"op": "ship", "ms": 40},
            },
        ],
    }


def enqueue_other(tid: str, n: int) -> None:
    for i in range(n):
        code, out = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"op": "load", "i": i, "ms": 20},
                "max_attempts": 1,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"load {tid} -> {code}: {out}")


def main() -> int:
    wait_healthy()
    pairs = [(f"smw-src-{i}", f"smw-dst-{i}") for i in range(PAIRS)]
    others = [f"smw-oth-{i}" for i in range(OTHERS)]
    logger.info("schedule-move-workflow soak pairs=%s others=%s", pairs, others)

    runs: dict[str, str] = {}
    scheds: dict[str, str] = {}
    for src, _dst in pairs:
        sig = f"go-{src}"
        sid = create_sched(src, f"wf-{src}", workflow=wf_body(sig))
        scheds[src] = sid
        out = fire(src, sid)
        rid = str(out.get("workflow_run_id") or "")
        if not rid:
            raise RuntimeError(f"fire missing workflow_run_id: {out}")
        runs[src] = rid
        http_json(
            "POST",
            f"/v1/workflows/{rid}/signals",
            {"tenant_id": src, "name": "early", "payload": {"src": src}},
        )

    for tid in others:
        enqueue_other(tid, 12)
        oid = create_sched(
            tid,
            f"plain-{tid}",
            workflow={
                "name": "plain",
                "steps": [
                    {
                        "name": "only",
                        "kind": "effect",
                        "payload": {"op": "only", "ms": 30},
                    }
                ],
            },
        )
        fire(tid, oid)

    # Give workers a moment to lease reserve, then move.
    time.sleep(0.15)
    for src, dst in pairs:
        sid = scheds[src]
        code, out = http_json(
            "POST",
            f"/v1/schedules/{sid}/move",
            {"tenant_id": src, "to_tenant_id": dst},
        )
        if code != 200:
            raise RuntimeError(f"move {src}->{dst} -> {code}: {out}")
        if (out.get("schedule") or {}).get("tenant_id") != dst:
            raise RuntimeError(f"move did not land on dest: {out}")
        rid = runs[src]
        if get_run(src, rid):
            raise RuntimeError(f"source {src} still sees run {rid}")
        code, body = http_json(
            "POST",
            f"/v1/workflows/{rid}/signals",
            {"tenant_id": src, "name": sig_name(src)},
        )
        if code != 404:
            raise RuntimeError(f"source send expected 404 got {code}: {body}")
        logger.info("moved %s -> %s run=%s", src, dst, rid)

    # Dest unblocks the parked wait (or the wait that parks after reserve acks).
    t0 = time.time()
    sent: set[str] = set()
    while time.time() - t0 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 40})
        done = 0
        for src, dst in pairs:
            rid = runs[src]
            st = get_run(dst, rid).get("status")
            if st == "completed":
                done += 1
                continue
            if src in sent:
                continue
            code, steps = http_json(
                "GET",
                f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": dst}),
            )
            if code != 200:
                continue
            gate = next(
                (s for s in (steps.get("steps") or []) if s.get("name") == "gate"),
                None,
            )
            if gate and gate.get("status") in ("awaiting_signal", "completed"):
                if gate.get("status") == "awaiting_signal":
                    sc, sb = http_json(
                        "POST",
                        f"/v1/workflows/{rid}/signals",
                        {
                            "tenant_id": dst,
                            "name": sig_name(src),
                            "payload": {"who": dst},
                        },
                    )
                    if sc not in (200, 201):
                        raise RuntimeError(f"dest send {dst} -> {sc}: {sb}")
                sent.add(src)
        if done == len(pairs) and len(sent) == len(pairs):
            break
        time.sleep(0.25)
    else:
        raise RuntimeError(
            f"timeout done wait sent={len(sent)}/{len(pairs)}"
        )

    for src, dst in pairs:
        if get_run(src, runs[src]):
            raise RuntimeError(f"source {src} regained run")
        code, jobs = http_json(
            "GET", "/v1/jobs?" + urlencode({"tenant_id": src, "limit": "20"})
        )
        leftover = [
            j
            for j in (jobs.get("jobs") or [])
            if str(j.get("schedule_id") or "") == scheds[src]
        ]
        if leftover:
            raise RuntimeError(f"source {src} still has tick jobs: {leftover[:2]}")

    logger.info(
        "schedule move workflow soak OK pairs=%s sent=%s",
        len(pairs),
        len(sent),
    )
    return 0


def sig_name(src: str) -> str:
    return f"go-{src}"


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule-move-workflow soak failed")
        sys.exit(1)
