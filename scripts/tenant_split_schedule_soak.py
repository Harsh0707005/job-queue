#!/usr/bin/env python3
"""Fleet soak: split matching live schedules including a mid-tick workflow.

  * Source tenants have an email/xfer workflow-tick schedule (reserve →
    wait_signal → ship) and a leftover default schedule that must keep
    working on source.
  * Split by queue=email moves the live tick, run, steps, signals, jobs,
    and sealed generations onto dest. Dest sends the signal and finishes.
  * Source leftover jobs still complete. Other tenants keep running.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.tenant-split-schedule.yml up --build \
    --abort-on-container-exit --exit-code-from tenant-split-sched-soak \
    tenant-split-sched-soak
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
logger = logging.getLogger("tenant_split_schedule_soak")

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
KEEP_JOBS = int(os.environ.get("KEEP_JOBS", "8") or "8")
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
                "payload": {"op": "reserve", "ms": 150},
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


def enqueue(tid: str, queue_name: str, n: int) -> None:
    for i in range(n):
        code, out = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "queue_name": queue_name,
                "payload": {"op": "load", "i": i, "ms": 20},
                "max_attempts": 1,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"enqueue {tid} {queue_name} -> {code}: {out}")


def main() -> int:
    wait_healthy()
    pairs = [(f"spls-src-{i}", f"spls-dst-{i}") for i in range(PAIRS)]
    others = [f"spls-oth-{i}" for i in range(OTHERS)]
    logger.info("split-schedule soak pairs=%s others=%s", pairs, others)

    runs: dict[str, str] = {}
    moved: dict[str, str] = {}
    leftover: dict[str, str] = {}
    for src, _dst in pairs:
        sig = f"go-{src}"
        sid = create_sched(
            src,
            f"wf-{src}",
            queue_name="email",
            labels=["xfer"],
            workflow=wf_body(sig),
        )
        moved[src] = sid
        leftover[src] = create_sched(
            src, f"keep-{src}", queue_name="default", labels=["keep"]
        )
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
        enqueue(src, "default", KEEP_JOBS)

    for tid in others:
        enqueue(tid, "default", KEEP_JOBS)
        oid = create_sched(
            tid,
            f"plain-{tid}",
            workflow={
                "name": "plain",
                "steps": [
                    {
                        "name": "only",
                        "kind": "effect",
                        "payload": {"op": "only", "ms": 25},
                    }
                ],
            },
        )
        fire(tid, oid)

    time.sleep(0.15)
    for src, dst in pairs:
        code, plan = http_json(
            "POST",
            f"/v1/tenants/{src}/split/plan",
            {"target_tenant_id": dst, "queue_name": "email"},
        )
        if code != 200 or not plan.get("ok"):
            raise RuntimeError(f"plan {src}->{dst} -> {code}: {plan}")
        if moved[src] not in (plan.get("move_schedule_ids") or []):
            raise RuntimeError(f"plan missed schedule {moved[src]}: {plan}")
        code, out = http_json(
            "POST",
            f"/v1/tenants/{src}/split",
            {"target_tenant_id": dst, "queue_name": "email"},
        )
        if code != 200:
            raise RuntimeError(f"split {src}->{dst} -> {code}: {out}")
        if int(out.get("schedules_moved") or 0) < 1:
            raise RuntimeError(f"split moved no schedules: {out}")
        rid = runs[src]
        if get_run(src, rid):
            raise RuntimeError(f"source {src} still sees run {rid}")
        code, body = http_json(
            "POST",
            f"/v1/workflows/{rid}/signals",
            {"tenant_id": src, "name": f"go-{src}"},
        )
        if code != 404:
            raise RuntimeError(f"source send expected 404 got {code}: {body}")
        logger.info(
            "split %s -> %s schedules=%s jobs=%s",
            src,
            dst,
            out.get("schedules_moved"),
            out.get("jobs_moved"),
        )

    t0 = time.time()
    sent: set[str] = set()
    leftover_fired: set[str] = set()
    while time.time() - t0 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 40})
        done = 0
        for src, dst in pairs:
            rid = runs[src]
            st = get_run(dst, rid).get("status")
            if st == "completed":
                done += 1
            elif src not in sent:
                code, steps = http_json(
                    "GET",
                    f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": dst}),
                )
                gate = next(
                    (
                        s
                        for s in (steps.get("steps") or [])
                        if s.get("name") == "gate"
                    ),
                    None,
                )
                if gate and gate.get("status") == "awaiting_signal":
                    sc, sb = http_json(
                        "POST",
                        f"/v1/workflows/{rid}/signals",
                        {
                            "tenant_id": dst,
                            "name": f"go-{src}",
                            "payload": {"who": dst},
                        },
                    )
                    if sc not in (200, 201):
                        raise RuntimeError(f"dest send {dst} -> {sc}: {sb}")
                    sent.add(src)
                elif gate and gate.get("status") == "completed":
                    sent.add(src)
            if src not in leftover_fired:
                try:
                    fire(src, leftover[src])
                    leftover_fired.add(src)
                except RuntimeError:
                    pass
        if done == len(pairs) and len(leftover_fired) == len(pairs):
            break
        time.sleep(0.25)
    else:
        raise RuntimeError(
            f"timeout done sent={len(sent)} leftover={len(leftover_fired)}"
        )

    for src, _dst in pairs:
        code, jobs = http_json(
            "GET",
            "/v1/jobs?"
            + urlencode({"tenant_id": src, "queue_name": "email", "limit": "20"}),
        )
        leftover_email = jobs.get("jobs") or []
        if leftover_email:
            raise RuntimeError(f"source {src} still has email jobs")
        code, stay = http_json(
            "GET",
            "/v1/jobs?"
            + urlencode({"tenant_id": src, "queue_name": "default", "limit": "50"}),
        )
        if not (stay.get("jobs") or leftover_fired):
            raise RuntimeError(f"source {src} leftover queue went empty too early")

    logger.info(
        "tenant split schedule soak OK pairs=%s sent=%s leftover=%s",
        len(pairs),
        len(sent),
        len(leftover_fired),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("split-schedule soak failed")
        sys.exit(1)
