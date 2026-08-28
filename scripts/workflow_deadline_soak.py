#!/usr/bin/env python3
"""Fleet soak: workflow run deadlines under coordinator + worker kill.

A due run cancels leftover forwards (sleep / approval / in-flight) and
compensates only completed work. Mid-soak we kill coordinator-1 and
worker-1 so a standby tick and a lease reclaim must not double-undo or
leave pending steps.

  COMPOSE_PROJECT_NAME=jq-wfdead docker compose -p jq-wfdead \\
    -f docker-compose.yml -f docker-compose.workflow.yml \\
    -f docker-compose.workflow-deadline.yml up --build -d
  docker logs -f jq-wfdead-workflow-deadline-soak-1
"""

from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("workflow_deadline_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
HAPPY_PER = int(os.environ.get("HAPPY_PER_TENANT", "3") or "3")
SLEEP_PER = int(os.environ.get("SLEEP_PER_TENANT", "3") or "3")
APPROVE_PER = int(os.environ.get("APPROVE_PER_TENANT", "2") or "2")
FAN_PER = int(os.environ.get("FAN_PER_TENANT", "2") or "2")
DEADLINE_SECONDS = float(os.environ.get("DEADLINE_SECONDS", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-wfdead")
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


def wait_healthy(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready = 0
        for b in COORDS:
            try:
                req = urllib.request.Request(b + "/health", method="GET")
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if json.loads(resp.read().decode()).get("ok"):
                        ready += 1
            except Exception:
                pass
        if ready >= 1:
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def docker_kill(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        subprocess.run(
            ["docker", "kill", name], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("/var/run/docker.sock")
        sock.sendall(
            f"POST /v1.41/containers/{name}/kill HTTP/1.1\r\n"
            "Host: localhost\r\nContent-Length: 0\r\n\r\n".encode()
        )
        sock.recv(4096)
        sock.close()


def docker_start(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        subprocess.run(
            ["docker", "start", name], capture_output=True, text=True, check=False
        )
    except FileNotFoundError:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect("/var/run/docker.sock")
        sock.sendall(
            f"POST /v1.41/containers/{name}/start HTTP/1.1\r\n"
            "Host: localhost\r\nContent-Length: 0\r\n\r\n".encode()
        )
        sock.recv(4096)
        sock.close()


def start_wf(
    tenant: str,
    name: str,
    steps: list[dict[str, Any]],
    *,
    deadline_seconds: float | None = None,
) -> str:
    body: dict[str, Any] = {
        "tenant_id": tenant,
        "name": name,
        "max_attempts": 4,
        "visibility_timeout_seconds": 8,
        "max_compensate_retries": 8,
        "steps": steps,
    }
    if deadline_seconds is not None:
        body["deadline_seconds"] = deadline_seconds
    code, payload = http_json("POST", "/v1/workflows", body)
    if code not in (200, 201):
        raise RuntimeError(f"start {tenant}/{name} -> {code}: {payload}")
    rid = str((payload.get("run") or {}).get("id") or "")
    if not rid:
        raise RuntimeError(f"no run id: {payload}")
    return rid


def get_run(tid: str, rid: str) -> dict[str, Any]:
    code, body = http_json(
        "GET", f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return body.get("run") or {}


def list_steps(tid: str, rid: str) -> list[dict[str, Any]]:
    code, body = http_json(
        "GET", f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return []
    return list(body.get("steps") or [])


def _reserve() -> dict[str, Any]:
    return {
        "name": "reserve",
        "kind": "effect",
        "payload": {"op": "reserve", "ms": 30},
        "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
    }


def happy_steps() -> list[dict[str, Any]]:
    return [
        _reserve(),
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def sleep_steps() -> list[dict[str, Any]]:
    return [
        _reserve(),
        {"name": "pause", "kind": "sleep", "seconds": 60},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def approve_steps() -> list[dict[str, Any]]:
    return [
        _reserve(),
        {"name": "gate", "kind": "approval", "message": "hold"},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def fan_steps() -> list[dict[str, Any]]:
    return [
        _reserve(),
        {
            "type": "parallel",
            "name": "fan",
            "branches": [
                [
                    {
                        "name": "slow",
                        "kind": "effect",
                        "payload": {"op": "slow", "ms": 400},
                    }
                ],
                [{"name": "pause", "kind": "sleep", "seconds": 60}],
            ],
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def tenant_ids() -> list[str]:
    return [f"wfdead-{i:02d}" for i in range(TENANTS)]


def wait_terminal(runs: list[tuple[str, str, str]], *, timeout: float) -> dict[str, int]:
    deadline = time.time() + timeout
    counts: dict[str, int] = {}
    while time.time() < deadline:
        counts = {
            "completed": 0,
            "cancelled": 0,
            "failed": 0,
            "other": 0,
        }
        for tid, rid, _kind in runs:
            st = str(get_run(tid, rid).get("status") or "")
            if st in counts:
                counts[st] += 1
            else:
                counts["other"] += 1
        logger.info("drain %s", counts)
        if counts["other"] == 0:
            return counts
        time.sleep(1.5)
    raise RuntimeError(f"did not drain: {counts}")


def assert_deadline_run(tid: str, rid: str, *, parked: str) -> None:
    run = get_run(tid, rid)
    if run.get("status") != "cancelled":
        raise RuntimeError(f"{tid}/{rid} status={run.get('status')} want cancelled")
    if "deadline" not in str(run.get("fail_reason") or ""):
        raise RuntimeError(f"{tid}/{rid} bad reason: {run.get('fail_reason')}")
    steps = list_steps(tid, rid)
    pending = [
        s.get("name")
        for s in steps
        if s.get("direction") == "forward"
        and s.get("status")
        in (
            "pending",
            "running",
            "awaiting_sleep",
            "awaiting_timer",
            "awaiting_signal",
            "awaiting_approval",
        )
    ]
    if pending:
        raise RuntimeError(f"{tid}/{rid} leftover pending {pending}")
    parked_row = next((s for s in steps if s.get("name") == parked), None)
    if parked_row is not None and parked_row.get("status") not in (
        "skipped",
        "cancelled",
    ):
        raise RuntimeError(f"{tid}/{rid} {parked}={parked_row.get('status')}")
    comps = [
        s
        for s in steps
        if s.get("direction") == "compensate" and s.get("name") == "reserve"
    ]
    if len(comps) > 1:
        raise RuntimeError(f"{tid}/{rid} double compensate {comps}")
    reserve = next(
        (
            s
            for s in steps
            if s.get("name") == "reserve" and s.get("direction") == "forward"
        ),
        None,
    )
    if reserve is not None and reserve.get("status") == "completed":
        if not comps:
            raise RuntimeError(f"{tid}/{rid} completed reserve not compensated")


def main() -> int:
    logger.info(
        "workflow deadline soak tenants=%d happy=%d sleep=%d approve=%d fan=%d",
        TENANTS,
        HAPPY_PER,
        SLEEP_PER,
        APPROVE_PER,
        FAN_PER,
    )
    wait_healthy()
    tenants = tenant_ids()
    runs: list[tuple[str, str, str]] = []
    errors: list[str] = []

    def launch(tid: str, kind: str, n: int) -> tuple[str, str, str]:
        if kind == "happy":
            rid = start_wf(tid, f"{kind}-{n}", happy_steps(), deadline_seconds=120)
        elif kind == "sleep":
            rid = start_wf(
                tid, f"{kind}-{n}", sleep_steps(), deadline_seconds=DEADLINE_SECONDS
            )
        elif kind == "approve":
            rid = start_wf(
                tid, f"{kind}-{n}", approve_steps(), deadline_seconds=DEADLINE_SECONDS
            )
        else:
            rid = start_wf(
                tid, f"{kind}-{n}", fan_steps(), deadline_seconds=DEADLINE_SECONDS
            )
        return tid, rid, kind

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for tid in tenants:
            for n in range(HAPPY_PER):
                futs.append(pool.submit(launch, tid, "happy", n))
            for n in range(SLEEP_PER):
                futs.append(pool.submit(launch, tid, "sleep", n))
            for n in range(APPROVE_PER):
                futs.append(pool.submit(launch, tid, "approve", n))
            for n in range(FAN_PER):
                futs.append(pool.submit(launch, tid, "fan", n))
        for fut in as_completed(futs):
            try:
                runs.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
    if errors:
        raise RuntimeError(f"start errors: {errors[:5]}")
    logger.info("started %d runs", len(runs))

    time.sleep(0.6)
    logger.info("killing coordinator-1")
    docker_kill("coordinator-1")
    time.sleep(1.2)
    logger.info("killing worker-1")
    docker_kill("worker-1")
    time.sleep(2)
    docker_start("coordinator-1")
    docker_start("worker-1")

    counts = wait_terminal(runs, timeout=MAX_WALL)
    want_ok = TENANTS * HAPPY_PER
    want_cancel = TENANTS * (SLEEP_PER + APPROVE_PER + FAN_PER)
    if counts["completed"] != want_ok:
        raise RuntimeError(f"completed {counts['completed']} want {want_ok}")
    if counts["cancelled"] != want_cancel:
        raise RuntimeError(f"cancelled {counts['cancelled']} want {want_cancel}")
    if counts["failed"] != 0:
        raise RuntimeError(f"unexpected failed {counts['failed']}")

    kinds = Counter(k for _t, _r, k in runs)
    logger.info("kinds %s", dict(kinds))
    for tid, rid, kind in runs:
        if kind == "happy":
            steps = list_steps(tid, rid)
            pending = [
                s.get("name")
                for s in steps
                if s.get("status")
                in ("pending", "running", "awaiting_sleep", "awaiting_approval")
            ]
            if pending:
                raise RuntimeError(f"{tid}/{rid} happy leftover {pending}")
            continue
        parked = {"sleep": "pause", "approve": "gate", "fan": "pause"}[kind]
        assert_deadline_run(tid, rid, parked=parked)

    logger.info("workflow deadline soak OK %s", counts)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow deadline soak failed")
        sys.exit(1)
