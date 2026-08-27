#!/usr/bin/env python3
"""Fleet soak: durable workflow sleep + timer under coordinator kill.

Sleep parks until deadline_at then continues. Timer parks until deadline
then fails and reverse-compensates. Mid-soak we kill coordinator-1 so a
standby has to fire the due rows. Short visibility reclaims in-flight
effect leases without losing a parked wait.

  COMPOSE_PROJECT_NAME=jq-wftm docker compose -p jq-wftm \\
    -f docker-compose.yml -f docker-compose.workflow.yml \\
    -f docker-compose.workflow-timer.yml up --build -d
  docker logs -f jq-wftm-workflow-soak-1
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("workflow_timer_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
SLEEP_PER = int(os.environ.get("SLEEP_PER_TENANT", "3") or "3")
TIMER_PER = int(os.environ.get("TIMER_PER_TENANT", "2") or "2")
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2") or "2")
TIMER_SECONDS = float(os.environ.get("TIMER_SECONDS", "1") or "1")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-wftm")
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
        r = subprocess.run(
            ["docker", "kill", name], capture_output=True, text=True, check=False
        )
        if r.returncode == 0:
            return
    except FileNotFoundError:
        pass
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/var/run/docker.sock")
    req = (
        f"POST /v1.41/containers/{name}/kill HTTP/1.1\r\n"
        "Host: localhost\r\nContent-Length: 0\r\n\r\n"
    )
    sock.sendall(req.encode())
    sock.recv(4096)
    sock.close()


def docker_start(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    try:
        subprocess.run(
            ["docker", "start", name], capture_output=True, text=True, check=False
        )
        return
    except FileNotFoundError:
        pass
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("/var/run/docker.sock")
    req = (
        f"POST /v1.41/containers/{name}/start HTTP/1.1\r\n"
        "Host: localhost\r\nContent-Length: 0\r\n\r\n"
    )
    sock.sendall(req.encode())
    sock.recv(4096)
    sock.close()


def start_wf(tenant: str, name: str, steps: list[dict[str, Any]]) -> str:
    code, body = http_json(
        "POST",
        "/v1/workflows",
        {
            "tenant_id": tenant,
            "name": name,
            "max_attempts": 1,
            "visibility_timeout_seconds": 4,
            "max_compensate_retries": 8,
            "steps": steps,
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"start {tenant}/{name} -> {code}: {body}")
    rid = str((body.get("run") or {}).get("id") or "")
    if not rid:
        raise RuntimeError(f"no run id: {body}")
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


def sleep_steps() -> list[dict[str, Any]]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 40},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        {"name": "pause", "kind": "sleep", "seconds": SLEEP_SECONDS},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 30}},
    ]


def timer_steps() -> list[dict[str, Any]]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 40},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        {"name": "watch", "kind": "timer", "timeout_seconds": TIMER_SECONDS},
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 30}},
    ]


def tenant_ids() -> list[str]:
    return [f"wftm-{i:02d}" for i in range(TENANTS)]


def main() -> int:
    logger.info(
        "workflow timer soak tenants=%d sleep=%d timer=%d",
        TENANTS,
        SLEEP_PER,
        TIMER_PER,
    )
    wait_healthy()
    tenants = tenant_ids()
    sleeps: list[tuple[str, str]] = []
    timers: list[tuple[str, str]] = []
    errors: list[str] = []

    def launch(tid: str, kind: str, n: int) -> tuple[str, str, str]:
        name = f"{kind}-{n}"
        steps = sleep_steps() if kind == "sleep" else timer_steps()
        rid = start_wf(tid, name, steps)
        return tid, kind, rid

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for tid in tenants:
            for n in range(SLEEP_PER):
                futs.append(pool.submit(launch, tid, "sleep", n))
            for n in range(TIMER_PER):
                futs.append(pool.submit(launch, tid, "timer", n))
        for fut in as_completed(futs):
            try:
                tid, kind, rid = fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                continue
            if kind == "sleep":
                sleeps.append((tid, rid))
            else:
                timers.append((tid, rid))
    if errors:
        raise RuntimeError(f"start errors: {errors[:5]}")
    logger.info("started sleep=%d timer=%d", len(sleeps), len(timers))

    time.sleep(0.8)
    logger.info("killing coordinator-1 mid-wait")
    docker_kill("coordinator-1")
    time.sleep(4)
    docker_start("coordinator-1")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        sleep_ok = 0
        timer_ok = 0
        pending = 0
        for tid, rid in sleeps:
            run = get_run(tid, rid)
            st = str(run.get("status") or "")
            if st == "completed":
                sleep_ok += 1
            else:
                pending += 1
        for tid, rid in timers:
            run = get_run(tid, rid)
            st = str(run.get("status") or "")
            if st == "failed":
                timer_ok += 1
            else:
                pending += 1
        logger.info(
            "sleep_done=%d/%d timer_failed=%d/%d pending=%d",
            sleep_ok,
            len(sleeps),
            timer_ok,
            len(timers),
            pending,
        )
        if sleep_ok == len(sleeps) and timer_ok == len(timers):
            break
        time.sleep(1.5)
    else:
        raise RuntimeError(
            f"did not drain sleeps={[(t, get_run(t, r).get('status')) for t, r in sleeps]} "
            f"timers={[(t, get_run(t, r).get('status')) for t, r in timers]}"
        )

    for tid, rid in sleeps:
        steps = list_steps(tid, rid)
        pause = next((s for s in steps if s.get("name") == "pause"), None)
        if not pause or pause.get("status") != "completed":
            raise RuntimeError(f"sleep {tid}/{rid} pause not completed: {pause}")
        if pause.get("job_id"):
            raise RuntimeError(f"sleep {tid}/{rid} used a worker job")
        ship = next((s for s in steps if s.get("name") == "ship"), None)
        if not ship or ship.get("status") != "completed":
            raise RuntimeError(f"sleep {tid}/{rid} ship missing")

    for tid, rid in timers:
        run = get_run(tid, rid)
        if "timer" not in str(run.get("fail_reason") or ""):
            raise RuntimeError(f"timer {tid}/{rid} bad reason: {run}")
        steps = list_steps(tid, rid)
        watch = next((s for s in steps if s.get("name") == "watch"), None)
        if not watch or watch.get("status") != "failed":
            raise RuntimeError(f"timer {tid}/{rid} watch not failed: {watch}")
        comps = [s for s in steps if s.get("direction") == "compensate"]
        if not any(s.get("status") == "compensated" for s in comps):
            raise RuntimeError(f"timer {tid}/{rid} did not compensate: {steps}")
        ship = next((s for s in steps if s.get("name") == "ship"), None)
        if ship is not None and ship.get("status") == "completed":
            raise RuntimeError(f"timer {tid}/{rid} shipped after timeout")

    logger.info(
        "workflow timer soak OK sleep=%d timer=%d",
        len(sleeps),
        len(timers),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow timer soak failed")
        sys.exit(1)
