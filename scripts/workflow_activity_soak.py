#!/usr/bin/env python3
"""Fleet soak: workflow external activities under worker death.

Happy activities complete. Start-to-close timeout and a killed worker
(heartbeat stop) fail the step and reverse-compensate. Other tenants
keep running.

  COMPOSE_PROJECT_NAME=jq-wfact docker compose -p jq-wfact \\
    -f docker-compose.yml -f docker-compose.workflow.yml \\
    -f docker-compose.workflow-activity.yml up --build -d
  docker logs -f jq-wfact-workflow-soak-1
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
logger = logging.getLogger("workflow_activity_soak")

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
TIMEOUT_PER = int(os.environ.get("TIMEOUT_PER_TENANT", "2") or "2")
LONG_PER = int(os.environ.get("LONG_PER_TENANT", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-wfact")
ACTIVITY_URL = os.environ.get("ACTIVITY_URL", "http://activity-runner:8092")
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
        try:
            req = urllib.request.Request(ACTIVITY_URL + "/health", method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                runner_ok = json.loads(resp.read().decode()).get("ok")
        except Exception:
            runner_ok = False
        if ready >= 1 and runner_ok:
            return
        time.sleep(1)
    raise RuntimeError("coordinators/runner not ready")


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
            "visibility_timeout_seconds": 8,
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


def activity_steps(
    *,
    work_ms: int,
    timeout: float,
    heartbeat: float,
    name: str = "charge",
) -> list[dict[str, Any]]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 30},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        {
            "name": name,
            "kind": "activity",
            "activity": name,
            "url": ACTIVITY_URL,
            "timeout_seconds": timeout,
            "heartbeat_timeout_seconds": heartbeat,
            "payload": {"op": name, "work_ms": work_ms},
            "compensate": {
                "kind": "effect",
                "payload": {"op": f"undo_{name}", "ms": 20},
            },
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def tenant_ids() -> list[str]:
    return [f"wfact-{i:02d}" for i in range(TENANTS)]


def wait_terminal(
    runs: list[tuple[str, str]], *, timeout: float, label: str
) -> dict[str, int]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = {"completed": 0, "failed": 0, "cancelled": 0, "other": 0}
        for tid, rid in runs:
            st = str(get_run(tid, rid).get("status") or "")
            if st in counts:
                counts[st] += 1
            else:
                counts["other"] += 1
        logger.info("%s %s", label, counts)
        if counts["other"] == 0:
            return counts
        time.sleep(1.5)
    raise RuntimeError(f"{label} did not drain: leftover runs")


def assert_failed_compensated(tid: str, rid: str) -> None:
    run = get_run(tid, rid)
    if run.get("status") != "failed":
        raise RuntimeError(f"{tid}/{rid} expected failed got {run}")
    reason = str(run.get("fail_reason") or "")
    if "activity" not in reason and "timeout" not in reason and "heartbeat" not in reason:
        # reserve fail would be unexpected
        logger.warning("fail reason %s/%s: %s", tid, rid, reason)
    steps = list_steps(tid, rid)
    comps = [s for s in steps if s.get("direction") == "compensate"]
    if not any(s.get("status") == "compensated" for s in comps):
        raise RuntimeError(f"{tid}/{rid} did not compensate: {steps}")
    ship = next((s for s in steps if s.get("name") == "ship"), None)
    if ship is not None and ship.get("status") == "completed":
        raise RuntimeError(f"{tid}/{rid} shipped after activity fail")


def main() -> int:
    logger.info(
        "workflow activity soak tenants=%d happy=%d timeout=%d long=%d",
        TENANTS,
        HAPPY_PER,
        TIMEOUT_PER,
        LONG_PER,
    )
    wait_healthy()
    tenants = tenant_ids()
    happy: list[tuple[str, str]] = []
    timeouts: list[tuple[str, str]] = []
    longs: list[tuple[str, str]] = []
    errors: list[str] = []

    def launch(tid: str, kind: str, n: int) -> tuple[str, str, str]:
        if kind == "happy":
            steps = activity_steps(work_ms=250, timeout=20, heartbeat=2)
        elif kind == "timeout":
            steps = activity_steps(work_ms=8000, timeout=1, heartbeat=5)
        else:
            steps = activity_steps(work_ms=6000, timeout=20, heartbeat=1)
        rid = start_wf(tid, f"{kind}-{n}", steps)
        return tid, kind, rid

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for tid in tenants:
            for n in range(HAPPY_PER):
                futs.append(pool.submit(launch, tid, "happy", n))
            for n in range(TIMEOUT_PER):
                futs.append(pool.submit(launch, tid, "timeout", n))
        for fut in as_completed(futs):
            try:
                tid, kind, rid = fut.result()
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                continue
            if kind == "happy":
                happy.append((tid, rid))
            else:
                timeouts.append((tid, rid))
    if errors:
        raise RuntimeError(f"start errors: {errors[:5]}")

    h = wait_terminal(happy, timeout=MAX_WALL, label="happy")
    if h["completed"] != len(happy):
        raise RuntimeError(f"happy not all completed: {h}")
    t = wait_terminal(timeouts, timeout=MAX_WALL, label="timeout")
    if t["failed"] != len(timeouts):
        raise RuntimeError(f"timeout not all failed: {t}")
    for tid, rid in timeouts:
        assert_failed_compensated(tid, rid)
    logger.info("stage 1-3 ok happy=%d timeout=%d", len(happy), len(timeouts))

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [
            pool.submit(launch, tid, "long", n)
            for tid in tenants
            for n in range(LONG_PER)
        ]
        for fut in as_completed(futs):
            tid, _kind, rid = fut.result()
            longs.append((tid, rid))
    time.sleep(0.8)
    logger.info("killing worker-1 mid-activity")
    docker_kill("worker-1")
    time.sleep(3)
    docker_start("worker-1")

    l = wait_terminal(longs, timeout=MAX_WALL, label="long")
    if l["other"] != 0:
        raise RuntimeError(f"long runs stuck: {l}")
    for tid, rid in longs:
        st = str(get_run(tid, rid).get("status") or "")
        if st == "failed":
            assert_failed_compensated(tid, rid)
        elif st != "completed":
            raise RuntimeError(f"long {tid}/{rid} status {st}")
    logger.info(
        "workflow activity soak OK happy=%d timeout=%d long=%s",
        len(happy),
        len(timeouts),
        l,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow activity soak failed")
        sys.exit(1)
