#!/usr/bin/env python3
"""Fleet soak: exclusive if/else workflow branches.

Each run decides vip vs std. Unused branch steps must be skipped so the
join can finish. Mix of happy, else, unmatched-if, and a failing then
path. Mid-soak we kill a worker.

  COMPOSE_PROJECT_NAME=jq-wfbr docker compose -p jq-wfbr \\
    -f docker-compose.yml -f docker-compose.workflow.yml \\
    -f docker-compose.workflow-branch.yml up --build -d
  docker logs -f jq-wfbr-workflow-branch-soak-1
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
logger = logging.getLogger("workflow_branch_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
VIP_PER = int(os.environ.get("VIP_PER_TENANT", "3") or "3")
STD_PER = int(os.environ.get("STD_PER_TENANT", "3") or "3")
NONE_PER = int(os.environ.get("NONE_PER_TENANT", "2") or "2")
FAIL_PER = int(os.environ.get("FAIL_PER_TENANT", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-wfbr")
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


def start_wf(tenant: str, name: str, steps: list[dict[str, Any]]) -> str:
    code, body = http_json(
        "POST",
        "/v1/workflows",
        {
            "tenant_id": tenant,
            "name": name,
            "max_attempts": 4,
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


def branch_steps(*, choice: str, fail_then: bool = False, with_else: bool = True):
    then = [
        {
            "name": "vip-work",
            "kind": "effect",
            "payload": {
                "op": "vip",
                "ms": 40,
                **({"force_fail": True} if fail_then else {}),
            },
            "compensate": {"kind": "effect", "payload": {"op": "undo-vip", "ms": 20}},
        }
    ]
    if fail_then:
        then[0]["fail"] = True
    block: dict[str, Any] = {
        "type": "if",
        "name": "route",
        "on": "branch",
        "equals": "vip",
        "then": then,
    }
    if with_else:
        block["else"] = [
            {
                "name": "std-work",
                "kind": "effect",
                "payload": {"op": "std", "ms": 40},
                "compensate": {
                    "kind": "effect",
                    "payload": {"op": "undo-std", "ms": 20},
                },
            }
        ]
    return [
        {
            "name": "decide",
            "kind": "effect",
            "payload": {"op": "decide", "ms": 20, "result": {"branch": choice}},
        },
        block,
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 20}},
    ]


def tenant_ids() -> list[str]:
    return [f"wfbr-{i:02d}" for i in range(TENANTS)]


def wait_terminal(runs: list[tuple[str, str, str]], *, timeout: float) -> dict[str, int]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = {"completed": 0, "failed": 0, "cancelled": 0, "other": 0}
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


def assert_skipped_unused(tid: str, rid: str, *, taken: str, unused: str) -> None:
    steps = list_steps(tid, rid)
    fwd = [s for s in steps if s.get("direction") == "forward"]
    by = {s.get("name"): s for s in fwd}
    if unused in by and by[unused].get("status") != "skipped":
        raise RuntimeError(
            f"{tid}/{rid} unused {unused} status={by[unused].get('status')}"
        )
    if taken in by and by[taken].get("status") not in ("completed", "failed", "skipped"):
        raise RuntimeError(f"{tid}/{rid} taken {taken}={by[taken].get('status')}")
    pending = [s.get("name") for s in fwd if s.get("status") == "pending"]
    if pending:
        raise RuntimeError(f"{tid}/{rid} leftover pending {pending}")


def main() -> int:
    logger.info(
        "workflow branch soak tenants=%d vip=%d std=%d none=%d fail=%d",
        TENANTS,
        VIP_PER,
        STD_PER,
        NONE_PER,
        FAIL_PER,
    )
    wait_healthy()
    tenants = tenant_ids()
    runs: list[tuple[str, str, str]] = []
    errors: list[str] = []

    def launch(tid: str, kind: str, n: int) -> tuple[str, str, str]:
        if kind == "vip":
            steps = branch_steps(choice="vip")
        elif kind == "std":
            steps = branch_steps(choice="std")
        elif kind == "none":
            steps = branch_steps(choice="nope", with_else=False)
        else:
            steps = branch_steps(choice="vip", fail_then=True)
        rid = start_wf(tid, f"{kind}-{n}", steps)
        return tid, rid, kind

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for tid in tenants:
            for n in range(VIP_PER):
                futs.append(pool.submit(launch, tid, "vip", n))
            for n in range(STD_PER):
                futs.append(pool.submit(launch, tid, "std", n))
            for n in range(NONE_PER):
                futs.append(pool.submit(launch, tid, "none", n))
            for n in range(FAIL_PER):
                futs.append(pool.submit(launch, tid, "fail", n))
        for fut in as_completed(futs):
            try:
                runs.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
    if errors:
        raise RuntimeError(f"start errors: {errors[:5]}")
    logger.info("started %d runs", len(runs))

    time.sleep(0.6)
    logger.info("killing worker-1")
    docker_kill("worker-1")
    time.sleep(2)
    docker_start("worker-1")

    counts = wait_terminal(runs, timeout=MAX_WALL)
    want_ok = TENANTS * (VIP_PER + STD_PER + NONE_PER)
    want_fail = TENANTS * FAIL_PER
    if counts["completed"] != want_ok:
        raise RuntimeError(f"completed {counts['completed']} want {want_ok}")
    if counts["failed"] != want_fail:
        raise RuntimeError(f"failed {counts['failed']} want {want_fail}")

    for tid, rid, kind in runs:
        if kind == "vip":
            assert_skipped_unused(tid, rid, taken="vip-work", unused="std-work")
        elif kind == "std":
            assert_skipped_unused(tid, rid, taken="std-work", unused="vip-work")
        elif kind == "none":
            assert_skipped_unused(tid, rid, taken="ship", unused="vip-work")
        else:
            assert_skipped_unused(tid, rid, taken="vip-work", unused="std-work")

    logger.info("workflow branch soak OK %s", counts)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow branch soak failed")
        sys.exit(1)
