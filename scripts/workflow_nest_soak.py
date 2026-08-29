#!/usr/bin/env python3
"""Fleet soak: nested parallel / if / map under worker kill.

  COMPOSE_PROJECT_NAME=jq-wfnest docker compose -p jq-wfnest \\
    -f docker-compose.yml -f docker-compose.workflow.yml \\
    -f docker-compose.workflow-nest.yml up --build -d
  docker logs -f jq-wfnest-workflow-nest-soak-1
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
logger = logging.getLogger("workflow_nest_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
PIF_PER = int(os.environ.get("PIF_PER_TENANT", "3") or "3")
IFP_PER = int(os.environ.get("IFP_PER_TENANT", "3") or "3")
MAP_PER = int(os.environ.get("MAP_PER_TENANT", "2") or "2")
FAIL_PER = int(os.environ.get("FAIL_PER_TENANT", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "jq-wfnest")
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


def pif_steps(*, choice: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "decide",
            "kind": "effect",
            "payload": {"op": "decide", "ms": 20, "result": {"branch": choice}},
        },
        {
            "type": "parallel",
            "name": "fan",
            "branches": [
                [
                    {
                        "type": "if",
                        "on": "branch",
                        "equals": "vip",
                        "then": [
                            {
                                "name": "a-vip",
                                "kind": "effect",
                                "payload": {"op": "a-vip", "ms": 30},
                            }
                        ],
                        "else": [
                            {
                                "name": "a-std",
                                "kind": "effect",
                                "payload": {"op": "a-std", "ms": 30},
                            }
                        ],
                    }
                ],
                [
                    {
                        "name": "plain",
                        "kind": "effect",
                        "payload": {"op": "plain", "ms": 20},
                    }
                ],
            ],
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 15}},
    ]


def ifp_steps(*, choice: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "decide",
            "kind": "effect",
            "payload": {
                "op": "decide",
                "ms": 20,
                "result": {"branch": choice, "ids": ["i", "j"]},
            },
        },
        {
            "type": "if",
            "on": "branch",
            "equals": "vip",
            "then": [
                {
                    "type": "parallel",
                    "name": "fan",
                    "branches": [
                        [
                            {
                                "name": "t1",
                                "kind": "effect",
                                "payload": {"op": "t1", "ms": 25},
                            }
                        ],
                        [
                            {
                                "name": "t2",
                                "kind": "effect",
                                "payload": {"op": "t2", "ms": 25},
                            }
                        ],
                    ],
                }
            ],
            "else": [
                {
                    "type": "map",
                    "name": "each",
                    "items": "ids",
                    "step": {
                        "type": "parallel",
                        "branches": [
                            [
                                {
                                    "name": "left",
                                    "kind": "effect",
                                    "payload": {"op": "L", "ms": 20},
                                }
                            ],
                            [
                                {
                                    "name": "right",
                                    "kind": "effect",
                                    "payload": {"op": "R", "ms": 20},
                                }
                            ],
                        ],
                    },
                }
            ],
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 15}},
    ]


def fail_steps() -> list[dict[str, Any]]:
    return [
        {
            "name": "decide",
            "kind": "effect",
            "payload": {"op": "decide", "ms": 20, "result": {"branch": "vip"}},
            "compensate": {"kind": "effect", "payload": {"op": "undo-d", "ms": 15}},
        },
        {
            "type": "if",
            "on": "branch",
            "equals": "vip",
            "then": [
                {
                    "name": "boom",
                    "kind": "effect",
                    "payload": {"op": "boom", "ms": 20, "force_fail": True},
                    "fail": True,
                    "compensate": {
                        "kind": "effect",
                        "payload": {"op": "undo-boom", "ms": 15},
                    },
                }
            ],
            "else": [
                {
                    "name": "else-work",
                    "kind": "effect",
                    "payload": {"op": "e", "ms": 20},
                    "compensate": {
                        "kind": "effect",
                        "payload": {"op": "undo-e", "ms": 15},
                    },
                }
            ],
        },
    ]


def tenant_ids() -> list[str]:
    return [f"wfnest-{i:02d}" for i in range(TENANTS)]


def wait_terminal(runs: list[tuple[str, str, str]], *, timeout: float) -> dict[str, int]:
    deadline = time.time() + timeout
    counts: dict[str, int] = {}
    while time.time() < deadline:
        counts = {"completed": 0, "failed": 0, "cancelled": 0, "other": 0}
        for tid, rid, _k in runs:
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


def assert_clean(tid: str, rid: str, kind: str) -> None:
    steps = list_steps(tid, rid)
    pending = [
        s.get("name")
        for s in steps
        if s.get("direction") == "forward"
        and s.get("status") in ("pending", "running")
    ]
    if pending:
        raise RuntimeError(f"{tid}/{rid} leftover pending {pending}")
    by = {
        s.get("name"): s
        for s in steps
        if s.get("direction") == "forward"
    }
    if kind == "pif-vip":
        if by.get("a-vip", {}).get("status") != "completed":
            raise RuntimeError(f"{tid}/{rid} a-vip not completed")
        if by.get("a-std", {}).get("status") != "skipped":
            raise RuntimeError(f"{tid}/{rid} a-std not skipped")
    elif kind == "pif-std":
        if by.get("a-std", {}).get("status") != "completed":
            raise RuntimeError(f"{tid}/{rid} a-std not completed")
        if by.get("a-vip", {}).get("status") != "skipped":
            raise RuntimeError(f"{tid}/{rid} a-vip not skipped")
    elif kind == "ifp-vip":
        if by.get("e1") and by["e1"].get("status") not in (None, "skipped"):
            pass
        if by.get("t1", {}).get("status") != "completed":
            raise RuntimeError(f"{tid}/{rid} t1 not completed")
        if any(n.startswith("left-") for n in by):
            raise RuntimeError(f"{tid}/{rid} else-map ran on vip")
    elif kind == "ifp-std":
        if by.get("t1", {}).get("status") not in (None, "skipped"):
            raise RuntimeError(f"{tid}/{rid} t1 should be skipped")
        if "left-0" not in by:
            raise RuntimeError(f"{tid}/{rid} map parallel missing")
    elif kind == "fail":
        if by.get("else-work", {}).get("status") != "skipped":
            raise RuntimeError(f"{tid}/{rid} else-work not skipped")
        comps = [
            s
            for s in steps
            if s.get("direction") == "compensate" and s.get("name") == "else-work"
        ]
        if any(s.get("status") not in ("skipped",) for s in comps):
            raise RuntimeError(f"{tid}/{rid} compensated unused else")


def main() -> int:
    logger.info(
        "workflow nest soak tenants=%d pif=%d ifp=%d fail=%d",
        TENANTS,
        PIF_PER,
        IFP_PER,
        FAIL_PER,
    )
    wait_healthy()
    tenants = tenant_ids()
    runs: list[tuple[str, str, str]] = []
    errors: list[str] = []

    def launch(tid: str, kind: str, n: int) -> tuple[str, str, str]:
        if kind == "pif-vip":
            steps = pif_steps(choice="vip")
        elif kind == "pif-std":
            steps = pif_steps(choice="std")
        elif kind == "ifp-vip":
            steps = ifp_steps(choice="vip")
        elif kind == "ifp-std":
            steps = ifp_steps(choice="std")
        else:
            steps = fail_steps()
        rid = start_wf(tid, f"{kind}-{n}", steps)
        return tid, rid, kind

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for tid in tenants:
            for n in range(PIF_PER):
                futs.append(pool.submit(launch, tid, "pif-vip" if n % 2 == 0 else "pif-std", n))
            for n in range(IFP_PER):
                futs.append(pool.submit(launch, tid, "ifp-vip" if n % 2 == 0 else "ifp-std", n))
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
    want_fail = TENANTS * FAIL_PER
    want_ok = len(runs) - want_fail
    if counts["completed"] != want_ok:
        raise RuntimeError(f"completed {counts['completed']} want {want_ok}")
    if counts["failed"] != want_fail:
        raise RuntimeError(f"failed {counts['failed']} want {want_fail}")

    for tid, rid, kind in runs:
        assert_clean(tid, rid, kind)

    logger.info("workflow nest soak OK %s", counts)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow nest soak failed")
        sys.exit(1)
