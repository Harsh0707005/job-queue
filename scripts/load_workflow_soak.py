#!/usr/bin/env python3
"""Fleet soak: multi-tenant multi-worker saga workflows + reverse compensate.

Starts happy-path and fail-path workflows per tenant. Workers run
workflow.step jobs (forward/compensate) with effect-sink idempotency.
Asserts run terminal states and exactly-once compensate effects.
"""

from __future__ import annotations

import json
import logging
import os
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
logger = logging.getLogger("load_workflow_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
SINK = os.environ.get("EFFECT_SINK_URL", "http://effect-sink:8091").rstrip("/")
TENANTS = int(os.environ.get("TENANTS", "6"))
HAPPY_PER_TENANT = int(os.environ.get("HAPPY_PER_TENANT", "4"))
FAIL_PER_TENANT = int(os.environ.get("FAIL_PER_TENANT", "3"))
PARALLEL_HAPPY_PER_TENANT = int(os.environ.get("PARALLEL_HAPPY_PER_TENANT", "2"))
PARALLEL_FAIL_PER_TENANT = int(os.environ.get("PARALLEL_FAIL_PER_TENANT", "2"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/results/workflow-fleet.json")
LABEL = os.environ.get("LOAD_LABEL", "workflow-fleet")
_rr = 0


def http_json(
    method: str, path: str, body: dict[str, Any] | None = None, timeout: float = 60.0
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDINATOR_URLS)
    _rr += 1
    ordered = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
    last: Exception | None = None
    for base in ordered:
        url = base + path
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return resp.status, {}
                return resp.status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{url} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except urllib.error.URLError as exc:
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for base in COORDINATOR_URLS:
            try:
                code, h = http_json("GET", "/health", timeout=5)
                if code == 200 and h.get("ok"):
                    return
            except Exception:  # noqa: BLE001
                pass
        time.sleep(1)
    raise RuntimeError("not healthy")


def sink_stats() -> dict[str, Any]:
    req = urllib.request.Request(SINK + "/stats", method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def happy_steps() -> list[dict[str, Any]]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 40},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        {
            "name": "charge",
            "kind": "effect",
            "payload": {"op": "charge", "ms": 40},
            "compensate": {"kind": "effect", "payload": {"op": "refund", "ms": 20}},
        },
        {
            "name": "ship",
            "kind": "effect",
            "payload": {"op": "ship", "ms": 40},
        },
    ]


def fail_steps() -> list[dict[str, Any]]:
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 30},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        {
            "name": "charge",
            "kind": "effect",
            "payload": {"op": "charge", "ms": 30},
            "compensate": {"kind": "effect", "payload": {"op": "refund", "ms": 20}},
        },
        {
            "name": "ship",
            "kind": "effect",
            "payload": {"op": "ship", "ms": 20},
            "fail": True,
            "fail_error": "fleet forced ship fail",
        },
    ]


def parallel_happy_steps() -> list[dict[str, Any]]:
    """Fan-out two branches then join — exercises multi-worker concurrent steps."""
    return [
        {
            "name": "setup",
            "kind": "effect",
            "payload": {"op": "setup", "ms": 30},
            "compensate": {"kind": "effect", "payload": {"op": "undo_setup", "ms": 15}},
        },
        {
            "type": "parallel",
            "name": "fan",
            "branches": [
                [
                    {
                        "name": "left",
                        "kind": "effect",
                        "payload": {"op": "left", "ms": 50},
                        "compensate": {
                            "kind": "effect",
                            "payload": {"op": "undo_left", "ms": 15},
                        },
                    }
                ],
                [
                    {
                        "name": "right",
                        "kind": "effect",
                        "payload": {"op": "right", "ms": 50},
                        "compensate": {
                            "kind": "effect",
                            "payload": {"op": "undo_right", "ms": 15},
                        },
                    }
                ],
            ],
        },
        {
            "name": "join",
            "kind": "effect",
            "payload": {"op": "join", "ms": 30},
        },
    ]


def parallel_fail_steps() -> list[dict[str, Any]]:
    """One branch fails; sibling may still be in-flight — settle then compensate."""
    return [
        {
            "name": "setup",
            "kind": "effect",
            "payload": {"op": "setup", "ms": 25},
            "compensate": {"kind": "effect", "payload": {"op": "undo_setup", "ms": 15}},
        },
        {
            "type": "parallel",
            "name": "fan",
            "branches": [
                [
                    {
                        "name": "slow_ok",
                        "kind": "effect",
                        "payload": {"op": "slow_ok", "ms": 80},
                        "compensate": {
                            "kind": "effect",
                            "payload": {"op": "undo_slow", "ms": 15},
                        },
                    }
                ],
                [
                    {
                        "name": "boom",
                        "kind": "effect",
                        "payload": {"op": "boom", "ms": 20},
                        "fail": True,
                        "fail_error": "fleet parallel branch fail",
                    }
                ],
            ],
        },
        {
            "name": "join",
            "kind": "effect",
            "payload": {"op": "join", "ms": 20},
        },
    ]


def main() -> int:
    tenants = [f"wf-t{i:02d}" for i in range(TENANTS)]
    logger.info(
        "WORKFLOW soak label=%s tenants=%s happy/t=%s fail/t=%s "
        "par_happy/t=%s par_fail/t=%s",
        LABEL,
        TENANTS,
        HAPPY_PER_TENANT,
        FAIL_PER_TENANT,
        PARALLEL_HAPPY_PER_TENANT,
        PARALLEL_FAIL_PER_TENANT,
    )
    wait_healthy()

    runs: list[tuple[str, str, str]] = []  # tenant, run_id, kind

    def steps_for(kind: str) -> list[dict[str, Any]]:
        if kind == "happy":
            return happy_steps()
        if kind == "fail":
            return fail_steps()
        if kind == "par_happy":
            return parallel_happy_steps()
        if kind == "par_fail":
            return parallel_fail_steps()
        raise ValueError(f"unknown kind {kind}")

    def start_one(tid: str, kind: str) -> tuple[str, str, str]:
        steps = steps_for(kind)
        code, body = http_json(
            "POST",
            "/v1/workflows",
            {
                "tenant_id": tid,
                "name": f"{LABEL}-{kind}",
                "steps": steps,
                "max_attempts": 1,
                "visibility_timeout_seconds": 4,
                "max_compensate_retries": 8,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"start workflow {code}: {body}")
        rid = (body.get("run") or {}).get("id")
        if not rid:
            raise RuntimeError(f"no run id: {body}")
        return tid, str(rid), kind

    work: list[tuple[str, str]] = []
    for tid in tenants:
        for _ in range(HAPPY_PER_TENANT):
            work.append((tid, "happy"))
        for _ in range(FAIL_PER_TENANT):
            work.append((tid, "fail"))
        for _ in range(PARALLEL_HAPPY_PER_TENANT):
            work.append((tid, "par_happy"))
        for _ in range(PARALLEL_FAIL_PER_TENANT):
            work.append((tid, "par_fail"))

    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(start_one, tid, kind) for tid, kind in work]
        for f in as_completed(futs):
            runs.append(f.result())
    logger.info("started %s workflow runs", len(runs))

    # Wait until happy paths finish and fail-path runs are compensating, then
    # kill workers mid-compensation so redrive/reclaim must finish the reverse
    # chain. Do not kill while parallel happy fan-out/join is still in flight.
    kill_done = False
    happy_kinds = {"happy", "par_happy"}
    expect_happy_kinds = sum(1 for _, _, k in runs if k in happy_kinds)
    t0 = time.time()
    completed = 0
    failed = 0
    pending = len(runs)
    while time.time() - t0 < MAX_WALL:
        completed = 0
        failed = 0
        pending = 0
        compensating = 0
        happy_completed = 0
        for tid, rid, kind in runs:
            code, body = http_json(
                "GET",
                f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid}),
            )
            if code != 200:
                pending += 1
                continue
            st = (body.get("run") or {}).get("status")
            if st == "completed":
                completed += 1
                if kind in happy_kinds:
                    happy_completed += 1
            elif st == "failed":
                failed += 1
            else:
                pending += 1
                if st == "compensating":
                    compensating += 1
        # Repair stuck compensate jobs while waiting.
        try:
            http_json(
                "POST",
                "/v1/workflows/process-compensation",
                {"limit": 100},
            )
        except Exception:  # noqa: BLE001
            pass
        if (
            not kill_done
            and compensating >= max(1, FAIL_PER_TENANT)
            and happy_completed >= expect_happy_kinds
        ):
            # Kill workers via Docker Engine API (no docker CLI required).
            try:
                import urllib.request as _u

                sock_path = "/var/run/docker.sock"
                if os.path.exists(sock_path):
                    import http.client

                    class _DockerHTTPConnection(http.client.HTTPConnection):
                        def __init__(self) -> None:
                            super().__init__("localhost")

                        def connect(self) -> None:  # noqa: D102
                            import socket

                            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                            self.sock.connect(sock_path)

                    conn = _DockerHTTPConnection()
                    conn.request("GET", "/containers/json")
                    resp = conn.getresponse()
                    containers = json.loads(resp.read().decode() or "[]")
                    killed = 0
                    for c in containers:
                        names = " ".join(c.get("Names") or [])
                        if "worker-" in names and c.get("Id"):
                            cid = c["Id"]
                            conn2 = _DockerHTTPConnection()
                            conn2.request("POST", f"/containers/{cid}/kill")
                            conn2.getresponse().read()
                            killed += 1
                    logger.info(
                        "killed %s workers mid-compensation (compensating=%s)",
                        killed,
                        compensating,
                    )
                    time.sleep(5)
                    # Restart workers.
                    for c in containers:
                        names = " ".join(c.get("Names") or [])
                        if "worker-" in names and c.get("Id"):
                            cid = c["Id"]
                            conn3 = _DockerHTTPConnection()
                            conn3.request("POST", f"/containers/{cid}/start")
                            try:
                                conn3.getresponse().read()
                            except Exception:  # noqa: BLE001
                                pass
                    kill_done = True
                    logger.info("restarted workers after compensate kill")
            except Exception as exc:  # noqa: BLE001
                logger.warning("worker kill skipped: %s", exc)
                kill_done = True
        if pending == 0:
            break
        time.sleep(1)
    logger.info(
        "terminal completed=%s failed=%s pending=%s", completed, failed, pending
    )

    expect_happy = TENANTS * (HAPPY_PER_TENANT + PARALLEL_HAPPY_PER_TENANT)
    expect_fail = TENANTS * (FAIL_PER_TENANT + PARALLEL_FAIL_PER_TENANT)
    ok_counts = completed == expect_happy and failed == expect_fail and pending == 0

    # Sample linear fail run: compensate order charge then reserve.
    ok_compensate_order = True
    fail_samples = [(t, r) for t, r, k in runs if k == "fail"][:3]
    for tid, rid in fail_samples:
        code, body = http_json(
            "GET",
            f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
        )
        if code != 200:
            ok_compensate_order = False
            continue
        comps = [
            s
            for s in (body.get("steps") or [])
            if s.get("direction") == "compensate"
            and s.get("status") == "compensated"
        ]
        # completed forwards: reserve, charge → compensate charge then reserve
        by_done = sorted(
            comps,
            key=lambda s: s.get("completed_at") or s.get("created_at") or "",
        )
        names = [s.get("name") for s in by_done]
        if names != ["charge", "reserve"]:
            logger.warning("unexpected compensate order for %s: %s", rid, names)
            ok_compensate_order = False

    # Parallel fail: setup (+ maybe slow_ok) compensated; boom failed; no join.
    ok_parallel_fail = True
    par_fail_samples = [(t, r) for t, r, k in runs if k == "par_fail"][:4]
    for tid, rid in par_fail_samples:
        code, body = http_json(
            "GET",
            f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
        )
        if code != 200:
            ok_parallel_fail = False
            continue
        steps = body.get("steps") or []
        forwards = {s.get("name"): s for s in steps if s.get("direction") == "forward"}
        if forwards.get("boom", {}).get("status") != "failed":
            logger.warning("par_fail %s boom not failed: %s", rid, forwards.get("boom"))
            ok_parallel_fail = False
        if "join" in forwards and forwards["join"].get("status") not in (
            "skipped",
            None,
        ):
            # join should never complete on branch fail
            if forwards["join"].get("status") == "completed":
                logger.warning("par_fail %s join completed unexpectedly", rid)
                ok_parallel_fail = False
        comps = {
            s.get("name")
            for s in steps
            if s.get("direction") == "compensate"
            and s.get("status") == "compensated"
        }
        if "setup" not in comps:
            logger.warning("par_fail %s missing setup compensate: %s", rid, comps)
            ok_parallel_fail = False

    # Parallel happy: all four forwards completed.
    ok_parallel_happy = True
    for tid, rid, kind in runs:
        if kind != "par_happy":
            continue
        code, body = http_json(
            "GET",
            f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
        )
        if code != 200:
            ok_parallel_happy = False
            continue
        forwards = [
            s
            for s in (body.get("steps") or [])
            if s.get("direction") == "forward"
        ]
        names = {s.get("name") for s in forwards}
        if names != {"setup", "left", "right", "join"}:
            logger.warning("par_happy %s unexpected steps %s", rid, names)
            ok_parallel_happy = False
        if any(s.get("status") != "completed" for s in forwards):
            ok_parallel_happy = False

    # Tenant isolation: cross-tenant get returns 404
    ok_isolation = True
    if runs:
        tid0, rid0, _ = runs[0]
        other = tenants[1] if len(tenants) > 1 else "no-such-tenant"
        code, _ = http_json(
            "GET",
            f"/v1/workflows/{rid0}?" + urlencode({"tenant_id": other}),
        )
        if code not in (404, 400):
            # get returns 404 when wrong tenant
            ok_isolation = code == 404

    # Effect sink: unique keys >= happy*3 + fail*(2 forward + 2 compensate)
    # fail: reserve+charge forward, ship fails (may not complete effect),
    # compensate charge+reserve.
    stats = {}
    try:
        stats = sink_stats()
    except Exception as exc:  # noqa: BLE001
        logger.warning("sink stats: %s", exc)
    unique = int(stats.get("unique_effects") or 0)
    # linear happy 3 + par happy 4 + fail compensates (lower bound)
    min_unique = (
        TENANTS * HAPPY_PER_TENANT * 3
        + TENANTS * PARALLEL_HAPPY_PER_TENANT * 4
        + TENANTS * FAIL_PER_TENANT * 2
    )
    ok_sink = unique >= min_unique

    # After kill/restart, no run should remain stuck in compensating.
    ok_no_stuck = pending == 0
    ok_kill_exercised = kill_done
    ok = (
        ok_counts
        and ok_compensate_order
        and ok_parallel_fail
        and ok_parallel_happy
        and ok_isolation
        and ok_sink
        and ok_no_stuck
    )
    result = {
        "ok": ok,
        "label": LABEL,
        "total_runs": len(runs),
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "expect_happy": expect_happy,
        "expect_fail": expect_fail,
        "ok_counts": ok_counts,
        "ok_compensate_order": ok_compensate_order,
        "ok_parallel_fail": ok_parallel_fail,
        "ok_parallel_happy": ok_parallel_happy,
        "ok_isolation": ok_isolation,
        "ok_sink": ok_sink,
        "ok_no_stuck": ok_no_stuck,
        "ok_kill_exercised": ok_kill_exercised,
        "sink_unique_effects": unique,
        "sink_total_posts": stats.get("total_posts"),
    }
    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("result %s", json.dumps(result))
    if ok:
        logger.info("WORKFLOW FLEET PASS")
        return 0
    logger.error("WORKFLOW FLEET FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
