#!/usr/bin/env python3
"""Fleet soak: signed inbound triggers across tenants and coordinators.

External-style POSTs enqueue jobs, DAGs, workflows, and signals without
the normal /v1/jobs API. Wrong signatures and cross-tenant bodies must
fail. Workers drain the work; tenants stay isolated.

  COMPOSE_PROJECT_NAME=jq-inbound docker compose -p jq-inbound \\
    -f docker-compose.yml -f docker-compose.inbound-triggers.yml up --build -d
  docker logs -f jq-inbound-inbound-trigger-soak-1
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from uuid import uuid4

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inbound_trigger_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "40") or "40")
DAGS = int(os.environ.get("DAGS_PER_TENANT", "4") or "4")
WFS = int(os.environ.get("WORKFLOWS_PER_TENANT", "3") or "3")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8") or "8")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0
_rr_lock = threading.Lock()


def _next_coord() -> str:
    global _rr
    with _rr_lock:
        base = COORDS[_rr % len(COORDS)]
        _rr += 1
    return base


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    last: Exception | None = None
    start = 0
    with _rr_lock:
        start = _rr % len(COORDS)
    for i in range(len(COORDS)):
        base = COORDS[(start + i) % len(COORDS)]
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
                last = RuntimeError(f"{base}{path} -> {exc.code}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def signed_post(
    trigger_id: str, secret: str, body: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    from job_queue import NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, sign_inbound

    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    nonce = uuid4().hex
    sig = sign_inbound(secret, raw, timestamp=ts, nonce=nonce)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        SIGNATURE_HEADER: sig,
        TIMESTAMP_HEADER: ts,
        NONCE_HEADER: nonce,
    }
    last: Exception | None = None
    for _ in range(len(COORDS)):
        url = f"{_next_coord()}/v1/inbound/{trigger_id}"
        req = urllib.request.Request(url, data=raw, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                got = resp.read()
                return int(resp.status), json.loads(got.decode()) if got else {}
        except urllib.error.HTTPError as exc:
            payload = json.loads((exc.read() or b"{}").decode() or "{}")
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{url} -> {exc.code}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"inbound post failed: {last}")


def wait_ready() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        ok = 0
        for b in COORDS:
            try:
                code, h = http_json("GET", "/health")
                if code == 200 and h.get("ok"):
                    ok += 1
                    break
            except Exception:
                pass
        if ok:
            # ping every coordinator
            ready = 0
            for b in COORDS:
                try:
                    req = urllib.request.Request(b + "/health", method="GET")
                    with urllib.request.urlopen(req, timeout=3) as resp:
                        if json.loads(resp.read().decode()).get("ok"):
                            ready += 1
                except Exception:
                    pass
            if ready == len(COORDS):
                return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def tenant_ids() -> list[str]:
    return [f"inb-{i:02d}" for i in range(TENANTS)]


def wait_steps() -> list[dict[str, Any]]:
    return [
        {"name": "reserve", "kind": "effect", "payload": {"op": "reserve", "ms": 10}},
        {
            "name": "gate",
            "kind": "wait_signal",
            "signal": "go",
            "timeout_seconds": 90,
        },
        {"name": "ship", "kind": "effect", "payload": {"op": "ship", "ms": 10}},
    ]


def main() -> int:
    logger.info(
        "inbound soak tenants=%d jobs=%d dags=%d wfs=%d conc=%d",
        TENANTS,
        JOBS,
        DAGS,
        WFS,
        CONCURRENCY,
    )
    wait_ready()
    tenants = tenant_ids()
    triggers: dict[str, dict[str, Any]] = {}
    for tid in tenants:
        code, out = http_json(
            "POST",
            "/v1/triggers",
            {
                "tenant_id": tid,
                "name": f"ext-{tid}",
                "secret": f"secret-{tid}-xxxxxxxx",
                "kinds": ["job", "dag", "workflow", "signal"],
            },
        )
        if code != 201:
            raise RuntimeError(f"create trigger {tid} -> {code}: {out}")
        triggers[tid] = out["trigger"]
        listed = http_json("GET", f"/v1/triggers?tenant_id={tid}")[1]
        if listed["triggers"][0].get("secret") is not None:
            raise RuntimeError("list leaked trigger secret")

    # Cross-tenant attacks must fail.
    a, b = tenants[0], tenants[1]
    code, err = signed_post(
        triggers[a]["id"],
        triggers[b]["secret"],
        {"action": "job", "payload": {"leak": True}},
    )
    if code != 401:
        raise RuntimeError(f"wrong secret should 401, got {code} {err}")
    code, err = signed_post(
        triggers[a]["id"],
        triggers[a]["secret"],
        {"action": "job", "tenant_id": b, "payload": {"hijack": True}},
    )
    if code != 403:
        raise RuntimeError(f"tenant hijack should 403, got {code} {err}")

    expected_jobs = 0
    count_lock = threading.Lock()
    errors: list[str] = []
    runs: dict[str, list[str]] = {t: [] for t in tenants}

    def fire_job(tid: str, n: int) -> None:
        nonlocal expected_jobs
        code, out = signed_post(
            triggers[tid]["id"],
            triggers[tid]["secret"],
            {
                "action": "job",
                "payload": {"kind": "inb", "tenant": tid, "n": n, "ms": 15},
            },
        )
        if code != 201:
            errors.append(f"job {tid}/{n} -> {code} {out}")
            return
        if (out.get("job") or {}).get("tenant_id") != tid:
            errors.append(f"job tenant leak {tid}: {out}")
            return
        with count_lock:
            expected_jobs += 1

    def fire_dag(tid: str, n: int) -> None:
        nonlocal expected_jobs
        code, out = signed_post(
            triggers[tid]["id"],
            triggers[tid]["secret"],
            {
                "action": "dag",
                "batch_key": f"{tid}-dag-{n}",
                "jobs": [
                    {"ref": "a", "payload": {"kind": "inb", "role": "a", "ms": 10}},
                    {
                        "ref": "b",
                        "payload": {"kind": "inb", "role": "b", "ms": 10},
                        "depends_on": ["a"],
                    },
                ],
            },
        )
        if code != 201:
            errors.append(f"dag {tid}/{n} -> {code} {out}")
            return
        with count_lock:
            expected_jobs += 2

    def fire_wf(tid: str, n: int) -> None:
        code, out = signed_post(
            triggers[tid]["id"],
            triggers[tid]["secret"],
            {
                "action": "workflow",
                "name": f"{tid}-wf-{n}",
                "steps": wait_steps(),
            },
        )
        if code != 201:
            errors.append(f"wf {tid}/{n} -> {code} {out}")
            return
        rid = (out.get("run") or {}).get("id")
        if not rid:
            errors.append(f"wf {tid}/{n} missing run")
            return
        runs[tid].append(str(rid))

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futs = []
        for tid in tenants:
            for n in range(JOBS):
                futs.append(pool.submit(fire_job, tid, n))
            for n in range(DAGS):
                futs.append(pool.submit(fire_dag, tid, n))
            for n in range(WFS):
                futs.append(pool.submit(fire_wf, tid, n))
        for fut in as_completed(futs):
            fut.result()
    if errors:
        raise RuntimeError(f"fire errors ({len(errors)}): {errors[:5]}")
    logger.info("enqueued jobs=%d workflows=%s", expected_jobs, {t: len(v) for t, v in runs.items()})

    # Signal each parked workflow. Drive a bit so reserve completes first.
    time.sleep(1.5)
    for tid in tenants:
        for rid in runs[tid]:
            code, out = signed_post(
                triggers[tid]["id"],
                triggers[tid]["secret"],
                {"action": "signal", "run_id": rid, "signal": "go", "payload": {"ok": True}},
            )
            if code not in (200, 201):
                # Cross-check isolation: signaling another tenant's first run must 404.
                errors.append(f"signal {tid}/{rid} -> {code} {out}")
    if errors:
        raise RuntimeError(f"signal errors: {errors[:5]}")

    other = tenants[1]
    if runs[tenants[0]]:
        code, out = signed_post(
            triggers[other]["id"],
            triggers[other]["secret"],
            {
                "action": "signal",
                "run_id": runs[tenants[0]][0],
                "signal": "go",
            },
        )
        if code != 404:
            raise RuntimeError(f"cross-tenant signal should 404, got {code} {out}")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        code, bl = http_json("GET", "/v1/admin/backlog")
        jobs = (bl.get("jobs") or {}) if code == 200 else {}
        pending = int(jobs.get("pending") or 0)
        leased = int(jobs.get("leased") or 0)
        completed = int(jobs.get("completed") or 0)
        logger.info(
            "progress completed=%d pending=%d leased=%d expected>=%d",
            completed,
            pending,
            leased,
            expected_jobs,
        )
        if pending == 0 and leased == 0 and completed >= expected_jobs:
            break
        time.sleep(1.5)
    else:
        raise RuntimeError("fleet did not drain inbound work")

    for tid in tenants:
        code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=200")
        if code != 200:
            raise RuntimeError(f"list {tid} -> {code}")
        for j in body.get("jobs") or []:
            if j.get("tenant_id") != tid:
                raise RuntimeError(f"tenant leak {tid}: {j}")
        listed = http_json("GET", f"/v1/triggers?tenant_id={tid}")[1]
        if any(t.get("secret") for t in listed.get("triggers") or []):
            raise RuntimeError("secret leaked after load")

    logger.info(
        "inbound trigger soak OK tenants=%d completed>=%d", TENANTS, expected_jobs
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("inbound trigger soak failed")
        sys.exit(1)
