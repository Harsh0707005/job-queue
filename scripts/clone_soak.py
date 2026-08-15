#!/usr/bin/env python3
"""Fleet soak: clone / re-run jobs across a multi-tenant worker fleet.

  * Dual coordinators, four workers, two tenants.
  * Seed jobs complete, then each seed is cloned (copy + payload override).
  * Live pending jobs are also cloned so both source and clone finish.
  * Every clone points at its source; drain must not be required for finish.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("clone_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SEED_EACH = int(os.environ.get("SEED_EACH", "20") or "20")
OVERRIDE_EACH = int(os.environ.get("OVERRIDE_EACH", "10") or "10")
LIVE_EACH = int(os.environ.get("LIVE_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "30") or "30")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
LEFT = "clone-a"
RIGHT = "clone-b"
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
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
                payload = json.loads(raw.decode()) if raw else {}
                return int(resp.status), payload
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
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            code, body = http_json("GET", "/health")
            if code == 200 and body.get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def list_jobs(tid: str, **extra: str) -> list[dict[str, Any]]:
    q = f"/v1/jobs?tenant_id={tid}&limit=500"
    for key, val in extra.items():
        q += f"&{key}={val}"
    code, body = http_json("GET", q)
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(
    tid: str, n: int, *, kind: str, prefix: str, priority: int
) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {
                    "kind": kind,
                    "ms": JOB_MS,
                    "i": i,
                    "p": prefix,
                },
                "priority": priority,
                "labels": [prefix, kind],
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid}/{kind} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def clone(
    tid: str,
    job_id: str,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"tenant_id": tid}
    if payload is not None:
        body["payload"] = payload
    code, resp = http_json("POST", f"/v1/jobs/{job_id}/clone", body)
    if code != 201:
        raise RuntimeError(f"clone {tid}/{job_id} {code} {resp}")
    return dict(resp["job"])


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def wait_completed(tids: list[str], expect: dict[str, int]) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        now = time.time()
        counts = {tid: completed(list_jobs(tid)) for tid in tids}
        if now - last >= 2:
            logger.info("drain %s", counts)
            last = now
        if all(counts[tid] >= expect[tid] for tid in tids):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for drain: {counts}")


def main() -> int:
    logger.info(
        "clone soak seed=%d override=%d live=%d ms=%d",
        SEED_EACH,
        OVERRIDE_EACH,
        LIVE_EACH,
        JOB_MS,
    )
    wait_coords()
    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    seeds: dict[str, list[str]] = {}
    for tid in (LEFT, RIGHT):
        seeds[tid] = enqueue(
            tid, SEED_EACH, kind="render", prefix="seed", priority=3
        )
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    wait_completed(
        [LEFT, RIGHT], {LEFT: SEED_EACH, RIGHT: SEED_EACH}
    )

    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    copies: dict[str, list[str]] = {LEFT: [], RIGHT: []}
    overrides: dict[str, list[str]] = {LEFT: [], RIGHT: []}
    for tid in (LEFT, RIGHT):
        for i, sid in enumerate(seeds[tid]):
            job = clone(tid, sid)
            if job.get("cloned_from_job_id") != sid:
                raise RuntimeError(f"copy pointer {job}")
            if job.get("priority") != 3:
                raise RuntimeError(f"copy priority {job}")
            copies[tid].append(str(job["id"]))
            if i < OVERRIDE_EACH:
                over = clone(
                    tid,
                    sid,
                    payload={
                        "kind": "other",
                        "ms": JOB_MS,
                        "i": i,
                        "p": "over",
                    },
                )
                if over.get("cloned_from_job_id") != sid:
                    raise RuntimeError(f"override pointer {over}")
                if over.get("payload_kind") != "other":
                    raise RuntimeError(f"override kind {over}")
                overrides[tid].append(str(over["id"]))

    live_src: dict[str, list[str]] = {}
    live_clones: dict[str, list[str]] = {LEFT: [], RIGHT: []}
    for tid in (LEFT, RIGHT):
        live_src[tid] = enqueue(
            tid, LIVE_EACH, kind="render", prefix="live", priority=1
        )
        for sid in live_src[tid]:
            job = clone(tid, sid)
            if job.get("cloned_from_job_id") != sid:
                raise RuntimeError(f"live pointer {job}")
            live_clones[tid].append(str(job["id"]))
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expect = {
        tid: SEED_EACH + SEED_EACH + OVERRIDE_EACH + LIVE_EACH + LIVE_EACH
        for tid in (LEFT, RIGHT)
    }
    wait_completed([LEFT, RIGHT], expect)

    for tid in (LEFT, RIGHT):
        jobs = {str(j["id"]): j for j in list_jobs(tid)}
        override_sources = set(seeds[tid][:OVERRIDE_EACH])
        for sid in seeds[tid]:
            kids = list_jobs(tid, cloned_from_job_id=sid)
            want = 2 if sid in override_sources else 1
            if len(kids) != want:
                raise RuntimeError(
                    f"{tid} source {sid} expected {want} clones got {len(kids)}"
                )
            for kid in kids:
                if kid.get("cloned_from_job_id") != sid:
                    raise RuntimeError(f"list pointer {kid}")
                if kid.get("status") != "completed":
                    raise RuntimeError(f"clone unfinished {kid}")
        for cid in copies[tid] + overrides[tid] + live_clones[tid]:
            row = jobs.get(cid)
            if row is None or row.get("status") != "completed":
                raise RuntimeError(f"{tid} clone {cid} not completed: {row}")
            if not row.get("cloned_from_job_id"):
                raise RuntimeError(f"{tid} clone {cid} missing source")
        for sid in live_src[tid]:
            row = jobs.get(sid)
            if row is None or row.get("status") != "completed":
                raise RuntimeError(f"{tid} live source {sid} not completed")
            if row.get("cloned_from_job_id") is not None:
                raise RuntimeError(f"{tid} live source {sid} should be original")

        # Drain still blocks clone after the fleet finishes leftovers.
        code, body = http_json(
            "POST", f"/v1/tenants/{tid}/drain-queue", {"queue_name": "default"}
        )
        if code != 200:
            raise RuntimeError(f"drain {tid} {code} {body}")
        code, body = http_json(
            "POST",
            f"/v1/jobs/{seeds[tid][0]}/clone",
            {"tenant_id": tid},
        )
        if code != 409 or body.get("code") != "queue_draining":
            raise RuntimeError(f"expected drain on clone {code} {body}")
        code, body = http_json(
            "POST", f"/v1/tenants/{tid}/undrain-queue", {"queue_name": "default"}
        )
        if code != 200:
            raise RuntimeError(f"undrain {tid} {code} {body}")

    logger.info(
        "clone soak OK left=%d right=%d",
        expect[LEFT],
        expect[RIGHT],
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("clone soak failed")
        sys.exit(1)
