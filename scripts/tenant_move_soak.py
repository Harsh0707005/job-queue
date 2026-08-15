#!/usr/bin/env python3
"""Fleet soak: in-place tenant transfer of pending jobs.

  * Dual coordinators, four workers, two source tenants and two dest tenants.
  * Sources get a backlog; dests get a few native controls.
  * Independent source jobs are moved (same id) onto the dest tenant.
  * A parent+child pair on the source must stay: moving either is refused
    so the child cannot stall forever.
  * Source can no longer see moved jobs; dest workers complete those.
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
logger = logging.getLogger("tenant_move_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SEED_EACH = int(os.environ.get("SEED_EACH", "20") or "20")
CONTROL_EACH = int(os.environ.get("CONTROL_EACH", "6") or "6")
JOB_MS = int(os.environ.get("JOB_MS", "25") or "25")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
PAIRS = (("src-a", "dst-a"), ("src-b", "dst-b"))
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


def get_job(tid: str, job_id: str) -> tuple[int, dict[str, Any]]:
    return http_json("GET", f"/v1/jobs/{job_id}?tenant_id={tid}")


def enqueue(tid: str, n: int, *, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {
                    "kind": "render",
                    "ms": JOB_MS,
                    "i": i,
                    "p": prefix,
                },
                "labels": [prefix],
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def move(src: str, dest: str, job_id: str) -> dict[str, Any]:
    code, resp = http_json(
        "POST",
        f"/v1/jobs/{job_id}/tenant",
        {"tenant_id": src, "to_tenant_id": dest},
    )
    if code != 200:
        raise RuntimeError(f"move {src}->{dest} {job_id} {code} {resp}")
    return dict(resp["job"])


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def wait_completed(tids: list[str], expect: dict[str, int]) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    counts = {tid: 0 for tid in tids}
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
        "tenant-move soak seed=%d control=%d ms=%d",
        SEED_EACH,
        CONTROL_EACH,
        JOB_MS,
    )
    wait_coords()
    dests = [d for _, d in PAIRS]
    srcs = [s for s, _ in PAIRS]
    for tid in srcs + dests:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    seeds: dict[str, list[str]] = {}
    controls: dict[str, list[str]] = {}
    dags: dict[str, tuple[str, str]] = {}
    for src, dest in PAIRS:
        seeds[src] = enqueue(src, SEED_EACH, prefix="xfer")
        controls[dest] = enqueue(dest, CONTROL_EACH, prefix="ok")
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": src,
                "payload": {"kind": "render", "ms": JOB_MS, "p": "parent"},
                "labels": ["dag", "parent"],
            },
        )
        if code != 201:
            raise RuntimeError(f"parent {src} {code} {resp}")
        parent_id = str(resp["job"]["id"])
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": src,
                "payload": {"kind": "render", "ms": JOB_MS, "p": "child"},
                "depends_on": [parent_id],
                "labels": ["dag", "child"],
            },
        )
        if code != 201:
            raise RuntimeError(f"child {src} {code} {resp}")
        child_id = str(resp["job"]["id"])
        dags[src] = (parent_id, child_id)
        for label, jid in (("parent", parent_id), ("child", child_id)):
            code, body = http_json(
                "POST",
                f"/v1/jobs/{jid}/tenant",
                {"tenant_id": src, "to_tenant_id": dest},
            )
            if code != 409 or body.get("code") != "tenant_move_error":
                raise RuntimeError(
                    f"{src} {label} move should be refused: {code} {body}"
                )

    for src, dest in PAIRS:
        for sid in seeds[src]:
            job = move(src, dest, sid)
            if job.get("id") != sid:
                raise RuntimeError(f"id changed {sid} -> {job}")
            if job.get("tenant_id") != dest:
                raise RuntimeError(f"owner {job}")
            code, body = get_job(src, sid)
            if code != 404:
                raise RuntimeError(f"source still sees {sid}: {code} {body}")
            code, body = http_json(
                "POST",
                f"/v1/jobs/{sid}/cancel",
                {"tenant_id": src},
            )
            if code != 409:
                raise RuntimeError(f"source cancel {sid} {code} {body}")
        leftover = {str(j["id"]) for j in list_jobs(src)}
        parent_id, child_id = dags[src]
        if leftover != {parent_id, child_id}:
            raise RuntimeError(f"{src} leftover {leftover} != dag pair")
        dest_ids = {str(j["id"]) for j in list_jobs(dest)}
        if not set(seeds[src]).issubset(dest_ids):
            raise RuntimeError(f"{dest} missing moved jobs")
        if parent_id in dest_ids or child_id in dest_ids:
            raise RuntimeError(f"{dest} absorbed dag jobs")

    for tid in dests + srcs:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expect = {dest: CONTROL_EACH + SEED_EACH for _, dest in PAIRS}
    expect.update({src: 2 for src in srcs})
    wait_completed(srcs + dests, expect)
    for src, dest in PAIRS:
        jobs = {str(j["id"]): j for j in list_jobs(dest)}
        for sid in seeds[src] + controls[dest]:
            row = jobs.get(sid)
            if row is None or row.get("status") != "completed":
                raise RuntimeError(f"{dest} {sid} not completed: {row}")
            if row.get("tenant_id") != dest:
                raise RuntimeError(f"{dest} {sid} owner {row}")
        parent_id, child_id = dags[src]
        src_jobs = {str(j["id"]): j for j in list_jobs(src)}
        for sid in (parent_id, child_id):
            row = src_jobs.get(sid)
            if row is None or row.get("status") != "completed":
                raise RuntimeError(f"{src} dag {sid} not completed: {row}")
            if row.get("tenant_id") != src:
                raise RuntimeError(f"{src} dag {sid} left tenant: {row}")
    logger.info("tenant-move soak OK %s", expect)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("tenant-move soak failed")
        sys.exit(1)
