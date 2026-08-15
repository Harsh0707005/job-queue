#!/usr/bin/env python3
"""Fleet soak: in-place queue move under multi-tenant load.

  * Dual coordinators, four workers (default queue only), two tenants.
  * Seed jobs land on ``wrong`` so workers cannot lease them.
  * Move every seed onto ``default`` (same id) then they complete.
  * A clone of one seed keeps its pointer after the source is moved.
  * Drain still blocks a move onto a closed destination.
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
logger = logging.getLogger("queue_move_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SEED_EACH = int(os.environ.get("SEED_EACH", "24") or "24")
CONTROL_EACH = int(os.environ.get("CONTROL_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "25") or "25")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
LEFT = "move-a"
RIGHT = "move-b"
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


def enqueue(tid: str, n: int, *, queue_name: str, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, resp = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "queue_name": queue_name,
                "payload": {
                    "kind": "render",
                    "ms": JOB_MS,
                    "i": i,
                    "p": prefix,
                },
                "labels": [prefix, queue_name],
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid}/{queue_name} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def move(tid: str, job_id: str, queue_name: str) -> dict[str, Any]:
    code, resp = http_json(
        "POST",
        f"/v1/jobs/{job_id}/queue",
        {"tenant_id": tid, "queue_name": queue_name},
    )
    if code != 200:
        raise RuntimeError(f"move {tid}/{job_id} {code} {resp}")
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
        "queue-move soak seed=%d control=%d ms=%d",
        SEED_EACH,
        CONTROL_EACH,
        JOB_MS,
    )
    wait_coords()
    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    wrong: dict[str, list[str]] = {}
    control: dict[str, list[str]] = {}
    clones: dict[str, str] = {}
    for tid in (LEFT, RIGHT):
        wrong[tid] = enqueue(
            tid, SEED_EACH, queue_name="wrong", prefix="mis"
        )
        control[tid] = enqueue(
            tid, CONTROL_EACH, queue_name="default", prefix="ok"
        )
        code, resp = http_json(
            "POST",
            f"/v1/jobs/{wrong[tid][0]}/clone",
            {"tenant_id": tid},
        )
        if code != 201:
            raise RuntimeError(f"clone {tid} {code} {resp}")
        clones[tid] = str(resp["job"]["id"])
        if resp["job"].get("cloned_from_job_id") != wrong[tid][0]:
            raise RuntimeError(f"clone pointer {resp}")

    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    # Only the default-queue controls should finish while seeds sit on wrong.
    wait_completed(
        [LEFT, RIGHT], {LEFT: CONTROL_EACH, RIGHT: CONTROL_EACH}
    )
    for tid in (LEFT, RIGHT):
        stuck = list_jobs(tid, queue_name="wrong", status="pending")
        if len(stuck) != SEED_EACH + 1:
            raise RuntimeError(
                f"{tid} expected {SEED_EACH + 1} stuck on wrong, got {len(stuck)}"
            )

    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        for sid in wrong[tid]:
            job = move(tid, sid, "default")
            if job.get("id") != sid:
                raise RuntimeError(f"{tid} move changed id {sid} -> {job}")
            if job.get("queue_name") != "default":
                raise RuntimeError(f"{tid} move dest {job}")
        # Clone of the first seed is still on wrong and still points at it.
        kids = list_jobs(tid, cloned_from_job_id=wrong[tid][0])
        if len(kids) != 1 or kids[0]["id"] != clones[tid]:
            raise RuntimeError(f"{tid} clone missing after move {kids}")
        if kids[0].get("queue_name") != "wrong":
            raise RuntimeError(f"{tid} clone should stay on wrong")
        if kids[0].get("cloned_from_job_id") != wrong[tid][0]:
            raise RuntimeError(f"{tid} clone pointer lost")
        # Drain dest still blocks a leftover clone move.
        code, body = http_json(
            "POST", f"/v1/tenants/{tid}/drain-queue", {"queue_name": "email"}
        )
        if code != 200:
            raise RuntimeError(f"drain {tid} {code} {body}")
        code, body = http_json(
            "POST",
            f"/v1/jobs/{clones[tid]}/queue",
            {"tenant_id": tid, "queue_name": "email"},
        )
        if code != 409 or body.get("code") != "queue_draining":
            raise RuntimeError(f"expected drain on move {code} {body}")
        code, body = http_json(
            "POST", f"/v1/tenants/{tid}/undrain-queue", {"queue_name": "email"}
        )
        if code != 200:
            raise RuntimeError(f"undrain {tid} {code} {body}")
        moved_clone = move(tid, clones[tid], "default")
        if moved_clone.get("cloned_from_job_id") != wrong[tid][0]:
            raise RuntimeError(f"{tid} moved clone lost pointer")
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expect = {
        tid: CONTROL_EACH + SEED_EACH + 1 for tid in (LEFT, RIGHT)
    }
    wait_completed([LEFT, RIGHT], expect)
    for tid in (LEFT, RIGHT):
        jobs = {str(j["id"]): j for j in list_jobs(tid)}
        for sid in wrong[tid] + control[tid] + [clones[tid]]:
            row = jobs.get(sid)
            if row is None or row.get("status") != "completed":
                raise RuntimeError(f"{tid} {sid} not completed: {row}")
            if row.get("queue_name") != "default":
                raise RuntimeError(f"{tid} {sid} not on default: {row}")
    logger.info("queue-move soak OK left=%d right=%d", expect[LEFT], expect[RIGHT])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("queue-move soak failed")
        sys.exit(1)
