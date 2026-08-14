#!/usr/bin/env python3
"""Fleet soak: bump/drop priority on live jobs under load + tenant weights.

  * Dual coordinators, four workers, three tenants (weights 4 / 2 / 1).
  * Equal low-priority backlogs start draining.
  * Mid-drain, pending jobs are bumped to a high lane.
  * Bumped work finishes while low leftovers remain.
  * Inside the high lane the heavier tenant still gets more turns.
  * Every job still completes. Cross-tenant bump is rejected.
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
logger = logging.getLogger("priority_change_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
LOW_EACH = int(os.environ.get("LOW_EACH", "50") or "50")
BUMP_EACH = int(os.environ.get("BUMP_EACH", "16") or "16")
JOB_MS = int(os.environ.get("JOB_MS", "80") or "80")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
MID_MIN = int(os.environ.get("MID_MIN", "16") or "16")
MID_MAX = int(os.environ.get("MID_MAX", "28") or "28")
WEIGHTS = {
    "pc-heavy": 4,
    "pc-mid": 2,
    "pc-light": 1,
}
HIGH_PRI = 100
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


def list_jobs(tid: str, *, status: str | None = None) -> list[dict[str, Any]]:
    path = f"/v1/jobs?tenant_id={tid}&limit=500"
    if status:
        path += f"&status={status}"
    code, body = http_json("GET", path)
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(tid: str, n: int, *, priority: int, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        body: dict[str, Any] = {
            "tenant_id": tid,
            "priority": priority,
            "payload": {
                "kind": "work",
                "ms": JOB_MS,
                "i": i,
                "t": tid,
                "p": prefix,
            },
        }
        code, resp = http_json("POST", "/v1/jobs", body)
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")
        ids.append(str(resp["job"]["id"]))
    return ids


def set_priority(tid: str, job_id: str, priority: int) -> dict[str, Any]:
    code, body = http_json(
        "POST",
        f"/v1/jobs/{job_id}/priority",
        {"tenant_id": tid, "priority": priority},
    )
    if code != 200:
        raise RuntimeError(f"priority {tid} {job_id} {code} {body}")
    return body.get("job") or {}


def counts_by_status(tid: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for j in list_jobs(tid):
        st = str(j.get("status") or "")
        out[st] = out.get(st, 0) + 1
    return out


def main() -> int:
    tenants = list(WEIGHTS)
    logger.info(
        "priority-change soak tenants=%s weights=%s low=%d bump=%d ms=%d",
        tenants,
        WEIGHTS,
        LOW_EACH,
        BUMP_EACH,
        JOB_MS,
    )
    wait_coords()

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/lease-weight",
            {"lease_weight": WEIGHTS[tid]},
        )
        if code != 200 or int(body.get("lease_weight") or 0) != WEIGHTS[tid]:
            raise RuntimeError(f"weight {tid} {code} {body}")
        enqueue(tid, LOW_EACH, priority=0, prefix="low")

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        done = sum(counts_by_status(tid).get("completed", 0) for tid in tenants)
        if done >= 8:
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("never reached initial drain")

    # Cross-tenant bump must fail.
    victim = list_jobs(tenants[0], status="pending")
    if not victim:
        raise RuntimeError("no pending job to probe isolation")
    code, body = http_json(
        "POST",
        f"/v1/jobs/{victim[0]['id']}/priority",
        {"tenant_id": tenants[1], "priority": HIGH_PRI},
    )
    if code != 409:
        raise RuntimeError(f"cross-tenant bump expected 409, got {code} {body}")

    bumped: dict[str, set[str]] = {tid: set() for tid in tenants}
    for tid in tenants:
        pending = [
            j
            for j in list_jobs(tid, status="pending")
            if int(j.get("priority") or 0) == 0
        ]
        if len(pending) < BUMP_EACH:
            raise RuntimeError(f"{tid} only {len(pending)} pending to bump")
        for j in pending[:BUMP_EACH]:
            out = set_priority(tid, str(j["id"]), HIGH_PRI)
            if int(out.get("priority") or 0) != HIGH_PRI:
                raise RuntimeError(f"bump failed {tid} {out}")
            bumped[tid].add(str(j["id"]))
    all_bumped = set().union(*bumped.values())
    logger.info("bumped %d jobs", len(all_bumped))

    last_log = 0.0
    mid_checked = False
    highs_done = False
    while time.time() < deadline:
        jobs = {tid: list_jobs(tid) for tid in tenants}
        high_done = {
            tid: sum(
                1
                for j in jobs[tid]
                if str(j["id"]) in bumped[tid] and j.get("status") == "completed"
            )
            for tid in tenants
        }
        low_pending = sum(
            1
            for tid in tenants
            for j in jobs[tid]
            if j.get("status") == "pending" and int(j.get("priority") or 0) == 0
        )
        total_high = sum(high_done.values())
        total_done = sum(
            1 for tid in tenants for j in jobs[tid] if j.get("status") == "completed"
        )
        now = time.time()
        if now - last_log >= 2:
            logger.info(
                "drain high=%s low_pending=%d total_done=%d",
                high_done,
                low_pending,
                total_done,
            )
            last_log = now
        if not mid_checked and MID_MIN <= total_high <= MID_MAX:
            heavy = high_done["pc-heavy"]
            light = high_done["pc-light"]
            if light < 1:
                raise RuntimeError(f"light tenant starved in high lane: {high_done}")
            if heavy <= light:
                raise RuntimeError(
                    f"heavy did not outpace light in high lane: {high_done}"
                )
            mid_checked = True
            logger.info("high-lane mid OK %s", high_done)
        if not highs_done and total_high == len(all_bumped):
            if low_pending < 1:
                raise RuntimeError(
                    "high lane drained but no low leftovers — bump did not jump"
                )
            highs_done = True
            logger.info(
                "all %d bumped complete with %d low still pending",
                total_high,
                low_pending,
            )
        expected = LOW_EACH * len(tenants)
        if total_done == expected and highs_done:
            if not mid_checked:
                raise RuntimeError(
                    f"drained too fast to sample high-lane mid ({high_done})"
                )
            logger.info("priority-change soak OK final high=%s", high_done)
            return 0
        time.sleep(0.15)
    raise RuntimeError(
        f"timed out mid={mid_checked} highs={highs_done} last={high_done}"
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("priority-change soak failed")
        sys.exit(1)
