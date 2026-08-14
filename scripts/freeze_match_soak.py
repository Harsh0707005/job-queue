#!/usr/bin/env python3
"""Fleet soak: freeze/unfreeze matching by tenant + label.

  * Dual coordinators, four workers, several tenants.
  * Each tenant gets hold:red, hold:blue, and unlabeled work.
  * freeze-matching hold:red on some tenants only. Those stay frozen.
  * Blue + unlabeled on every tenant, and red on the untouched tenant,
    keep draining.
  * unfreeze-matching releases only that tenant's red jobs.
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
logger = logging.getLogger("freeze_match_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "3") or "3")
EACH = int(os.environ.get("JOBS_EACH", "10") or "10")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
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


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(tid: str, n: int, *, labels: list[str] | None, prefix: str) -> None:
    for i in range(n):
        body: dict[str, Any] = {
            "tenant_id": tid,
            "payload": {"kind": "work", "ms": 10, "i": i, "p": prefix},
        }
        if labels:
            body["labels"] = labels
        code, resp = http_json("POST", "/v1/jobs", body)
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")


def bucket(jobs: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out = {
        "red": {"frozen": 0, "completed": 0, "other": 0},
        "blue": {"frozen": 0, "completed": 0, "other": 0},
        "bare": {"frozen": 0, "completed": 0, "other": 0},
    }
    for j in jobs:
        labs = j.get("labels") or []
        if "hold:red" in labs:
            kind = "red"
        elif "hold:blue" in labs:
            kind = "blue"
        else:
            kind = "bare"
        st = j.get("status")
        if st == "frozen":
            out[kind]["frozen"] += 1
        elif st == "completed":
            out[kind]["completed"] += 1
        else:
            out[kind]["other"] += 1
    return out


def main() -> int:
    tenants = [f"fm-{i:02d}" for i in range(TENANTS)]
    hold = tenants[:2]
    free = tenants[2:]
    logger.info("freeze-match soak tenants=%s hold=%s", tenants, hold)
    wait_coords()

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        enqueue(tid, EACH, labels=["hold:red", "kind:work"], prefix="red")
        enqueue(tid, EACH, labels=["hold:blue", "kind:work"], prefix="blue")
        enqueue(tid, EACH, labels=None, prefix="bare")

    for tid in hold:
        code, body = http_json(
            "POST",
            "/v1/jobs/freeze-matching",
            {"tenant_id": tid, "labels": ["hold:red"], "reason": "soak"},
        )
        if code != 200 or int(body.get("count") or 0) != EACH:
            raise RuntimeError(f"freeze-matching {tid} {code} {body}")
        if not body.get("complete"):
            raise RuntimeError(f"freeze-matching incomplete {tid} {body}")

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        snaps = {tid: bucket(list_jobs(tid)) for tid in tenants}
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s", snaps)
            last = now
        ok = True
        for tid in tenants:
            b = snaps[tid]
            if b["blue"]["completed"] != EACH or b["bare"]["completed"] != EACH:
                ok = False
            if b["blue"]["frozen"] or b["bare"]["frozen"]:
                raise RuntimeError(f"{tid} non-red frozen: {b}")
        for tid in hold:
            if snaps[tid]["red"]["frozen"] != EACH:
                ok = False
            if snaps[tid]["red"]["completed"]:
                raise RuntimeError(f"{tid} red completed while frozen")
        for tid in free:
            if snaps[tid]["red"]["completed"] != EACH:
                ok = False
            if snaps[tid]["red"]["frozen"]:
                raise RuntimeError(f"{tid} red frozen without matching call")
        if ok:
            break
        time.sleep(0.4)
    else:
        raise RuntimeError("timed out waiting for isolation drain")

    # Unfreeze only the first held tenant's red jobs.
    thaw = hold[0]
    still = hold[1]
    code, body = http_json(
        "POST",
        "/v1/jobs/unfreeze-matching",
        {"tenant_id": thaw, "labels": ["hold:red"]},
    )
    if code != 200 or int(body.get("count") or 0) != EACH:
        raise RuntimeError(f"unfreeze-matching {thaw} {code} {body}")

    while time.time() < deadline:
        b_thaw = bucket(list_jobs(thaw))
        b_still = bucket(list_jobs(still))
        now = time.time()
        if now - last >= 2:
            logger.info("thaw %s still %s", b_thaw, b_still)
            last = now
        if b_still["red"]["frozen"] != EACH:
            raise RuntimeError(f"{still} red unfrozen by other tenant: {b_still}")
        if b_thaw["red"]["completed"] == EACH and b_thaw["red"]["frozen"] == 0:
            logger.info(
                "freeze-match soak OK thawed=%s held=%s each=%d",
                thaw,
                still,
                EACH,
            )
            return 0
        time.sleep(0.4)
    raise RuntimeError("timed out after unfreeze-matching")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("freeze-match soak failed")
        sys.exit(1)
