#!/usr/bin/env python3
"""Fleet soak: last-success worker preference under multi-tenant load.

  * Dual coordinators, four workers, two tenants.
  * aff-hot opts the default queue into last-worker preference.
  * aff-cold does not.
  * Equal backlogs of kind=render and kind=other.
  * Hot tenant's render completions concentrate on the first winner
    more than the cold tenant.
  * Every job still completes (preference must not stall work).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("affinity_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
RENDER_EACH = int(os.environ.get("RENDER_EACH", "24") or "24")
OTHER_EACH = int(os.environ.get("OTHER_EACH", "16") or "16")
JOB_MS = int(os.environ.get("JOB_MS", "40") or "40")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
HOT = "aff-hot"
COLD = "aff-cold"
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


def enqueue(tid: str, n: int, *, kind: str, prefix: str) -> None:
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
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid}/{kind} {code} {resp}")


def actors(tid: str, render_ids: set[str]) -> Counter[str]:
    code, body = http_json(
        "GET", f"/v1/tenants/{tid}/audit?limit=2000"
    )
    if code != 200:
        raise RuntimeError(f"audit {tid} {code}: {body}")
    out: Counter[str] = Counter()
    for ev in body.get("events") or []:
        if ev.get("event_type") != "completed":
            continue
        if str(ev.get("job_id") or "") not in render_ids:
            continue
        actor = ev.get("actor")
        if actor:
            out[str(actor)] += 1
    return out


def completed(jobs: list[dict[str, Any]], *, kind: str | None = None) -> int:
    n = 0
    for j in jobs:
        if j.get("status") != "completed":
            continue
        if kind is None or (j.get("payload") or {}).get("kind") == kind:
            n += 1
    return n


def main() -> int:
    logger.info(
        "affinity soak render=%d other=%d ms=%d",
        RENDER_EACH,
        OTHER_EACH,
        JOB_MS,
    )
    wait_coords()
    for tid in (HOT, COLD):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
    code, body = http_json(
        "POST",
        f"/v1/tenants/{HOT}/worker-affinity",
        {"queue_name": "default", "enabled": True, "hold_seconds": 20},
    )
    if code != 200 or "default" not in (body.get("queues") or []):
        raise RuntimeError(f"affinity {code} {body}")
    for tid in (HOT, COLD):
        enqueue(tid, RENDER_EACH, kind="render", prefix="r")
        enqueue(tid, OTHER_EACH, kind="other", prefix="o")
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expect = RENDER_EACH + OTHER_EACH
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        hot = list_jobs(HOT)
        cold = list_jobs(COLD)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "drain hot=%d/%d cold=%d/%d",
                completed(hot),
                expect,
                completed(cold),
                expect,
            )
            last = now
        if completed(hot) == expect and completed(cold) == expect:
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("timed out waiting for drain")

    hot_jobs = list_jobs(HOT)
    cold_jobs = list_jobs(COLD)
    hot_ids = {
        str(j["id"])
        for j in hot_jobs
        if (j.get("payload_kind") or (j.get("payload") or {}).get("kind"))
        == "render"
    }
    cold_ids = {
        str(j["id"])
        for j in cold_jobs
        if (j.get("payload_kind") or (j.get("payload") or {}).get("kind"))
        == "render"
    }
    hot_a = actors(HOT, hot_ids)
    cold_a = actors(COLD, cold_ids)
    hot_top = hot_a.most_common(1)[0][1] if hot_a else 0
    cold_top = cold_a.most_common(1)[0][1] if cold_a else 0
    logger.info("render actors hot=%s cold=%s", dict(hot_a), dict(cold_a))
    if sum(hot_a.values()) != RENDER_EACH:
        raise RuntimeError(f"hot render audit {sum(hot_a.values())} != {RENDER_EACH}")
    if sum(cold_a.values()) != RENDER_EACH:
        raise RuntimeError(
            f"cold render audit {sum(cold_a.values())} != {RENDER_EACH}"
        )
    # Soft prefer + partitions: require a clear concentration on the hot tenant.
    if hot_top < max(cold_top, 2) and hot_top < int(RENDER_EACH * 0.4):
        raise RuntimeError(
            f"hot tenant did not concentrate renders: hot_top={hot_top} "
            f"cold_top={cold_top} hot={dict(hot_a)} cold={dict(cold_a)}"
        )
    logger.info(
        "affinity soak OK hot_top=%d cold_top=%d",
        hot_top,
        cold_top,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("affinity soak failed")
        sys.exit(1)
