#!/usr/bin/env python3
"""Fleet soak: graph-closed dead-letter replay under live load.

  * Dual coordinators, four workers, several tenants.
  * A DAG parent is frozen+dead-lettered so children cascade-cancel.
  * replay-graph brings parent+children back together; dest keep work
    keeps completing through the replay.
  * Concurrent replay of the same graph is idempotent (200/409) and
    never leaves a half-replayed DAG.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("replay_graph_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
KEEP_EACH = int(os.environ.get("KEEP_EACH", "8") or "8")
JOB_MS = int(os.environ.get("JOB_MS", "25") or "25")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
PAIRS = (("rg-src-a", "rg-ctl-a"), ("rg-src-b", "rg-ctl-b"))
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


def enqueue(
    tid: str,
    *,
    prefix: str,
    depends_on: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "queue_name": "default",
        "payload": {"kind": "render", "ms": JOB_MS, "p": prefix},
        "labels": [prefix],
        "max_attempts": 1,
    }
    if depends_on is not None:
        body["depends_on"] = depends_on
    if extra:
        body.update(extra)
    code, resp = http_json("POST", "/v1/jobs", body)
    if code != 201:
        raise RuntimeError(f"enqueue {tid} {code} {resp}")
    return str(resp["job"]["id"])


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


def get_job(tid: str, jid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/jobs/{jid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get {tid}/{jid} {code} {body}")
    return body.get("job") or {}


def main() -> int:
    logger.info("replay-graph soak keep=%d ms=%d", KEEP_EACH, JOB_MS)
    wait_coords()
    srcs = [s for s, _ in PAIRS]
    ctls = [c for _, c in PAIRS]
    for tid in srcs:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    parents: dict[str, str] = {}
    children: dict[str, list[str]] = {}
    keep: dict[str, list[str]] = {}
    for src, ctl in PAIRS:
        parents[src] = enqueue(src, prefix="par")
        children[src] = [
            enqueue(src, prefix="ch", depends_on=[parents[src]]),
            enqueue(src, prefix="ch", depends_on=[parents[src]]),
        ]
        keep[src] = [
            enqueue(src, prefix="keep") for _ in range(KEEP_EACH)
        ]
        for _ in range(KEEP_EACH):
            enqueue(ctl, prefix="ctl")

    wait_completed(ctls, {c: KEEP_EACH for c in ctls})

    for src in srcs:
        pid = parents[src]
        code, body = http_json(
            "POST", f"/v1/jobs/{pid}/freeze", {"tenant_id": src}
        )
        if code != 200:
            raise RuntimeError(f"freeze {src}/{pid} {code} {body}")
        code, body = http_json(
            "POST",
            f"/v1/jobs/{pid}/freeze/dead",
            {"tenant_id": src, "error": "graph-soak"},
        )
        if code != 200:
            raise RuntimeError(f"dead {src}/{pid} {code} {body}")
        if get_job(src, pid).get("status") != "dead":
            raise RuntimeError(f"parent not dead {src}/{pid}")
        for cid in children[src]:
            st = get_job(src, cid).get("status")
            if st != "cancelled":
                raise RuntimeError(f"child {cid} expected cancelled got {st}")

    for src in srcs:
        code, plan = http_json(
            "POST",
            "/v1/dead-letters/replay-graph/plan",
            {"tenant_id": src, "job_ids": [parents[src]]},
        )
        if code != 200 or plan.get("ok") is not True:
            raise RuntimeError(f"plan {src} {code} {plan}")
        want = {parents[src], *children[src]}
        if set(plan.get("replay_job_ids") or []) != want:
            raise RuntimeError(f"plan set {src}: {plan}")
        code, body = http_json(
            "POST",
            "/v1/dead-letters/replay-graph",
            {"tenant_id": src, "job_ids": [parents[src]], "clear_error": True},
        )
        if code != 200:
            raise RuntimeError(f"replay-graph {src} {code} {body}")
        if int(body.get("jobs_replayed") or 0) != 3:
            raise RuntimeError(f"replay count {src} {body}")
        if get_job(src, parents[src]).get("status") != "pending":
            raise RuntimeError(f"parent not pending {src}")
        for cid in children[src]:
            job = get_job(src, cid)
            if job.get("status") != "pending":
                raise RuntimeError(f"child not pending {cid} {job}")
            if job.get("depends_on") != [parents[src]]:
                raise RuntimeError(f"child deps {cid} {job.get('depends_on')}")

    for src in srcs:
        code, body = http_json("POST", f"/v1/tenants/{src}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {src} {code} {body}")

    expect = {s: 3 + KEEP_EACH for s in srcs}
    expect.update({c: KEEP_EACH for c in ctls})
    wait_completed(srcs + ctls, expect)

    # Concurrent replay of a second DAG: two coordinators, same graph.
    race_src = "rg-race"
    race_ctl = "rg-race-ctl"
    code, body = http_json("POST", f"/v1/tenants/{race_src}/pause", {})
    if code != 200:
        raise RuntimeError(f"pause {race_src} {code} {body}")
    rparent = enqueue(race_src, prefix="rpar")
    rkids = [
        enqueue(race_src, prefix="rch", depends_on=[rparent]),
        enqueue(race_src, prefix="rch", depends_on=[rparent]),
    ]
    for _ in range(KEEP_EACH):
        enqueue(race_ctl, prefix="rctl")
    wait_completed([race_ctl], {race_ctl: KEEP_EACH})
    code, body = http_json(
        "POST", f"/v1/jobs/{rparent}/freeze", {"tenant_id": race_src}
    )
    if code != 200:
        raise RuntimeError(f"freeze race {code} {body}")
    code, body = http_json(
        "POST",
        f"/v1/jobs/{rparent}/freeze/dead",
        {"tenant_id": race_src, "error": "race"},
    )
    if code != 200:
        raise RuntimeError(f"dead race {code} {body}")
    # Fire the same graph replay twice (round-robin hits both coordinators).
    codes: list[int] = []
    moved: list[int] = []
    for _ in range(2):
        code, body = http_json(
            "POST",
            "/v1/dead-letters/replay-graph",
            {"tenant_id": race_src, "job_ids": [rparent]},
        )
        if code not in (200, 409):
            raise RuntimeError(f"race replay {code} {body}")
        codes.append(code)
        moved.append(int((body or {}).get("jobs_replayed") or 0))
        logger.info("race replay code=%s moved=%s", code, moved[-1])
    if sum(moved) not in (3, 0 + 3, 3 + 0):
        # One call replays the trio; the other is 0 (already pending) or 409.
        if sum(moved) != 3:
            raise RuntimeError(f"race replay counts {codes} {moved}")
    for jid in [rparent, *rkids]:
        st = get_job(race_src, jid).get("status")
        if st not in ("pending", "awaiting_skills"):
            raise RuntimeError(f"race job {jid} stuck {st}")
    code, body = http_json("POST", f"/v1/tenants/{race_src}/resume", {})
    if code != 200:
        raise RuntimeError(f"resume race {code} {body}")
    wait_completed([race_src, race_ctl], {race_src: 3, race_ctl: KEEP_EACH})

    # TTL-killed DAG: replay must refresh expires_at or the cleaner reaps again.
    ttl_src, ttl_ctl = "rg-ttl", "rg-ttl-ctl"
    code, body = http_json("POST", f"/v1/tenants/{ttl_src}/pause", {})
    if code != 200:
        raise RuntimeError(f"pause {ttl_src} {code} {body}")
    code, body = http_json(
        "POST",
        f"/v1/tenants/{ttl_src}/job-ttl",
        {"ttl_seconds": 60, "expiry_action": "dead"},
    )
    if code != 200:
        raise RuntimeError(f"set ttl {ttl_src} {code} {body}")
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    ttl_extra = {"expires_at": past, "expiry_action": "dead"}
    tparent = enqueue(ttl_src, prefix="tpar", extra=ttl_extra)
    tkids = [
        enqueue(ttl_src, prefix="tch", depends_on=[tparent], extra=ttl_extra),
        enqueue(ttl_src, prefix="tch", depends_on=[tparent], extra=ttl_extra),
    ]
    for _ in range(KEEP_EACH):
        enqueue(ttl_ctl, prefix="tctl")
    wait_completed([ttl_ctl], {ttl_ctl: KEEP_EACH})
    code, body = http_json(
        "POST", "/v1/jobs/process-expired", {"tenant_id": ttl_src, "limit": 50}
    )
    if code != 200:
        raise RuntimeError(f"expire {ttl_src} {code} {body}")
    if get_job(ttl_src, tparent).get("status") != "dead":
        raise RuntimeError(f"ttl parent not dead {get_job(ttl_src, tparent)}")
    code, body = http_json(
        "POST",
        "/v1/dead-letters/replay-graph",
        {"tenant_id": ttl_src, "job_ids": [tparent], "clear_error": True},
    )
    if code != 200 or int(body.get("jobs_replayed") or 0) < 1:
        raise RuntimeError(f"ttl replay {code} {body}")
    code, body = http_json(
        "POST", "/v1/jobs/process-expired", {"tenant_id": ttl_src, "limit": 50}
    )
    if code != 200:
        raise RuntimeError(f"re-expire {ttl_src} {code} {body}")
    for jid in [tparent, *tkids]:
        st = get_job(ttl_src, jid).get("status")
        if st not in ("pending", "awaiting_skills"):
            raise RuntimeError(f"ttl cleaner reaped {jid} as {st}")
    code, body = http_json("POST", f"/v1/tenants/{ttl_src}/resume", {})
    if code != 200:
        raise RuntimeError(f"resume ttl {code} {body}")
    wait_completed([ttl_src, ttl_ctl], {ttl_src: 3, ttl_ctl: KEEP_EACH})
    logger.info("replay-graph soak OK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("replay-graph soak failed")
        sys.exit(1)
