#!/usr/bin/env python3
"""Fleet soak: concurrent clones of the same source must stay atomic.

  * Dual coordinators, four workers, two tenants.
  * Many threads clone one completed source at the same time.
  * HTTP retries on error (the old UniqueViolation path) must not create
    extra jobs: persisted clones == successful 201 responses.
  * Source audit ``cloned`` seqs are unique and match that count.
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

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("clone_race_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
CLONES_EACH = int(os.environ.get("CLONES_EACH", "40") or "40")
THREADS = int(os.environ.get("RACE_THREADS", "20") or "20")
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
LEFT = "clone-race-a"
RIGHT = "clone-race-b"
_rr = 0
_rr_lock = threading.Lock()


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
    with _rr_lock:
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


def list_audit(tid: str, job_id: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs/{job_id}/audit?tenant_id={tid}&limit=2000")
    if code != 200:
        raise RuntimeError(f"audit {tid} {code}: {body}")
    return list(body.get("events") or [])


def enqueue_source(tid: str) -> str:
    code, resp = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": tid,
            "payload": {"kind": "render", "ms": JOB_MS, "p": "src"},
            "priority": 5,
            "labels": ["race"],
        },
    )
    if code != 201:
        raise RuntimeError(f"enqueue {tid} {code} {resp}")
    return str(resp["job"]["id"])


def clone_once(tid: str, job_id: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/jobs/{job_id}/clone", {"tenant_id": tid})


def clone_with_retry(tid: str, job_id: str) -> tuple[int, dict[str, Any], int]:
    """Return (status, body, extra_attempts). extra_attempts is 1 if retried."""
    code, body = clone_once(tid, job_id)
    if code == 201:
        return code, body, 0
    code2, body2 = clone_once(tid, job_id)
    return code2, body2, 1


def wait_completed(tids: list[str], expect: dict[str, int]) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    counts = {tid: 0 for tid in tids}
    while time.time() < deadline:
        now = time.time()
        counts = {
            tid: sum(1 for j in list_jobs(tid) if j.get("status") == "completed")
            for tid in tids
        }
        if now - last >= 2:
            logger.info("drain %s", counts)
            last = now
        if all(counts[tid] >= expect[tid] for tid in tids):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for drain: {counts}")


def race_clones(tid: str, source_id: str, n: int) -> tuple[int, int]:
    created = 0
    retries = 0
    barrier = threading.Barrier(min(THREADS, n))

    def one(_: int) -> tuple[int, int]:
        barrier.wait()
        code, body, extra = clone_with_retry(tid, source_id)
        if code != 201:
            raise RuntimeError(f"clone {tid} {code} {body}")
        if not body.get("job", {}).get("id"):
            raise RuntimeError(f"clone missing id {body}")
        if body["job"].get("cloned_from_job_id") != source_id:
            raise RuntimeError(f"bad pointer {body}")
        return 1, extra

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        futs = [pool.submit(one, i) for i in range(n)]
        for fut in as_completed(futs):
            ok, extra = fut.result()
            created += ok
            retries += extra
    return created, retries


def main() -> int:
    logger.info(
        "clone race soak clones=%d threads=%d ms=%d",
        CLONES_EACH,
        THREADS,
        JOB_MS,
    )
    wait_coords()
    sources: dict[str, str] = {}
    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        sources[tid] = enqueue_source(tid)
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    wait_completed(
        [LEFT, RIGHT], {LEFT: 1, RIGHT: 1}
    )

    for tid in (LEFT, RIGHT):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    totals: dict[str, tuple[int, int]] = {}
    for tid in (LEFT, RIGHT):
        created, retries = race_clones(tid, sources[tid], CLONES_EACH)
        totals[tid] = (created, retries)
        logger.info("%s created=%d retries=%d", tid, created, retries)
        if created != CLONES_EACH:
            raise RuntimeError(f"{tid} created {created} != {CLONES_EACH}")
        kids = list_jobs(tid, cloned_from_job_id=sources[tid])
        if len(kids) != CLONES_EACH:
            raise RuntimeError(
                f"{tid} persisted {len(kids)} clones != {CLONES_EACH} successes "
                f"(retry extras?)"
            )
        events = [
            e
            for e in list_audit(tid, sources[tid])
            if e.get("event_type") == "cloned"
        ]
        if len(events) != CLONES_EACH:
            raise RuntimeError(
                f"{tid} cloned audit {len(events)} != {CLONES_EACH}"
            )
        seqs = [int(e["seq"]) for e in events]
        if len(set(seqs)) != CLONES_EACH:
            raise RuntimeError(f"{tid} duplicate audit seq {seqs}")
        if retries:
            raise RuntimeError(
                f"{tid} needed {retries} retries — clone is not atomic"
            )
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expect = {tid: 1 + CLONES_EACH for tid in (LEFT, RIGHT)}
    wait_completed([LEFT, RIGHT], expect)
    logger.info("clone race soak OK %s", totals)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("clone race soak failed")
        sys.exit(1)
