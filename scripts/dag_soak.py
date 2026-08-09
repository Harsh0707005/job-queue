#!/usr/bin/env python3
"""Fleet soak: atomic DAG enqueue under multi-tenant multi-worker load.

  * Each tenant submits several diamond/join DAGs through dual coordinators.
  * The same batch_key is submitted concurrently (crash-retry / double-click).
  * Workers drain unskilled work; join nodes must not complete before parents.
  * Final job counts match unique batches (no duplicate graphs).
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

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dag_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "6") or "6")
DAGS_PER_TENANT = int(os.environ.get("DAGS_PER_TENANT", "4") or "4")
DUP_SUBMITS = int(os.environ.get("DUP_SUBMITS", "4") or "4")
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


def dag_jobs(i: int) -> list[dict[str, Any]]:
    return [
        {
            "ref": "extract",
            "payload": {"kind": "work", "ms": 15, "step": "extract", "i": i},
        },
        {
            "ref": "t1",
            "payload": {"kind": "work", "ms": 15, "step": "t1", "i": i},
            "depends_on": ["extract"],
        },
        {
            "ref": "t2",
            "payload": {"kind": "work", "ms": 15, "step": "t2", "i": i},
            "depends_on": ["extract"],
        },
        {
            "ref": "join",
            "payload": {"kind": "work", "ms": 15, "step": "join", "i": i},
            "depends_on": ["t1", "t2"],
        },
        {
            "ref": "notify",
            "payload": {"kind": "work", "ms": 10, "step": "notify", "i": i},
            "depends_on": ["join"],
        },
    ]


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def main() -> int:
    expect_dags = TENANTS * DAGS_PER_TENANT
    expect_jobs = expect_dags * 5
    logger.info(
        "dag soak tenants=%d dags_each=%d dups=%d expect_jobs=%d",
        TENANTS,
        DAGS_PER_TENANT,
        DUP_SUBMITS,
        expect_jobs,
    )
    wait_coords()

    batches = [
        (f"dag-{t:02d}", f"batch-{t:02d}-{i}")
        for t in range(TENANTS)
        for i in range(DAGS_PER_TENANT)
    ]

    created = 0
    reused = 0
    errors: list[str] = []

    def submit(item: tuple[str, str]) -> tuple[bool, str]:
        tid, key = item
        code, body = http_json(
            "POST",
            "/v1/jobs/dag",
            {
                "tenant_id": tid,
                "batch_key": key,
                "jobs": dag_jobs(int(key.rsplit("-", 1)[-1])),
            },
        )
        if code not in (200, 201):
            return False, f"{tid}/{key} -> {code} {body}"
        return bool(body.get("created")), ""

    # Concurrent duplicate submits of every batch.
    work = batches * DUP_SUBMITS
    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(submit, w) for w in work]
        for f in as_completed(futs):
            ok, err = f.result()
            if err:
                errors.append(err)
                continue
            if ok:
                created += 1
            else:
                reused += 1
    if errors:
        raise RuntimeError("enqueue errors: " + "; ".join(errors[:6]))
    if created != expect_dags:
        raise RuntimeError(f"created {created} unique DAGs want {expect_dags}")
    if reused != expect_dags * (DUP_SUBMITS - 1):
        raise RuntimeError(
            f"idempotent reuse {reused} want {expect_dags * (DUP_SUBMITS - 1)}"
        )
    logger.info("enqueued unique=%d reused=%d", created, reused)

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        done = 0
        pending = 0
        for t in range(TENANTS):
            for j in list_jobs(f"dag-{t:02d}"):
                st = j.get("status")
                if st == "completed":
                    done += 1
                elif st in ("pending", "leased"):
                    pending += 1
        now = time.time()
        if now - last >= 2:
            logger.info("completed=%d/%d pending=%d", done, expect_jobs, pending)
            last = now
        if done == expect_jobs and pending == 0:
            logger.info("dag soak OK jobs=%d", done)
            return 0
        time.sleep(0.4)
    raise RuntimeError("dag soak timed out waiting for drain")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("dag soak failed")
        sys.exit(1)
