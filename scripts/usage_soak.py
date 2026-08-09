#!/usr/bin/env python3
"""Fleet soak: per-tenant billing usage must match real work (no double count).

Enqueues N tenants × M jobs through dual coordinators, drains via the compose
workers, then compares GET /v1/usage to completed job counts.

  * enqueues == jobs created per tenant
  * acks == completed jobs per tenant
  * leases >= acks (reclaims allowed) and not wildly larger
  * no tenant is billed for another tenant's work
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
logger = logging.getLogger("usage_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or os.environ.get("COORDINATOR_URL")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "8"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "40"))
ENQUEUE_WORKERS = int(os.environ.get("ENQUEUE_WORKERS", "16"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240"))
POLL = float(os.environ.get("POLL_SECONDS", "1.0"))
LEASE_SLACK_RATIO = float(os.environ.get("LEASE_SLACK_RATIO", "2.5"))
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 60.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDINATOR_URLS)
    _rr += 1
    ordered = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
    last: Exception | None = None
    for base in ordered:
        url = base + path
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                code = resp.status
                if code == 204 or not raw:
                    return code, {}
                return code, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{url} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except urllib.error.URLError as exc:
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, body = http_json("GET", "/health", timeout=5)
            if code == 200 and body.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not healthy")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list jobs {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    logger.info(
        "usage soak tenants=%s jobs_per=%s total=%s coords=%s",
        TENANTS,
        JOBS_PER_TENANT,
        total,
        COORDINATOR_URLS,
    )
    wait_healthy()

    jobs = [(f"u{t:02d}", i) for t in range(TENANTS) for i in range(JOBS_PER_TENANT)]

    def one(item: tuple[str, int]) -> None:
        tenant, i = item
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tenant,
                "payload": {"n": i, "kind": "work", "ms": 15},
                "max_attempts": 3,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"enqueue failed {code}: {body}")

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=ENQUEUE_WORKERS) as pool:
        futs = [pool.submit(one, j) for j in jobs]
        for f in as_completed(futs):
            f.result()
    logger.info("enqueued %s in %.2fs", total, time.time() - t0)

    deadline = time.time() + MAX_WALL
    last_log = 0.0
    while time.time() < deadline:
        completed = 0
        pending = 0
        for t in range(TENANTS):
            for j in list_jobs(f"u{t:02d}"):
                st = j.get("status")
                if st == "completed":
                    completed += 1
                elif st in ("pending", "leased"):
                    pending += 1
        now = time.time()
        if now - last_log >= 2:
            logger.info("completed=%s pending=%s", completed, pending)
            last_log = now
        if completed >= total and pending == 0:
            break
        time.sleep(POLL)
    else:
        raise RuntimeError("usage soak timed out waiting for drain")

    time.sleep(0.5)
    code, body = http_json("GET", "/v1/usage")
    if code != 200:
        raise RuntimeError(f"usage list {code}: {body}")
    rows = {
        r["tenant_id"]: r
        for r in (body.get("usage") or [])
        if str(r.get("tenant_id") or "").startswith("u")
    }
    errors: list[str] = []
    for t in range(TENANTS):
        tid = f"u{t:02d}"
        jobs_t = list_jobs(tid)
        done = sum(1 for j in jobs_t if j.get("status") == "completed")
        created = len(jobs_t)
        u = rows.get(tid) or {}
        enq = int(u.get("enqueues") or 0)
        acks = int(u.get("acks") or 0)
        leases = int(u.get("leases") or 0)
        if created != JOBS_PER_TENANT:
            errors.append(f"{tid} jobs={created} want {JOBS_PER_TENANT}")
        if enq != JOBS_PER_TENANT:
            errors.append(f"{tid} enqueues={enq} want {JOBS_PER_TENANT}")
        if acks != done or done != JOBS_PER_TENANT:
            errors.append(f"{tid} acks={acks} completed={done}")
        if leases < acks:
            errors.append(f"{tid} leases={leases} < acks={acks}")
        if leases > int(acks * LEASE_SLACK_RATIO) + 2:
            errors.append(f"{tid} leases={leases} too high vs acks={acks}")
        if int(u.get("fails") or 0) or int(u.get("dead_letters") or 0):
            errors.append(f"{tid} unexpected fail/dead {u}")
        one = http_json("GET", f"/v1/usage?tenant_id={tid}")[1].get("usage") or {}
        if int(one.get("enqueues") or 0) != enq:
            errors.append(f"{tid} list vs get mismatch")
    if errors:
        raise RuntimeError("usage mismatches: " + "; ".join(errors[:8]))

    # Cross-check: no extra tenants billed for this soak prefix.
    billed = sum(int(r.get("enqueues") or 0) for r in rows.values())
    if billed != total:
        raise RuntimeError(f"billed enqueues {billed} != {total}")
    logger.info(
        "usage soak OK tenants=%s enqueues=%s acks match completed runtime_ms sample=%s",
        TENANTS,
        total,
        int((rows.get("u00") or {}).get("runtime_ms") or 0),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("usage soak failed")
        sys.exit(1)
