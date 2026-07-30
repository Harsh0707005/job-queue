#!/usr/bin/env python3
"""Fleet soak: spawn subtasks, parent parks without lease, join resumes.

Multi-tenant multi-worker:
  1. Enqueue parent jobs across tenants that spawn N children each.
  2. Assert parents park as awaiting_children (no lease held).
  3. Children complete on the worker fleet; parents resume and complete.
  4. One parent with a failing child is cancelled (not hung forever).
  5. One parent times out on join and is cancelled.
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
logger = logging.getLogger("spawn_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
PARENTS_PER_TENANT = int(os.environ.get("PARENTS_PER_TENANT", "2") or "2")
CHILDREN = int(os.environ.get("CHILDREN", "3") or "3")
CHILD_MS = int(os.environ.get("CHILD_MS", "300") or "300")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120") or "120")
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDS)
        _rr += 1
        bases = COORDS[start:] + COORDS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            last = RuntimeError(f"{exc.code}: {payload}")
            if 400 <= int(exc.code) < 500:
                raise last
            continue
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def get_job(job_id: str, tenant_id: str) -> dict[str, Any] | None:
    try:
        return http_json(
            "GET", f"/v1/jobs/{job_id}?tenant_id={tenant_id}"
        ).get("job")
    except Exception:
        return None


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def main() -> int:
    logger.info(
        "spawn soak tenants=%d parents/tenant=%d children=%d child_ms=%d",
        TENANTS,
        PARENTS_PER_TENANT,
        CHILDREN,
        CHILD_MS,
    )
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")

    # Concurrency=1 per tenant so parking frees a real slot.
    for i in range(TENANTS):
        tid = f"spawn-{i:02d}"
        http_json(
            "POST",
            f"/v1/tenants/{tid}/concurrency",
            {"max_concurrent_leases": 1},
        )

    happy_ids: list[tuple[str, str]] = []  # (tenant, job_id)
    for i in range(TENANTS):
        tid = f"spawn-{i:02d}"
        for p in range(PARENTS_PER_TENANT):
            body = {
                "tenant_id": tid,
                "payload": {
                    "kind": "spawn",
                    "spawn_children": [
                        {"payload": {"kind": "work", "ms": CHILD_MS, "i": c}}
                        for c in range(CHILDREN)
                    ],
                    "after_ms": 50,
                    "pre_ms": 30,
                },
                "priority": 0,
                "max_attempts": 3,
                "visibility_timeout_seconds": 30,
            }
            resp = http_json("POST", "/v1/jobs", body)
            job = resp.get("job") or {}
            happy_ids.append((tid, job["id"]))
            logger.info("enqueued parent %s tenant=%s", job["id"], tid)

    # Failing-child parent.
    fail_tid = "spawn-fail"
    http_json(
        "POST",
        f"/v1/tenants/{fail_tid}/concurrency",
        {"max_concurrent_leases": 2},
    )
    fail_resp = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": fail_tid,
            "payload": {
                "kind": "spawn",
                "spawn_children": [
                    {"payload": {"kind": "work", "ms": 100}},
                    {"payload": {"kind": "fail", "error": "child-forced-fail", "ms": 50}},
                ],
                "after_ms": 20,
            },
            "max_attempts": 1,
            "visibility_timeout_seconds": 30,
        },
    )
    fail_id = (fail_resp.get("job") or {})["id"]
    logger.info("enqueued fail-parent %s", fail_id)

    # Timeout parent (children sleep longer than join timeout).
    to_tid = "spawn-timeout"
    http_json(
        "POST",
        f"/v1/tenants/{to_tid}/concurrency",
        {"max_concurrent_leases": 2},
    )
    to_resp = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": to_tid,
            "payload": {
                "kind": "spawn",
                "spawn_children": [
                    {"payload": {"kind": "work", "ms": 30000}},
                ],
                "timeout_seconds": 3,
                "after_ms": 20,
            },
            "max_attempts": 1,
            "visibility_timeout_seconds": 60,
        },
    )
    to_id = (to_resp.get("job") or {})["id"]
    logger.info("enqueued timeout-parent %s", to_id)

    # Wait until some parents park (awaiting_children).
    parked_peak = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < 30:
        parked = 0
        for tid, jid in happy_ids:
            j = get_job(jid, tid)
            if j and j.get("status") == "awaiting_children":
                parked += 1
                # Must not hold a lease.
                if j.get("leased_by") or j.get("lease_token"):
                    logger.error("parked parent still leased: %s", j)
                    return 1
        parked_peak = max(parked_peak, parked)
        logger.info("parked parents=%d backlog=%s", parked, backlog())
        if parked_peak >= max(1, len(happy_ids) // 2):
            break
        time.sleep(0.4)
    if parked_peak < 1:
        logger.error("no parents parked as awaiting_children")
        return 1
    logger.info("saw parked_peak=%d", parked_peak)

    # Join deadlines are reaped on the normal lease path (same as approval
    # timeouts). Keep workers busy with lightweight probes so fair lease keeps
    # firing and expires the timeout-parent without a dedicated endpoint call.
    for i in range(12):
        try:
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": f"probe-{i % 4}",
                    "payload": {"kind": "work", "ms": 20},
                    "priority": -10,
                    "max_attempts": 1,
                    "visibility_timeout_seconds": 15,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("probe enqueue: %s", exc)

    # Wait for happy parents to complete; fail/timeout parents must cancel via
    # child failure and lease-path join-timeout reaping (not process-timeouts).
    deadline = time.monotonic() + MAX_WALL
    while time.monotonic() < deadline:
        done = 0
        for tid, jid in happy_ids:
            j = get_job(jid, tid)
            if j and j.get("status") == "completed":
                done += 1
        fail_j = get_job(fail_id, fail_tid)
        to_j = get_job(to_id, to_tid)
        logger.info(
            "progress happy=%d/%d fail=%s timeout=%s backlog=%s",
            done,
            len(happy_ids),
            (fail_j or {}).get("status"),
            (to_j or {}).get("status"),
            backlog(),
        )
        # Occasional more probes so an otherwise quiet fleet still leases.
        if int(time.monotonic() - t0) % 3 == 0:
            try:
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": "probe-keepalive",
                        "payload": {"kind": "work", "ms": 15},
                        "priority": -20,
                        "max_attempts": 1,
                    },
                )
            except Exception:
                pass
        if (
            done >= len(happy_ids)
            and fail_j
            and fail_j.get("status") == "cancelled"
            and to_j
            and to_j.get("status") == "cancelled"
        ):
            break
        time.sleep(0.8)

    # Validate happy path results.
    for tid, jid in happy_ids:
        j = get_job(jid, tid)
        if not j or j.get("status") != "completed":
            logger.error("happy parent not completed: %s %s", tid, j)
            return 1
        result = j.get("result") or {}
        if not result.get("joined"):
            logger.error("parent missing joined result: %s", j)
            return 1
        kids = http_json(
            "GET", f"/v1/jobs/{jid}/children?tenant_id={tid}"
        ).get("children") or []
        if len(kids) != CHILDREN:
            logger.error("expected %d children got %d", CHILDREN, len(kids))
            return 1
        if not all(k.get("status") == "completed" for k in kids):
            logger.error("not all children completed: %s", kids)
            return 1

    fail_j = get_job(fail_id, fail_tid)
    if not fail_j or fail_j.get("status") != "cancelled":
        logger.error("fail parent not cancelled: %s", fail_j)
        return 1
    if fail_j.get("await_children_outcome") != "failed":
        logger.error("fail parent outcome=%s", fail_j.get("await_children_outcome"))
        return 1

    to_j = get_job(to_id, to_tid)
    if not to_j or to_j.get("status") != "cancelled":
        logger.error("timeout parent not cancelled: %s", to_j)
        return 1
    if to_j.get("await_children_outcome") != "timeout":
        logger.error("timeout outcome=%s", to_j.get("await_children_outcome"))
        return 1

    logger.info(
        "SPAWN SOAK PASSED happy=%d fail=cancelled timeout=cancelled elapsed=%.1fs",
        len(happy_ids),
        time.monotonic() - t0,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
