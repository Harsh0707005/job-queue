#!/usr/bin/env python3
"""Fleet soak: operator handoff moves the control leader without TTL wait.

  * Three coordinators, 30s lease TTL.
  * POST /v1/coordinator/leader/handoff to a follower.
  * New node must become leader in well under the TTL.
  * Old node is no longer leader and cannot process-due.
  * Two tenants keep completing work.

  docker compose -f docker-compose.yml -f docker-compose.coordinator-handoff.yml \
    up --build --abort-on-container-exit --exit-code-from handoff-soak handoff-soak
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
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("coordinator_handoff_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
MAX_HANDOFF = float(os.environ.get("MAX_HANDOFF_SECONDS", "5") or "5")
TTL = float(os.environ.get("EXPECT_LEADER_TTL", "30") or "30")
_rr = 0


def http_one(
    base: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 15.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        base + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return int(resp.status), json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read() if exc.fp else b""
        try:
            payload = json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            payload = {"error": raw.decode(errors="replace")}
        return int(exc.code), payload


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 15.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        try:
            code, payload = http_one(base, method, path, body, timeout=timeout)
            if 500 <= code < 600:
                last = RuntimeError(f"{base}{path} -> {code}: {payload}")
                continue
            return code, payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        ready = 0
        for base in COORDS:
            try:
                code, h = http_one(base, "GET", "/health", timeout=3)
                if code == 200 and h.get("ok"):
                    ready += 1
            except Exception:  # noqa: BLE001
                pass
        if ready == len(COORDS):
            return
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def poll_health() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for base in COORDS:
        try:
            code, h = http_one(base, "GET", "/health", timeout=3)
            if code == 200:
                out.append({"base": base, **h})
        except Exception:  # noqa: BLE001
            continue
    return out


def unique_leader() -> dict[str, Any] | None:
    rows = poll_health()
    leads = [r for r in rows if r.get("leader")]
    if len(leads) != 1:
        return None
    return leads[0]


def wait_unique_leader(*, timeout: float = 20.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        lead = unique_leader()
        if lead is not None:
            return lead
        time.sleep(0.15)
    raise RuntimeError(f"no unique leader: {poll_health()}")


def wait_leader_node(node: str, *, timeout: float = 10.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        lead = unique_leader()
        if lead is not None and lead.get("node") == node:
            return lead
        time.sleep(0.1)
    raise RuntimeError(f"{node} never became leader: {poll_health()}")


def main() -> int:
    wait_healthy()
    tenants = ["ho-a", "ho-b"]
    logger.info("handoff soak coords=%s ttl=%.0fs", COORDS, TTL)

    before = wait_unique_leader()
    old = str(before.get("node") or "")
    others = [
        r.get("node")
        for r in poll_health()
        if r.get("node") and r.get("node") != old
    ]
    if not others:
        raise RuntimeError("need a follower to hand off to")
    dest = str(others[0])
    logger.info("handoff from %s to %s epoch=%s", old, dest, before.get("leader_epoch"))

    for tid in tenants:
        code, out = http_json(
            "POST",
            "/v1/schedules",
            {
                "tenant_id": tid,
                "name": f"cron-{tid}",
                "interval_seconds": 2,
                "payload": {"who": tid},
                "max_attempts": 1,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"schedule {tid} -> {code}: {out}")
        for i in range(4):
            code, out = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"op": "load", "i": i, "ms": 20},
                    "max_attempts": 1,
                },
            )
            if code not in (200, 201):
                raise RuntimeError(f"enqueue {tid} -> {code}: {out}")

    t0 = time.time()
    code, body = http_json(
        "POST",
        "/v1/coordinator/leader/handoff",
        {"to_node_id": dest, "handoff_seconds": 20},
    )
    if code != 200:
        raise RuntimeError(f"handoff -> {code}: {body}")
    if (body.get("handoff_to") or (body.get("lease") or {}).get("handoff_to")) not in (
        dest,
        None,
    ):
        # None if dest claimed in the same instant.
        logger.info("handoff response %s", body.get("handoff_to"))

    after = wait_leader_node(dest, timeout=MAX_HANDOFF)
    elapsed = time.time() - t0
    if elapsed >= TTL * 0.5:
        raise RuntimeError(
            f"handoff took {elapsed:.2f}s, expected well under TTL={TTL}"
        )
    logger.info(
        "handoff landed dest=%s epoch=%s in %.2fs",
        after.get("node"),
        after.get("leader_epoch"),
        elapsed,
    )

    # Old node must not still think it is leader, and must refuse process-due.
    for row in poll_health():
        if row.get("node") == old and row.get("leader"):
            raise RuntimeError(f"old leader still set: {row}")
    old_base = next(r["base"] for r in poll_health() if r.get("node") == old)
    code, out = http_one(
        old_base, "POST", "/v1/schedules/process-due", {"limit": 10}, timeout=10
    )
    if code != 503:
        raise RuntimeError(f"old process-due -> {code}: {out}")

    # New leader keeps ticking; tenants stay isolated.
    t1 = time.time()
    saw_jobs = {tid: False for tid in tenants}
    while time.time() - t1 < 20:
        if unique_leader() is None:
            raise RuntimeError(f"split brain after handoff: {poll_health()}")
        if unique_leader().get("node") != dest:
            raise RuntimeError(f"leader drifted: {poll_health()}")
        for tid in tenants:
            code, jobs = http_json(
                "GET", "/v1/jobs?" + urlencode({"tenant_id": tid, "limit": "50"})
            )
            rows = jobs.get("jobs") or [] if code == 200 else []
            if any(j.get("tenant_id") != tid for j in rows):
                raise RuntimeError(f"tenant leak {tid}")
            if rows:
                saw_jobs[tid] = True
        if all(saw_jobs.values()):
            break
        time.sleep(0.3)
    if not all(saw_jobs.values()):
        raise RuntimeError(f"missing tenant work after handoff: {saw_jobs}")

    logger.info(
        "coordinator handoff soak OK from=%s to=%s elapsed=%.2fs tenants=%s",
        old,
        dest,
        elapsed,
        len(tenants),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("coordinator-handoff soak failed")
        sys.exit(1)
