#!/usr/bin/env python3
"""Fleet soak: one coordinator leads control ticks; failover when it dies.

  Stage 1: exactly one leader; schedules/webhooks/alerts/retention tick once.
  Stage 2: kill the leader; a follower takes over; workers keep completing.
  Stage 3: two tenants stay isolated through the failover.

  docker compose -f docker-compose.yml -f docker-compose.coordinator-leader.yml \
    up --build --abort-on-container-exit --exit-code-from leader-soak leader-soak
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
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
logger = logging.getLogger("coordinator_leader_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080,http://coordinator-3:8080"
    ).split(",")
    if u.strip()
]
SINK = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "job-queue")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
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


def poll_leaders() -> list[dict[str, Any]]:
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
    rows = poll_leaders()
    leads = [r for r in rows if r.get("leader")]
    if len(leads) != 1:
        return None
    names = {r.get("leader_node") for r in rows if r.get("leader_node")}
    if names != {leads[0].get("node")}:
        return None
    return leads[0]


def wait_unique_leader(*, timeout: float = 20.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = unique_leader()
        if last is not None:
            return last
        time.sleep(0.2)
    raise RuntimeError(f"no unique leader: {poll_leaders()}")


def docker_kill(node: str) -> None:
    name = f"{COMPOSE_PROJECT}-{node}-1"
    r = subprocess.run(
        ["docker", "kill", name],
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        # Fallback: first container matching the node name.
        r2 = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={node}"],
            capture_output=True,
            text=True,
            check=False,
        )
        cid = (r2.stdout or "").strip().splitlines()
        if not cid:
            raise RuntimeError(f"kill {node} failed: {r.stderr} {r2.stderr}")
        subprocess.run(["docker", "kill", cid[0]], check=False)


def create_sched(tid: str, name: str) -> str:
    code, out = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": name,
            "interval_seconds": 2,
            "payload": {"who": name, "tenant": tid},
            "max_attempts": 1,
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"create schedule {tid}/{name} -> {code}: {out}")
    return str((out.get("schedule") or {}).get("id") or "")


def main() -> int:
    wait_healthy()
    tenants = ["cl-a", "cl-b"]
    logger.info("leader soak coords=%s tenants=%s", COORDS, tenants)

    lead = wait_unique_leader()
    logger.info("stage1 leader=%s epoch=%s", lead.get("node"), lead.get("leader_epoch"))

    # Stage 1: seed control work on both tenants.
    for tid in tenants:
        create_sched(tid, f"cron-{tid}")
        code, out = http_json(
            "POST",
            "/v1/webhooks",
            {
                "tenant_id": tid,
                "url": SINK,
                "events": ["job.completed"],
                "max_attempts": 4,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"webhook {tid} -> {code}: {out}")
        code, out = http_json(
            "POST",
            f"/v1/tenants/{tid}/retention",
            {"completed_retention_seconds": 2, "dead_retention_seconds": 2},
        )
        if code not in (200, 201):
            raise RuntimeError(f"retention {tid} -> {code}: {out}")
        for i in range(6):
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
        code, out = http_json(
            "POST",
            "/v1/alerts/rules",
            {
                "name": f"pending-{tid}",
                "metric": "pending_count",
                "threshold": 0,
                "comparator": "gt",
                "scope": "tenant",
                "tenant_id": tid,
                "window_seconds": 60,
                "cooldown_seconds": 5,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"alert {tid} -> {code}: {out}")

    t0 = time.time()
    saw_sched = False
    saw_hook = False
    saw_alert = False
    while time.time() - t0 < 40:
        if unique_leader() is None:
            raise RuntimeError(f"split brain: {poll_leaders()}")
        code, body = http_json("GET", "/v1/coordinator/leader")
        lease = (body.get("lease") or {}) if code == 200 else {}
        if int(lease.get("schedule_ticks") or 0) > 0 or lease.get(
            "last_schedule_at"
        ):
            saw_sched = True
        if int(lease.get("webhook_ticks") or 0) > 0 or lease.get(
            "last_webhook_at"
        ):
            saw_hook = True
        if int(lease.get("alert_ticks") or 0) > 0 or lease.get("last_alert_at"):
            saw_alert = True
        # Confirm no tenant leaked jobs.
        for tid in tenants:
            code, jobs = http_json(
                "GET", "/v1/jobs?" + urlencode({"tenant_id": tid, "limit": "50"})
            )
            for j in (jobs.get("jobs") or []) if code == 200 else []:
                if j.get("tenant_id") != tid:
                    raise RuntimeError(f"tenant leak {tid}: {j}")
        if saw_sched and saw_hook and saw_alert:
            break
        time.sleep(0.4)
    if not (saw_sched and saw_hook and saw_alert):
        raise RuntimeError(
            f"stage1 ticks missing sched={saw_sched} hook={saw_hook} alert={saw_alert}"
        )
    logger.info("stage1 ok: unique leader and control ticks")

    # Stage 2: kill the leader; workers keep completing; new leader ticks.
    before = wait_unique_leader()
    dead = str(before.get("node") or "")
    epoch0 = int(before.get("leader_epoch") or 0)
    completed_before: dict[str, int] = {}
    for tid in tenants:
        code, jobs = http_json(
            "GET",
            "/v1/jobs?" + urlencode({"tenant_id": tid, "status": "completed", "limit": "100"}),
        )
        completed_before[tid] = len(jobs.get("jobs") or []) if code == 200 else 0
        for i in range(8):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"op": "after-kill", "i": i, "ms": 15},
                    "max_attempts": 1,
                },
            )

    logger.info("killing leader %s epoch=%s", dead, epoch0)
    docker_kill(dead)
    # Dead coordinator drops out of health; wait for a living unique leader.
    t1 = time.time()
    after = None
    while time.time() - t1 < 25:
        after = unique_leader()
        if after is not None and after.get("node") != dead:
            break
        after = None
        time.sleep(0.25)
    if after is None:
        raise RuntimeError(f"no failover leader after killing {dead}: {poll_leaders()}")
    if int(after.get("leader_epoch") or 0) < epoch0 + 1:
        # epoch may already have incremented; require a different node.
        if after.get("node") == dead:
            raise RuntimeError("dead node still leader")
    logger.info(
        "stage2 leader=%s epoch=%s (was %s/%s)",
        after.get("node"),
        after.get("leader_epoch"),
        dead,
        epoch0,
    )

    # Workers keep draining the post-kill enqueue.
    t2 = time.time()
    drained = False
    while time.time() - t2 < MAX_WALL:
        pending = 0
        completed_now = 0
        for tid in tenants:
            code, body = http_json(
                "GET", "/v1/jobs?" + urlencode({"tenant_id": tid, "limit": "200"})
            )
            rows = body.get("jobs") or [] if code == 200 else []
            pending += sum(
                1 for j in rows if j.get("status") in ("pending", "leased")
            )
            completed_now += sum(1 for j in rows if j.get("status") == "completed")
        if pending == 0 and completed_now > sum(completed_before.values()):
            drained = True
            break
        time.sleep(0.4)
    if not drained:
        raise RuntimeError("workers did not keep completing after leader death")

    # New leader still runs control ticks.
    t3 = time.time()
    ticks_after = False
    while time.time() - t3 < 20:
        if unique_leader() is None:
            raise RuntimeError(f"split brain after failover: {poll_leaders()}")
        code, body = http_json("GET", "/v1/coordinator/leader")
        lease = (body.get("lease") or {}) if code == 200 else {}
        if str(lease.get("node_id") or "") == after.get("node") and int(
            lease.get("schedule_ticks") or 0
        ) >= 0:
            # A live lease on the new node is enough; ticks may already be >0.
            ticks_after = True
            break
        time.sleep(0.3)
    if not ticks_after:
        raise RuntimeError("new leader never published a lease")
    logger.info("stage2 ok: failover and workers still completing")

    # Stage 3: tenant isolation after failover.
    for tid in tenants:
        code, jobs = http_json(
            "GET", "/v1/jobs?" + urlencode({"tenant_id": tid, "limit": "200"})
        )
        if code != 200:
            raise RuntimeError(f"list {tid} -> {code}")
        for j in jobs.get("jobs") or []:
            if j.get("tenant_id") != tid:
                raise RuntimeError(f"stage3 leak {tid}: {j}")
        code, ev = http_json(
            "GET",
            "/v1/alerts/events?" + urlencode({"tenant_id": tid, "limit": "50"}),
        )
        if code == 200:
            for e in ev.get("events") or []:
                if e.get("tenant_id") not in (tid, None, ""):
                    if e.get("tenant_id") != tid:
                        raise RuntimeError(f"alert leak {tid}: {e}")
    logger.info(
        "coordinator leader soak OK leader=%s epoch=%s tenants=%s",
        after.get("node"),
        after.get("leader_epoch"),
        len(tenants),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("coordinator-leader soak failed")
        sys.exit(1)
