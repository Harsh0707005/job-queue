#!/usr/bin/env python3
"""Fleet soak: delete a schedule mid multi-step tick.

  Stage 1: delete cancels the live run; it is not left running.
  Stage 2: that tick's generation is not left pending.
  Stage 3: waiters and joins fail closed (409), other tenants keep working.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.schedule-workflow-delete.yml up --build \
    --abort-on-container-exit --exit-code-from sw-delete-soak sw-delete-soak
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
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schedule_workflow_delete_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
DSN = os.environ.get("JOB_QUEUE_DSN", "")
PAIRS = int(os.environ.get("PAIRS", "2") or "2")
OTHERS = int(os.environ.get("OTHERS", "2") or "2")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
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
                return int(resp.status), json.loads(raw.decode()) if raw else {}
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
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            code, h = http_json("GET", "/health", timeout=5)
            if code == 200 and h.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def future() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()


def create_sched(tid: str, name: str, **extra: Any) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "interval_seconds": 3600,
        "payload": {"who": name},
        "start_at": future(),
        "max_attempts": 1,
        "visibility_timeout_seconds": 8,
    }
    body.update(extra)
    code, out = http_json("POST", "/v1/schedules", body)
    if code not in (200, 201):
        raise RuntimeError(f"create {tid}/{name} -> {code}: {out}")
    sid = str((out.get("schedule") or {}).get("id") or "")
    if not sid:
        raise RuntimeError(f"no schedule id: {out}")
    return sid


def fire(tid: str, sid: str) -> dict[str, Any]:
    code, out = http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})
    if code not in (200, 201):
        raise RuntimeError(f"fire {tid} {sid} -> {code}: {out}")
    return out


def get_run(tid: str, rid: str) -> dict[str, Any]:
    code, out = http_json(
        "GET", f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return out.get("run") or {}


def pg_connect():
    import psycopg
    from psycopg.rows import dict_row

    if not DSN:
        raise RuntimeError("JOB_QUEUE_DSN is required")
    return psycopg.connect(DSN, row_factory=dict_row)


def gen_status(sid: str, iteration: int, tenant: str) -> str | None:
    with pg_connect() as conn:
        row = conn.execute(
            """
            SELECT status
            FROM schedule_generations
            WHERE schedule_id = %s AND iteration = %s AND tenant_id = %s
            """,
            (sid, iteration, tenant),
        ).fetchone()
    if row is None:
        return None
    return str(row["status"])


def pending_gens(sid: str) -> int:
    with pg_connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM schedule_generations
            WHERE schedule_id = %s AND status = 'pending'
            """,
            (sid,),
        ).fetchone()
    return int(row["n"] if row else 0)


def live_runs(sid: str, tenant: str) -> int:
    with pg_connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM workflow_runs
            WHERE schedule_id = %s
              AND tenant_id = %s
              AND status IN ('running', 'compensating')
            """,
            (sid, tenant),
        ).fetchone()
    return int(row["n"] if row else 0)


def inflight_jobs(sid: str) -> int:
    with pg_connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM jobs
            WHERE schedule_id = %s
              AND status = ANY(%s)
            """,
            (
                sid,
                [
                    "pending",
                    "leased",
                    "awaiting_approval",
                    "awaiting_children",
                    "awaiting_skills",
                    "frozen",
                ],
            ),
        ).fetchone()
    return int(row["n"] if row else 0)


def wf_body(signal: str) -> dict[str, Any]:
    return {
        "name": "tick-wf",
        "steps": [
            {
                "name": "reserve",
                "kind": "effect",
                "payload": {"op": "reserve", "ms": 40},
                "compensate": {
                    "kind": "effect",
                    "payload": {"op": "release", "ms": 15},
                },
            },
            {
                "name": "gate",
                "kind": "wait_signal",
                "signal": signal,
                "timeout_seconds": 90,
            },
            {
                "name": "ship",
                "kind": "effect",
                "payload": {"op": "ship", "ms": 25},
            },
        ],
    }


def enqueue_load(tid: str, n: int) -> None:
    for i in range(n):
        code, out = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"op": "load", "i": i, "ms": 15},
                "max_attempts": 1,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"load {tid} -> {code}: {out}")


def main() -> int:
    wait_healthy()
    tenants = [f"swd-t{i}" for i in range(PAIRS)]
    others = [f"swd-oth-{i}" for i in range(OTHERS)]
    logger.info("workflow-delete soak tenants=%s others=%s", tenants, others)

    ups: dict[str, str] = {}
    waits: dict[str, str] = {}
    joins: dict[str, str] = {}
    siblings: dict[str, str] = {}
    runs: dict[str, str] = {}
    for tid in tenants:
        sig = f"go-{tid}"
        ups[tid] = create_sched(tid, f"up-{tid}", workflow=wf_body(sig))
        waits[tid] = create_sched(tid, f"w-{tid}", waits_on=[ups[tid]])
        joins[tid] = create_sched(tid, f"j-{tid}", joins_on=[waits[tid]])
        siblings[tid] = create_sched(
            tid,
            f"sib-{tid}",
            workflow={
                "name": "sib",
                "steps": [
                    {
                        "name": "only",
                        "kind": "effect",
                        "payload": {"op": "sib", "ms": 20},
                    }
                ],
            },
        )
        out = fire(tid, ups[tid])
        rid = str(out.get("workflow_run_id") or "")
        if not rid:
            raise RuntimeError(f"fire missing run: {out}")
        runs[tid] = rid

    other_runs: dict[str, str] = {}
    for ot in others:
        enqueue_load(ot, 10)
        oid = create_sched(
            ot,
            f"plain-{ot}",
            workflow={
                "name": "plain",
                "steps": [
                    {
                        "name": "only",
                        "kind": "effect",
                        "payload": {"op": "only", "ms": 20},
                    }
                ],
            },
        )
        other_runs[ot] = str(fire(ot, oid).get("workflow_run_id") or "")

    # Wait until the first step is in or the wait is parked, then delete.
    t0 = time.time()
    mid: set[str] = set()
    while time.time() - t0 < 30:
        for tid in tenants:
            if tid in mid:
                continue
            rid = runs[tid]
            code, body = http_json(
                "GET",
                f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
            )
            steps = body.get("steps") or []
            names = {str(s.get("name")) for s in steps}
            parked = any(
                s.get("name") == "gate" and s.get("status") == "awaiting_signal"
                for s in steps
            )
            started = "reserve" in names or parked
            if not started:
                continue
            mid.add(tid)
        if len(mid) == len(tenants):
            break
        time.sleep(0.15)
    if mid != set(tenants):
        raise RuntimeError(f"ticks never reached mid-run: {mid}")

    deleted: set[str] = set()
    for tid in tenants:
        sid = ups[tid]
        code, out = http_json(
            "POST",
            f"/v1/schedules/{sid}/delete",
            {"tenant_id": tid},
        )
        if code != 200:
            raise RuntimeError(f"delete {tid} -> {code}: {out}")
        deleted.add(tid)

    # Stage 1: runs cancelled, not left live; no inflight tick jobs.
    for tid in tenants:
        st = get_run(tid, runs[tid]).get("status")
        if st != "cancelled":
            raise RuntimeError(f"stage1 {tid} run status {st} want cancelled")
        if live_runs(ups[tid], tid) != 0:
            raise RuntimeError(f"stage1 {tid} still has a live run")
        if inflight_jobs(ups[tid]) != 0:
            raise RuntimeError(f"stage1 {tid} still has inflight jobs")
        code, out = http_json(
            "GET",
            f"/v1/schedules/{ups[tid]}?" + urlencode({"tenant_id": tid}),
        )
        if code != 404:
            raise RuntimeError(f"stage1 {tid} schedule still present -> {code}")
    logger.info("stage1 ok: deleted mid-tick runs cancelled tenants=%s", len(deleted))

    # Stage 2: no pending generation hanging for the deleted tick.
    for tid in tenants:
        g1 = gen_status(ups[tid], 1, tid)
        if g1 == "pending":
            raise RuntimeError(f"stage2 {tid} generation 1 still pending")
        if pending_gens(ups[tid]) != 0:
            raise RuntimeError(f"stage2 {tid} leftover pending generations")
    logger.info("stage2 ok: no pending generations")

    # Stage 3: waiters/joins fail closed; sibling and other tenants proceed.
    for tid in tenants:
        code, out = http_json(
            "POST", f"/v1/schedules/{waits[tid]}/fire", {"tenant_id": tid}
        )
        if code not in (404, 409):
            raise RuntimeError(f"stage3 waiter fired {tid} -> {code}: {out}")
        code, out = http_json(
            "POST", f"/v1/schedules/{joins[tid]}/fire", {"tenant_id": tid}
        )
        if code not in (404, 409):
            raise RuntimeError(f"stage3 join fired {tid} -> {code}: {out}")
        code, out = http_json(
            "POST",
            f"/v1/schedules/{siblings[tid]}/fire",
            {"tenant_id": tid},
        )
        if code not in (200, 201):
            raise RuntimeError(f"stage3 sibling {tid} -> {code}: {out}")

    t1 = time.time()
    sib_done: set[str] = set()
    other_done: set[str] = set()
    while time.time() - t1 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 40})
        for tid in tenants:
            if tid in sib_done:
                continue
            code, body = http_json(
                "GET",
                f"/v1/schedules/{siblings[tid]}?" + urlencode({"tenant_id": tid}),
            )
            sched = body.get("schedule") or {}
            if sched.get("last_completed_iteration") == 1:
                sib_done.add(tid)
        for ot in others:
            if ot in other_done:
                continue
            rid = other_runs.get(ot) or ""
            if rid and get_run(ot, rid).get("status") == "completed":
                other_done.add(ot)
        if len(sib_done) == len(tenants) and len(other_done) == len(others):
            break
        time.sleep(0.25)
    else:
        raise RuntimeError(
            f"timeout siblings={len(sib_done)} others={len(other_done)}"
        )

    logger.info(
        "workflow delete tick soak OK tenants=%s siblings=%s others=%s",
        len(tenants),
        len(sib_done),
        len(other_done),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow-delete soak failed")
        sys.exit(1)
