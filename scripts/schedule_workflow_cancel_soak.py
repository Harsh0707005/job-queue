#!/usr/bin/env python3
"""Fleet soak: cancelling a workflow tick fails its generation.

  * Each tenant pair has U (reserve → wait_signal) plus a waiter and join.
  * Fire U, cancel the live run, generation must go failed — waiter/join
    do not fire empty and stay blocked on that generation.
  * A later successful U tick unblocks the waiter/join on the new generation.
  * Other tenants keep completing through the churn.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.schedule-workflow-cancel.yml up --build \
    --abort-on-container-exit --exit-code-from sw-cancel-soak sw-cancel-soak
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
logger = logging.getLogger("schedule_workflow_cancel_soak")

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


def gen_status(sid: str, iteration: int, tenant: str) -> str | None:
    if not DSN:
        return None
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(DSN, row_factory=dict_row) as conn:
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
    pairs = [(f"swc-t{i}",) for i in range(PAIRS)]
    tenants = [p[0] for p in pairs]
    others = [f"swc-oth-{i}" for i in range(OTHERS)]
    logger.info("workflow-cancel soak tenants=%s others=%s", tenants, others)

    ups: dict[str, str] = {}
    waits: dict[str, str] = {}
    joins: dict[str, str] = {}
    runs: dict[str, str] = {}
    for tid in tenants:
        sig = f"go-{tid}"
        ups[tid] = create_sched(tid, f"up-{tid}", workflow=wf_body(sig))
        waits[tid] = create_sched(tid, f"w-{tid}", waits_on=[ups[tid]])
        joins[tid] = create_sched(tid, f"j-{tid}", joins_on=[waits[tid]])
        out = fire(tid, ups[tid])
        rid = str(out.get("workflow_run_id") or "")
        if not rid:
            raise RuntimeError(f"fire missing run: {out}")
        runs[tid] = rid

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
        fire(ot, oid)

    # Cancel once the first step is in or the wait is parked.
    t0 = time.time()
    cancelled: set[str] = set()
    while time.time() - t0 < 30:
        for tid in tenants:
            if tid in cancelled:
                continue
            rid = runs[tid]
            code, body = http_json(
                "GET",
                f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
            )
            steps = body.get("steps") or []
            if not steps:
                continue
            code, out = http_json(
                "POST",
                f"/v1/workflows/{rid}/cancel",
                {"tenant_id": tid, "compensate": False},
            )
            if code == 200:
                cancelled.add(tid)
                continue
            if code == 409:
                # Already completed — still a bug if gen sealed as success.
                raise RuntimeError(f"run completed before cancel {tid}: {out}")
        if len(cancelled) == len(tenants):
            break
        time.sleep(0.15)
    if cancelled != set(tenants):
        raise RuntimeError(f"did not cancel all runs: {cancelled}")

    for tid in tenants:
        st = get_run(tid, runs[tid]).get("status")
        if st != "cancelled":
            raise RuntimeError(f"{tid} run status {st} after cancel")
        g1 = gen_status(ups[tid], 1, tid)
        if g1 is not None and g1 != "failed":
            raise RuntimeError(f"{tid} gen 1 status {g1} want failed")
        code, out = http_json(
            "POST", f"/v1/schedules/{waits[tid]}/fire", {"tenant_id": tid}
        )
        if code not in (404, 409):
            raise RuntimeError(f"waiter fired after cancel {tid} -> {code}: {out}")
        code, out = http_json(
            "POST", f"/v1/schedules/{joins[tid]}/fire", {"tenant_id": tid}
        )
        if code not in (404, 409):
            raise RuntimeError(f"join fired after cancel {tid} -> {code}: {out}")

    # New successful tick unblocks waiters/joins.
    runs2: dict[str, str] = {}
    for tid in tenants:
        out = fire(tid, ups[tid])
        rid = str(out.get("workflow_run_id") or "")
        if not rid:
            raise RuntimeError(f"second fire missing run: {out}")
        runs2[tid] = rid

    t1 = time.time()
    sent: set[str] = set()
    waiter_done: set[str] = set()
    join_done: set[str] = set()
    while time.time() - t1 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 40})
        for tid in tenants:
            rid = runs2[tid]
            if tid not in sent:
                code, body = http_json(
                    "GET",
                    f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
                )
                gate = next(
                    (s for s in (body.get("steps") or []) if s.get("name") == "gate"),
                    None,
                )
                if gate and gate.get("status") == "awaiting_signal":
                    sc, sb = http_json(
                        "POST",
                        f"/v1/workflows/{rid}/signals",
                        {
                            "tenant_id": tid,
                            "name": f"go-{tid}",
                            "payload": {"tick": 2},
                        },
                    )
                    if sc not in (200, 201):
                        raise RuntimeError(f"send {tid} -> {sc}: {sb}")
                    sent.add(tid)
            if get_run(tid, rid).get("status") == "completed" and tid not in waiter_done:
                code, out = http_json(
                    "POST",
                    f"/v1/schedules/{waits[tid]}/fire",
                    {"tenant_id": tid},
                )
                if code in (200, 201):
                    job = out.get("job") or {}
                    up = (job.get("payload") or {}).get("upstream") or {}
                    if up.get("iteration") not in (2, None):
                        # payload may be redacted; still count as unblocked
                        logger.info("waiter payload iteration=%s", up.get("iteration"))
                    waiter_done.add(tid)
            if tid in waiter_done and tid not in join_done:
                # Waiter job must finish before join can fire.
                http_json("POST", "/v1/schedules/process-due", {"tenant_id": tid})
                code, out = http_json(
                    "POST",
                    f"/v1/schedules/{joins[tid]}/fire",
                    {"tenant_id": tid},
                )
                if code in (200, 201):
                    join_done.add(tid)
        if len(join_done) == len(tenants):
            break
        time.sleep(0.25)
    else:
        raise RuntimeError(
            f"timeout sent={len(sent)} waiters={len(waiter_done)} joins={len(join_done)}"
        )

    for tid in tenants:
        g1 = gen_status(ups[tid], 1, tid)
        g2 = gen_status(ups[tid], 2, tid)
        if g1 is not None and g1 != "failed":
            raise RuntimeError(f"{tid} gen 1 became {g1}")
        if g2 is not None and g2 != "completed":
            raise RuntimeError(f"{tid} gen 2 status {g2}")

    logger.info(
        "workflow cancel generation soak OK tenants=%s joins=%s",
        len(tenants),
        len(join_done),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow-cancel soak failed")
        sys.exit(1)
