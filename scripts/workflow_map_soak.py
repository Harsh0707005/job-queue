#!/usr/bin/env python3
"""Fleet soak: dynamic workflow map fan-out.

  * Stage 1: a step result spawns N children, join, continue.
  * Stage 2: cancel / fail mid-fan-out settles cleanly (no extra join).
  * Stage 3: a huge items list is capped — it must not explode the fleet.
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
logger = logging.getLogger("workflow_map_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240"))
TENANTS = int(os.environ.get("MAP_TENANTS", "3") or "3")
HAPPY = int(os.environ.get("MAP_HAPPY", "3") or "3")
ITEMS = int(os.environ.get("MAP_ITEMS", "6") or "6")
CANCEL_N = int(os.environ.get("MAP_CANCEL", "2") or "2")
FAIL_N = int(os.environ.get("MAP_FAIL", "2") or "2")
HUGE = int(os.environ.get("MAP_HUGE_ITEMS", "4000") or "4000")
CAP = int(os.environ.get("MAP_MAX_ITEMS", "8") or "8")
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
        targets = [b + path for b in (COORDS[start:] + COORDS[:start])]  # type: ignore[operator]
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
                payload = {"error": raw.decode("utf-8", errors="replace")}
            payload["_http"] = int(exc.code)
            return payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def start_wf(tenant_id: str, name: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    out = http_json(
        "POST",
        "/v1/workflows",
        {
            "tenant_id": tenant_id,
            "name": name,
            "max_attempts": 1,
            "steps": steps,
        },
    )
    run = out.get("run") if isinstance(out.get("run"), dict) else out
    if not run or not run.get("id"):
        raise RuntimeError(f"start_workflow failed: {out}")
    return run


def get_wf(run_id: str, tenant_id: str) -> dict[str, Any]:
    out = http_json("GET", f"/v1/workflows/{run_id}?tenant_id={tenant_id}")
    return out.get("run") if isinstance(out.get("run"), dict) else out


def list_steps(run_id: str, tenant_id: str) -> list[dict[str, Any]]:
    out = http_json("GET", f"/v1/workflows/{run_id}/steps?tenant_id={tenant_id}")
    return list(out.get("steps") or [])


def list_jobs(tenant_id: str) -> list[dict[str, Any]]:
    out = http_json("GET", f"/v1/jobs?tenant_id={tenant_id}&limit=500")
    return list(out.get("jobs") or [])


def cancel_wf(run_id: str, tenant_id: str) -> None:
    last: dict[str, Any] | None = None
    for _ in range(8):
        out = http_json(
            "POST",
            f"/v1/workflows/{run_id}/cancel",
            {
                "tenant_id": tenant_id,
                "compensate": False,
                "release_in_flight": False,
            },
        )
        last = out
        code = int(out.get("_http") or 200)
        if code < 400:
            return
        if code == 409 and "completed" in str(out.get("error") or ""):
            raise RuntimeError(f"cancel raced to completion: {out}")
        # Concurrent child nack can race the cancel lock; retry.
        time.sleep(0.15)
    raise RuntimeError(f"cancel failed: {last}")


def happy_steps(n: int, *, child_ms: int = 60) -> list[dict[str, Any]]:
    return [
        {
            "name": "list",
            "kind": "effect",
            "payload": {"op": "list", "ms": 40, "items": list(range(n))},
        },
        {
            "type": "map",
            "name": "each",
            "items": "items",
            "max_items": max(n, 1),
            "step": {
                "name": "work",
                "kind": "effect",
                "payload": {"op": "work", "ms": child_ms},
                "compensate": {"kind": "effect", "payload": {"op": "undo", "ms": 20}},
            },
        },
        {
            "name": "join",
            "kind": "effect",
            "payload": {"op": "join", "ms": 30},
        },
    ]


def fail_steps(n: int) -> list[dict[str, Any]]:
    items = list(range(n))
    items[0] = "boom"
    return [
        {
            "name": "list",
            "kind": "effect",
            "payload": {"op": "list", "ms": 30, "items": items},
        },
        {
            "type": "map",
            "name": "each",
            "items": "items",
            "max_items": n,
            "step": {
                "name": "work",
                "kind": "effect",
                "payload": {"op": "work", "ms": 40, "fail_item": "boom"},
                "compensate": {"kind": "effect", "payload": {"op": "undo", "ms": 15}},
            },
        },
        {"name": "join", "kind": "effect", "payload": {"op": "join", "ms": 20}},
    ]


def huge_steps(n: int, cap: int) -> list[dict[str, Any]]:
    return [
        {
            "name": "list",
            "kind": "effect",
            "payload": {"op": "list", "ms": 20, "items": list(range(n))},
        },
        {
            "type": "map",
            "name": "each",
            "items": "items",
            "max_items": cap,
            "step": {
                "name": "work",
                "kind": "effect",
                "payload": {"op": "work", "ms": 20},
            },
        },
    ]


def wait_until(pred, *, timeout: float, msg: str, interval: float = 0.25) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return
        time.sleep(interval)
    raise RuntimeError(f"{msg} (last={last})")


def main() -> int:
    started = time.time()
    logger.info("workflow map soak coords=%s", COORDS)
    wait_coords()

    tenants = [f"map-{i:02d}" for i in range(TENANTS)]
    happy_ids: list[tuple[str, str]] = []
    logger.info("stage1: happy map fan-out items=%d x %d", ITEMS, HAPPY * TENANTS)
    for tid in tenants:
        for n in range(HAPPY):
            run = start_wf(tid, f"happy-{n}", happy_steps(ITEMS))
            happy_ids.append((tid, str(run["id"])))

    def _happy_done() -> bool:
        leftover = []
        for tid, rid in happy_ids:
            run = get_wf(rid, tid)
            if run.get("status") != "completed":
                leftover.append(f"{tid}:{rid}:{run.get('status')}")
        if leftover:
            logger.info("stage1 leftover %d e.g. %s", len(leftover), leftover[:4])
            return False
        return True

    wait_until(
        _happy_done,
        timeout=max(40.0, MAX_WALL / 3),
        msg="happy map workflows did not complete",
    )
    for tid, rid in happy_ids:
        steps = list_steps(rid, tid)
        kids = [
            s
            for s in steps
            if s.get("direction") == "forward"
            and str(s.get("name") or "").startswith("work-")
        ]
        if len(kids) != ITEMS:
            raise RuntimeError(f"{rid} children={len(kids)} want {ITEMS}")
        if any(s.get("status") != "completed" for s in kids):
            raise RuntimeError(f"{rid} child not completed")
    logger.info("stage1 ok runs=%d", len(happy_ids))

    logger.info("stage2: cancel + fail mid-fan-out")
    cancel_ids: list[tuple[str, str]] = []
    fail_ids: list[tuple[str, str]] = []
    slow_items = max(ITEMS, 8)
    for i in range(CANCEL_N):
        tid = tenants[i % len(tenants)]
        run = start_wf(tid, f"cancel-{i}", happy_steps(slow_items, child_ms=400))
        cancel_ids.append((tid, str(run["id"])))
    for i in range(FAIL_N):
        tid = tenants[i % len(tenants)]
        run = start_wf(tid, f"fail-{i}", fail_steps(slow_items))
        fail_ids.append((tid, str(run["id"])))

    def _has_children(tid: str, rid: str) -> bool:
        return any(
            str(s.get("name") or "").startswith("work-")
            for s in list_steps(rid, tid)
        )

    for tid, rid in cancel_ids:
        wait_until(
            lambda t=tid, r=rid: _has_children(t, r),
            timeout=15.0,
            msg=f"cancel target {rid} never spawned children",
        )
        st = get_wf(rid, tid).get("status")
        if st == "completed":
            raise RuntimeError(f"cancel target {rid} finished before cancel")
        cancel_wf(rid, tid)

    def _stage2_done() -> bool:
        leftover = []
        for tid, rid in cancel_ids:
            run = get_wf(rid, tid)
            if run.get("status") not in ("cancelled", "compensating"):
                leftover.append(f"c:{tid}:{rid}:{run.get('status')}")
        for tid, rid in fail_ids:
            run = get_wf(rid, tid)
            if run.get("status") not in ("failed", "compensating"):
                leftover.append(f"f:{tid}:{rid}:{run.get('status')}")
        if leftover:
            logger.info("stage2 leftover %s", leftover[:6])
            return False
        return True

    wait_until(
        _stage2_done,
        timeout=max(40.0, MAX_WALL / 3),
        msg="mid-fan-out cancel/fail did not settle",
    )
    for tid, rid in cancel_ids + fail_ids:
        steps = list_steps(rid, tid)
        if any(
            s.get("name") == "join" and s.get("status") == "completed"
            for s in steps
        ):
            raise RuntimeError(f"{rid} join ran after cancel/fail")
    logger.info("stage2 ok cancel=%d fail=%d", len(cancel_ids), len(fail_ids))

    logger.info("stage3: huge result %d items cap=%d", HUGE, CAP)
    huge_tid = "map-huge"
    huge = start_wf(huge_tid, "huge", huge_steps(HUGE, CAP))
    hid = str(huge["id"])

    def _huge_settled() -> bool:
        run = get_wf(hid, huge_tid)
        return run.get("status") in ("failed", "compensating")

    wait_until(
        _huge_settled,
        timeout=30.0,
        msg="huge map did not fail closed",
    )
    jobs = list_jobs(huge_tid)
    work_jobs = [
        j
        for j in jobs
        if "work-" in str((j.get("payload") or {}).get("step_name") or j.get("labels"))
        or any(str(x).startswith("wf-step:") and str(x) != "wf-step:0" for x in (j.get("labels") or []))
    ]
    # Producer + maybe nothing else. Hard fail if we enqueued anywhere near HUGE.
    if len(jobs) > CAP + 8:
        raise RuntimeError(f"huge map exploded fleet: jobs={len(jobs)}")
    steps = list_steps(hid, huge_tid)
    if any(str(s.get("name") or "").startswith("work-") for s in steps):
        raise RuntimeError(f"huge map spawned children: {steps}")
    join = next((s for s in steps if s.get("name") == "each"), None)
    if join is None or join.get("status") != "failed":
        raise RuntimeError(f"huge map join not failed: {join}")
    logger.info("stage3 ok jobs=%d (capped)", len(jobs))

    logger.info(
        "workflow map soak ok in %.1fs happy=%d",
        time.time() - started,
        len(happy_ids),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow map soak failed")
        sys.exit(1)
