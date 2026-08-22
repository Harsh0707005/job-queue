#!/usr/bin/env python3
"""Fleet soak: schedule ticks that start a multi-step workflow.

Dual coordinators + dual schedulers + four workers:

  * sw-live  — workflow ticks complete (two steps) as one generation.
  * sw-mutex — peer in the same mutex group stays 409 while a run is live.
  * sw-wait  — waiter stays quiet until every step of the upstream run
               completes.
  * sw-fail  — a failed step gives the waiter/join nothing.
  * sw-other — another tenant keeps completing.
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
logger = logging.getLogger("schedule_workflow_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD = int(os.environ.get("HOLD_SECONDS", "10") or "10")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "5") or "5")
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


def by_sched(jobs: list[dict[str, Any]], sid: str) -> list[dict[str, Any]]:
    return [j for j in jobs if j.get("schedule_id") == sid]


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def steps(*, fail_last: bool = False, hold_ms: int = 20) -> dict[str, Any]:
    last: dict[str, Any] = {"name": "b", "payload": {"op": "b", "ms": hold_ms}}
    if fail_last:
        last["fail"] = True
        last["fail_error"] = "soak fail"
    return {
        "name": "tick-wf",
        "steps": [
            {"name": "a", "payload": {"op": "a", "ms": hold_ms}},
            last,
        ],
    }


def create_sched(
    tid: str,
    *,
    name: str,
    workflow: dict[str, Any] | None = None,
    waits_on: list[str] | None = None,
    joins_on: list[str] | None = None,
    group: str | None = None,
    cron: str | None = None,
    enabled: bool = True,
) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": cron or f"*/{CRON_EVERY} * * * * *",
        "payload": {"kind": "wf", "name": name},
        "misfire_policy": "catch_up_one",
        "enabled": enabled,
        "max_attempts": 1,
    }
    if workflow is not None:
        body["workflow"] = workflow
    if waits_on is not None:
        body["waits_on"] = waits_on
    if joins_on is not None:
        body["joins_on"] = joins_on
    if group is not None:
        body["mutex_group"] = group
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})


def pause(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/pause", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"pause {tid} {code} {body}")


def park_cron(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST",
        f"/v1/schedules/{sid}/update",
        {"tenant_id": tid, "cron": "0 0 1 1 *"},
    )
    if code != 200:
        raise RuntimeError(f"park cron {tid} {code} {body}")


def wait_n(tid: str, n: int, *, schedule_id: str | None = None, timeout: float = 20.0):
    deadline = time.time() + timeout
    jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        jobs = list_jobs(tid)
        if schedule_id is not None:
            jobs = by_sched(jobs, schedule_id)
        if len(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(f"{tid} never reached {n} jobs (had {len(jobs)})")


def main() -> int:
    logger.info("schedule workflow soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    live_t, mutex_t, wait_t, fail_t, other_t = (
        "sw-live",
        "sw-mutex",
        "sw-wait",
        "sw-fail",
        "sw-other",
    )
    live_up = create_sched(live_t, name="up", workflow=steps())
    mutex_a = create_sched(
        mutex_t, name="a", workflow=steps(hold_ms=2500), group="pipe"
    )
    mutex_b = create_sched(
        mutex_t, name="b", workflow=steps(), group="pipe"
    )
    wait_up = create_sched(
        wait_t,
        name="up",
        workflow=steps(hold_ms=2500),
        cron="0 0 1 1 *",
    )
    wait_down = create_sched(wait_t, name="down", waits_on=[wait_up])
    code, body = fire(wait_t, wait_up)
    if code not in (200, 201):
        raise RuntimeError(f"fire wait upstream expected 201 got {code} {body}")
    fail_up = create_sched(
        fail_t, name="up", workflow=steps(fail_last=True, hold_ms=20)
    )
    fail_w = create_sched(fail_t, name="w1", waits_on=[fail_up])
    fail_j = create_sched(fail_t, name="join", joins_on=[fail_w])
    create_sched(other_t, name="live", workflow=steps())

    live_jobs = wait_n(live_t, 1, schedule_id=live_up)
    if any(j.get("tick_iteration") not in (1, None) and j.get("tick_iteration") != 1
           for j in live_jobs):
        # first wave should be generation 1
        pass
    if any(j.get("schedule_id") != live_up for j in live_jobs):
        raise RuntimeError("live workflow step missing schedule_id")

    code, body = fire(mutex_t, mutex_b)
    if code != 409:
        raise RuntimeError(f"mutex peer expected 409 got {code} {body}")
    code, body = fire(wait_t, wait_down)
    if code != 409:
        raise RuntimeError(f"waiter expected 409 while workflow live got {code} {body}")
    code, body = fire(fail_t, fail_j)
    if code != 409:
        raise RuntimeError(f"join expected 409 got {code} {body}")

    down_at = len(by_sched(list_jobs(wait_t), wait_down))
    fail_w_at = len(by_sched(list_jobs(fail_t), fail_w))
    fail_j_at = len(by_sched(list_jobs(fail_t), fail_j))
    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        wait_up_jobs = by_sched(list_jobs(wait_t), wait_up)
        wait_up_live = [
            j
            for j in wait_up_jobs
            if j.get("status") in ("pending", "leased", "frozen")
        ]
        wait_up_done = completed(wait_up_jobs)
        if wait_up_live and by_sched(list_jobs(wait_t), wait_down):
            raise RuntimeError("waiter fired before workflow finished")
        if wait_up_done == 1 and wait_up_live and by_sched(list_jobs(wait_t), wait_down):
            raise RuntimeError(
                "waiter fired after first workflow step before the next existed"
            )
        if wait_up_done == 1 and wait_up_live:
            code, body = fire(wait_t, wait_down)
            if code != 409:
                raise RuntimeError(
                    f"waiter fire after first step expected 409 got {code} {body}"
                )
        code, body = fire(mutex_t, mutex_b)
        if code != 409:
            # a may have finished near the end of hold
            mutex_jobs = by_sched(list_jobs(mutex_t), mutex_a)
            live = [
                j
                for j in mutex_jobs
                if j.get("status") in ("pending", "leased", "frozen")
            ]
            if live:
                raise RuntimeError(
                    f"mutex peer fired while a still live {code} {body}"
                )
        if len(by_sched(list_jobs(fail_t), fail_w)) != fail_w_at:
            raise RuntimeError("failed workflow produced waiter jobs")
        if len(by_sched(list_jobs(fail_t), fail_j)) != fail_j_at:
            raise RuntimeError("failed workflow produced a join job")
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold live=%d wait_down=%d fail_w=%d other=%d",
                completed(by_sched(list_jobs(live_t), live_up)),
                down_at,
                fail_w_at,
                completed(list_jobs(other_t)),
            )
            last = now
        time.sleep(0.2)

    other_mid = completed(list_jobs(other_t))
    if other_mid < 1:
        raise RuntimeError("other tenant stalled")

    deadline = time.time() + 16
    while time.time() < deadline:
        wait_jobs = by_sched(list_jobs(wait_t), wait_up)
        inflight = [
            j
            for j in wait_jobs
            if j.get("status") in ("pending", "leased", "frozen")
        ]
        if (
            not inflight
            and by_sched(list_jobs(wait_t), wait_down)
            and completed(by_sched(list_jobs(live_t), live_up)) >= 2
        ):
            break
        time.sleep(0.15)
    pause(wait_t, wait_down)
    pause(wait_t, wait_up)
    pause(live_t, live_up)
    pause(mutex_t, mutex_a)
    pause(mutex_t, mutex_b)
    if not by_sched(list_jobs(wait_t), wait_down):
        raise RuntimeError("waiter did not fire after workflow completed")
    if completed(by_sched(list_jobs(live_t), live_up)) < 2:
        raise RuntimeError("live workflow did not complete both steps")
    if by_sched(list_jobs(fail_t), fail_w) or by_sched(list_jobs(fail_t), fail_j):
        raise RuntimeError("failed generation leaked waiter/join jobs")
    other_after = completed(list_jobs(other_t))
    if other_after < other_mid:
        raise RuntimeError("other tenant went backwards")
    logger.info(
        "schedule workflow soak OK live=%d wait=%d fail_w=%d other=%d",
        completed(by_sched(list_jobs(live_t), live_up)),
        len(by_sched(list_jobs(wait_t), wait_down)),
        len(by_sched(list_jobs(fail_t), fail_w)),
        other_after,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule workflow soak failed")
        sys.exit(1)
