#!/usr/bin/env python3
"""Fleet soak: downstream waits for an upstream tick / paused mutex peer.

Dual coordinators + dual schedulers + four workers:

  * sw-up / sw-skip  — freeze upstream; skip downstream does not dump.
  * sw-up / sw-one   — same window; catch_up_one is a single pickup.
  * sw-pipe          — upstream in mutex group 'pipe'; paused peer blocks
                       the waiter. Resume peer, waiter may fire once.
  * sw-live          — other tenant keeps completing.
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
logger = logging.getLogger("schedule_wait_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD = int(os.environ.get("HOLD_SECONDS", "8") or "8")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "2") or "2")
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
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


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def by_sched(jobs: list[dict[str, Any]], sid: str) -> list[dict[str, Any]]:
    return [j for j in jobs if j.get("schedule_id") == sid]


def get_sched(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get {sid} {code} {body}")
    return body.get("schedule") or {}


def create_sched(
    tid: str,
    *,
    name: str,
    group: str | None = None,
    waits_on: list[str] | None = None,
    policy: str = "catch_up_one",
    ms: int | None = None,
    enabled: bool = True,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{CRON_EVERY} * * * * *",
        "payload": {"kind": "render", "ms": job_ms, "name": name},
        "misfire_policy": policy,
        "catch_up_max": 3,
        "enabled": enabled,
    }
    if group is not None:
        body["mutex_group"] = group
    if waits_on is not None:
        body["waits_on"] = waits_on
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def pause(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/pause", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"pause {tid} {code} {body}")


def resume(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/resume", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"resume {tid} {code} {body}")


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})


def wait_n(
    tid: str,
    n: int,
    *,
    schedule_id: str | None = None,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
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
    logger.info("schedule wait soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    skip_t, one_t, pipe_t, live_t = "sw-skip", "sw-one", "sw-pipe", "sw-live"
    skip_up = create_sched(skip_t, name="up", ms=2500)
    skip_down = create_sched(
        skip_t, name="down", waits_on=[skip_up], policy="skip"
    )
    one_up = create_sched(one_t, name="up", ms=2500)
    one_down = create_sched(
        one_t, name="down", waits_on=[one_up], policy="catch_up_one"
    )
    pipe_up = create_sched(pipe_t, name="up", group="pipe", ms=2500)
    pipe_peer = create_sched(
        pipe_t, name="peer", group="pipe", enabled=False
    )
    pipe_down = create_sched(pipe_t, name="down", waits_on=[pipe_up])
    create_sched(live_t, name="live")

    skip_jobs = wait_n(skip_t, 1, schedule_id=skip_up)
    one_jobs = wait_n(one_t, 1, schedule_id=one_up)
    pipe_jobs = wait_n(pipe_t, 1, schedule_id=pipe_up)
    for tid, job, reason in (
        (skip_t, skip_jobs[0], "hold skip"),
        (one_t, one_jobs[0], "hold one"),
        (pipe_t, pipe_jobs[0], "hold pipe"),
    ):
        code, body = http_json(
            "POST",
            f"/v1/jobs/{job['id']}/freeze",
            {"tenant_id": tid, "reason": reason},
        )
        if code != 200:
            raise RuntimeError(f"freeze {tid} {code} {body}")

    skip_at = len(list_jobs(skip_t))
    one_at = len(list_jobs(one_t))
    pipe_at = len(by_sched(list_jobs(pipe_t), pipe_down))

    for tid, sid in ((skip_t, skip_down), (one_t, one_down), (pipe_t, pipe_down)):
        code, body = fire(tid, sid)
        if code != 409:
            raise RuntimeError(f"waiter fire {tid} expected 409 got {code} {body}")

    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        if len(list_jobs(skip_t)) != skip_at:
            raise RuntimeError("skip downstream fired while upstream live")
        if len(list_jobs(one_t)) != one_at:
            raise RuntimeError("catch_up_one downstream fired while upstream live")
        if len(by_sched(list_jobs(pipe_t), pipe_down)) != pipe_at:
            raise RuntimeError("pipe waiter fired while peer paused / up live")
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold skip=%d one=%d pipe_down=%d live=%d",
                len(list_jobs(skip_t)),
                len(list_jobs(one_t)),
                len(by_sched(list_jobs(pipe_t), pipe_down)),
                completed(list_jobs(live_t)),
            )
            last = now
        time.sleep(0.2)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during wait hold")

    # Unfreeze upstreams and let those ticks finish. Do not pause them
    # first — a paused upstream also blocks the waiter. Pipe peer stays
    # paused so pipe_down remains blocked after up goes idle.
    for tid, jid in (
        (skip_t, skip_jobs[0]["id"]),
        (one_t, one_jobs[0]["id"]),
        (pipe_t, pipe_jobs[0]["id"]),
    ):
        code, body = http_json(
            "POST", f"/v1/jobs/{jid}/unfreeze", {"tenant_id": tid}
        )
        if code != 200:
            raise RuntimeError(f"unfreeze {tid} {code} {body}")

    # Upstream ticks finish; skip/one waiters are overdue. Pipe peer is
    # still paused so pipe_down stays blocked.
    expected = max(1, HOLD // CRON_EVERY)
    deadline = time.time() + 10
    skip_sched: dict[str, Any] = {}
    extra_one = 0
    while time.time() < deadline:
        skip_sched = get_sched(skip_down, skip_t)
        extra_one = len(by_sched(list_jobs(one_t), one_down))
        missed = int(skip_sched.get("missed_ticks") or 0)
        if missed >= expected - 1 and extra_one >= 1:
            break
        time.sleep(0.15)
    if len(by_sched(list_jobs(pipe_t), pipe_down)) != pipe_at:
        raise RuntimeError("pipe waiter fired while mutex peer still paused")

    pause(skip_t, skip_down)
    pause(one_t, one_down)
    extra_skip = len(by_sched(list_jobs(skip_t), skip_down))
    if extra_skip > 1:
        raise RuntimeError(f"skip dumped {extra_skip} after upstream finished")
    extra_one = len(by_sched(list_jobs(one_t), one_down))
    if extra_one < 1:
        raise RuntimeError("catch_up_one produced no pickup after wait")
    if extra_one > expected:
        raise RuntimeError(f"catch_up_one flooded {extra_one}")

    resume(pipe_t, pipe_peer)
    resume(pipe_t, pipe_up)
    deadline = time.time() + 8
    while time.time() < deadline:
        if by_sched(list_jobs(pipe_t), pipe_down):
            break
        time.sleep(0.15)
    pause(pipe_t, pipe_down)
    pipe_extra = len(by_sched(list_jobs(pipe_t), pipe_down))
    if pipe_extra < 1:
        raise RuntimeError("pipe waiter did not fire after peer resume")
    if pipe_extra > expected:
        raise RuntimeError(f"pipe waiter flooded {pipe_extra}")

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    logger.info(
        "schedule wait soak OK extra_skip=%d extra_one=%d pipe=%d missed=%s live=%d",
        extra_skip,
        extra_one,
        pipe_extra,
        skip_sched.get("missed_ticks"),
        live_after,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule wait soak failed")
        sys.exit(1)
