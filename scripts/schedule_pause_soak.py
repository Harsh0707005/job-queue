#!/usr/bin/env python3
"""Fleet soak: pause one schedule, sibling and other tenants keep working.

Dual coordinators + dual schedulers + four workers:

  * sp-held   — paused after first tick; no more jobs while paused.
  * sp-sib    — same tenant as held; keeps firing.
  * sp-skip   — paused through several 2s ticks; resume+skip does not flood.
  * sp-one    — same window; resume+catch_up_one is one pickup.
  * sp-live   — other tenant keeps completing.
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
logger = logging.getLogger("schedule_pause_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD = int(os.environ.get("HOLD_SECONDS", "8") or "8")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "2") or "2")
CATCH_MAX = int(os.environ.get("CATCH_UP_MAX", "3") or "3")
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


def get_sched(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get {sid} {code} {body}")
    return body.get("schedule") or {}


def create_sched(tid: str, *, name: str, policy: str) -> str:
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": name,
            "cron": f"*/{CRON_EVERY} * * * * *",
            "payload": {"kind": "render", "ms": JOB_MS, "name": name},
            "misfire_policy": policy,
            "catch_up_max": CATCH_MAX,
        },
    )
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {body}")
    return str(body["schedule"]["id"])


def pause(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/pause", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"pause {tid} {code} {body}")


def resume(tid: str, sid: str) -> dict[str, Any]:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/resume", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"resume {tid} {code} {body}")
    return body.get("schedule") or {}


def wait_n(tid: str, n: int, *, timeout: float = 15.0) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        jobs = list_jobs(tid)
        if len(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(f"{tid} never reached {n} jobs (had {len(jobs)})")


def main() -> int:
    logger.info("schedule pause soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    held_t, skip_t, one_t, live_t = "sp-held", "sp-skip", "sp-one", "sp-live"
    held_id = create_sched(held_t, name="held", policy="catch_up_one")
    sib_id = create_sched(held_t, name="sib", policy="catch_up_one")
    skip_id = create_sched(skip_t, name="skip", policy="skip")
    one_id = create_sched(one_t, name="one", policy="catch_up_one")
    create_sched(live_t, name="live", policy="catch_up_one")

    wait_n(held_t, 1)
    wait_n(skip_t, 1)
    wait_n(one_t, 1)
    pause(held_t, held_id)
    pause(skip_t, skip_id)
    pause(one_t, one_id)
    held_at_pause = len([j for j in list_jobs(held_t) if j.get("schedule_id") == held_id])
    skip_at_pause = len(list_jobs(skip_t))
    one_at_pause = len(list_jobs(one_t))
    logger.info(
        "paused held=%d skip=%d one=%d",
        held_at_pause,
        skip_at_pause,
        one_at_pause,
    )

    time.sleep(HOLD)

    held_jobs = [j for j in list_jobs(held_t) if j.get("schedule_id") == held_id]
    if len(held_jobs) != held_at_pause:
        raise RuntimeError(
            f"held schedule enqueued while paused {len(held_jobs)} was {held_at_pause}"
        )
    sib_jobs = [j for j in list_jobs(held_t) if j.get("schedule_id") == sib_id]
    if len(sib_jobs) < 1:
        raise RuntimeError("sibling schedule did not fire while peer was paused")
    if len(list_jobs(skip_t)) != skip_at_pause:
        raise RuntimeError("skip schedule enqueued while paused")
    if len(list_jobs(one_t)) != one_at_pause:
        raise RuntimeError("one schedule enqueued while paused")
    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during pause window")
    logger.info(
        "during pause sib=%d live_completed=%d", len(sib_jobs), live_mid
    )

    resume(skip_t, skip_id)
    resume(one_t, one_id)
    resume(held_t, held_id)

    expected = max(1, HOLD // CRON_EVERY)
    deadline = time.time() + 15
    skip_sched: dict[str, Any] = {}
    while time.time() < deadline:
        skip_sched = get_sched(skip_id, skip_t)
        if int(skip_sched.get("missed_ticks") or 0) >= expected - 1:
            break
        time.sleep(0.15)
    skip_after = list_jobs(skip_t)
    extra_skip = len(skip_after) - skip_at_pause
    if extra_skip > 1:
        raise RuntimeError(f"skip flooded {extra_skip} jobs after resume")
    logger.info(
        "skip extra=%d missed=%s", extra_skip, skip_sched.get("missed_ticks")
    )

    # Freeze further ticks before counting catch_up_one pickup.
    time.sleep(1.5)
    pause(one_t, one_id)
    pause(skip_t, skip_id)
    pause(held_t, held_id)
    one_after = list_jobs(one_t)
    extra_one = len(one_after) - one_at_pause
    if extra_one < 1:
        raise RuntimeError("catch_up_one produced no pickup after resume")
    if extra_one > expected:
        raise RuntimeError(f"catch_up_one flooded {extra_one}")
    logger.info("catch_up_one extra=%d", extra_one)

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    logger.info("schedule pause soak OK live_completed=%d", live_after)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule pause soak failed")
        sys.exit(1)
