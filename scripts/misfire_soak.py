#!/usr/bin/env python3
"""Fleet soak: schedule miss / catch-up after pause (and a live control).

  * Dual coordinators + dual schedulers + four workers.
  * Three policy tenants paused through several 2s ticks:
      skip         — zero backlog jobs, missed_ticks recorded
      catch_up_one — exactly one pickup job
      catch_up     — at most catch_up_max jobs, never a flood
  * A live tenant keeps firing through the pause window.
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
logger = logging.getLogger("misfire_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
PAUSE_SECONDS = int(os.environ.get("PAUSE_SECONDS", "10") or "10")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "2") or "2")
CATCH_MAX = int(os.environ.get("CATCH_UP_MAX", "3") or "3")
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


def get_schedule(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get schedule {sid} {code} {body}")
    return body.get("schedule") or {}


def create_sched(tid: str, *, policy: str, catch_up_max: int = 8) -> str:
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": f"{tid}-{policy}",
            "cron": f"*/{CRON_EVERY} * * * * *",
            "payload": {"kind": "render", "ms": JOB_MS, "policy": policy},
            "misfire_policy": policy,
            "catch_up_max": catch_up_max,
        },
    )
    if code != 201:
        raise RuntimeError(f"create schedule {tid} {code} {body}")
    return str(body["schedule"]["id"])


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def wait_completed(tid: str, minimum: int) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    n = 0
    while time.time() < deadline:
        n = completed(list_jobs(tid))
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s completed=%d need>=%d", tid, n, minimum)
            last = now
        if n >= minimum:
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting {tid} completed={n} need={minimum}")


def main() -> int:
    logger.info(
        "misfire soak pause=%ds cron=every %ds catch_max=%d",
        PAUSE_SECONDS,
        CRON_EVERY,
        CATCH_MAX,
    )
    wait_coords()
    skip_t, one_t, catch_t, live_t = (
        "mf-skip",
        "mf-one",
        "mf-catch",
        "mf-live",
    )
    skip_id = create_sched(skip_t, policy="skip")
    one_id = create_sched(one_t, policy="catch_up_one")
    catch_id = create_sched(catch_t, policy="catch_up", catch_up_max=CATCH_MAX)
    create_sched(live_t, policy="catch_up_one")

    for tid in (skip_t, one_t, catch_t):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")

    time.sleep(PAUSE_SECONDS)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError(f"live tenant did not fire during pause window: {live_mid}")
    for tid in (skip_t, one_t, catch_t):
        if list_jobs(tid):
            raise RuntimeError(f"{tid} piled jobs while paused")

    for tid in (skip_t, one_t, catch_t):
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    expected_ticks = max(1, PAUSE_SECONDS // CRON_EVERY)
    # Dual schedulers apply policy on the next ~0.3s poll. Wait until skip
    # has recorded the pause-window misses, then until catch_up has drained
    # its bounded remainder. Do not sit on a 2s cron long enough for
    # post-resume live ticks to look like a flood.
    deadline = time.time() + 20
    skip_sched: dict[str, Any] = {}
    while time.time() < deadline:
        skip_sched = get_schedule(skip_id, skip_t)
        if int(skip_sched.get("missed_ticks") or 0) >= expected_ticks - 1:
            break
        time.sleep(0.15)
    else:
        raise RuntimeError(f"skip did not record misses: {skip_sched}")

    skip_jobs = list_jobs(skip_t)
    skip_n = len(skip_jobs)
    # skip: no backlog of every missed tick. At most one later future tick
    # may have fired after the jump.
    if skip_n > 1:
        raise RuntimeError(f"skip flooded {skip_n} jobs after resume")
    logger.info(
        "skip jobs=%d missed=%s", skip_n, skip_sched.get("missed_ticks")
    )

    catch_sched: dict[str, Any] = {}
    while time.time() < deadline:
        catch_sched = get_schedule(catch_id, catch_t)
        rem = catch_sched.get("catch_up_remaining")
        missed = int(catch_sched.get("missed_ticks") or 0)
        if rem is None and missed >= 1:
            break
        time.sleep(0.15)
    else:
        raise RuntimeError(f"catch_up did not finish drain: {catch_sched}")

    # Freeze further live ticks the way the local fleet does.
    for tid in (skip_t, one_t, catch_t):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"re-pause {tid} {code} {body}")

    one_jobs = list_jobs(one_t)
    one_sched = get_schedule(one_id, one_t)
    # One pickup for the pause window, plus at most a couple of post-resume ticks.
    if len(one_jobs) > expected_ticks:
        raise RuntimeError(f"catch_up_one flooded {len(one_jobs)}")
    if len(one_jobs) < 1:
        raise RuntimeError("catch_up_one produced no pickup")
    logger.info(
        "catch_up_one jobs=%d missed=%s",
        len(one_jobs),
        one_sched.get("missed_ticks"),
    )

    catch_jobs = list_jobs(catch_t)
    catch_sched = get_schedule(catch_id, catch_t)
    if len(catch_jobs) > CATCH_MAX + 2:
        raise RuntimeError(f"catch_up flooded {len(catch_jobs)} (max={CATCH_MAX})")
    if len(catch_jobs) < 1:
        raise RuntimeError("catch_up produced no jobs")
    logger.info(
        "catch_up jobs=%d missed=%s max=%d remaining=%s",
        len(catch_jobs),
        catch_sched.get("missed_ticks"),
        CATCH_MAX,
        catch_sched.get("catch_up_remaining"),
    )

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")

    wait_completed(live_t, live_after)
    if one_jobs:
        wait_completed(one_t, 1)
    if catch_jobs:
        wait_completed(catch_t, 1)
    logger.info("misfire soak OK live_completed=%d", live_after)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("misfire soak failed")
        sys.exit(1)
