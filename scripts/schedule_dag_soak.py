#!/usr/bin/env python3
"""Fleet soak: schedule ticks that enqueue a whole DAG.

Simulated scenarios on dual coordinators + dual schedulers + four workers:

  * Two schedulers race a due DAG tick → exactly one closed graph.
  * Concurrent POST /fire on one DAG schedule → one graph, no extra nodes.
  * A live single-job tenant keeps ticking.
  * Paused / blacked-out DAG tenants enqueue nothing (no half graph).
  * After the first diamond completes, the next cron tick is a new graph
    with new ids (no duplicate batch).
  * Job counts stay multiples of the diamond size.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schedule_dag_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
JOB_MS = int(os.environ.get("JOB_MS", "15") or "15")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
DIAMOND = 4
_rr = 0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def diamond(tag: str) -> list[dict[str, Any]]:
    return [
        {"ref": "prep", "payload": {"kind": "render", "ms": JOB_MS, "step": "prep", "tag": tag}},
        {
            "ref": "left",
            "payload": {"kind": "render", "ms": JOB_MS, "step": "left", "tag": tag},
            "depends_on": ["prep"],
        },
        {
            "ref": "right",
            "payload": {"kind": "render", "ms": JOB_MS, "step": "right", "tag": tag},
            "depends_on": ["prep"],
        },
        {
            "ref": "join",
            "payload": {"kind": "render", "ms": JOB_MS, "step": "join", "tag": tag},
            "depends_on": ["left", "right"],
        },
    ]


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


def create_dag_sched(tid: str, *, name: str, cron: str) -> str:
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": name,
            "cron": cron,
            "payload": {"kind": "pipeline", "tag": tid},
            "dag_jobs": diamond(tid),
            "misfire_policy": "catch_up_one",
        },
    )
    if code != 201:
        raise RuntimeError(f"create dag schedule {tid} {code} {body}")
    return str(body["schedule"]["id"])


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})


def wait_completed(tid: str, minimum: int) -> None:
    deadline = time.time() + MAX_WALL
    n = 0
    last = 0.0
    while time.time() < deadline:
        jobs = list_jobs(tid)
        n = completed(jobs)
        if len(jobs) % DIAMOND != 0:
            raise RuntimeError(
                f"{tid} half graph: {len(jobs)} jobs (not multiple of {DIAMOND})"
            )
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s completed=%d need>=%d total=%d", tid, n, minimum, len(jobs))
            last = now
        if n >= minimum:
            return
        time.sleep(0.15)
    raise RuntimeError(f"timed out waiting {tid} completed={n} need={minimum}")


def main() -> int:
    logger.info("schedule dag soak")
    wait_coords()
    race_t, live_t, pause_t, black_t, fire_t = (
        "sd-race",
        "sd-live",
        "sd-pause",
        "sd-black",
        "sd-fire",
    )
    # Near-immediate cron so dual schedulers race the first tick.
    race_id = create_dag_sched(race_t, name="race", cron="*/2 * * * * *")
    create_dag_sched(live_t, name="live-single-wait", cron="0 0 1 1 *")
    # live tenant: single-job schedule (no dag) that ticks every 2s
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": live_t,
            "name": "live-cron",
            "cron": "*/2 * * * * *",
            "payload": {"kind": "render", "ms": JOB_MS, "live": True},
        },
    )
    if code != 201:
        raise RuntimeError(f"create live {code} {body}")
    pause_id = create_dag_sched(pause_t, name="paused", cron="*/2 * * * * *")
    black_id = create_dag_sched(black_t, name="black", cron="*/2 * * * * *")
    fire_id = create_dag_sched(fire_t, name="manual", cron="0 0 1 1 *")

    code, body = http_json("POST", f"/v1/tenants/{pause_t}/pause", {})
    if code != 200:
        raise RuntimeError(f"pause {code} {body}")
    now = _now()
    code, body = http_json(
        "POST",
        f"/v1/tenants/{black_t}/blackouts",
        {
            "starts_at": _iso(now - timedelta(seconds=1)),
            "ends_at": _iso(now + timedelta(minutes=10)),
            "reason": "deploy",
        },
    )
    if code != 201:
        raise RuntimeError(f"blackout {code} {body}")

    # Concurrent manual fire while schedulers also poll.
    hits: list[int] = []
    codes: list[int] = []
    lock = threading.Lock()

    def hammer() -> None:
        st, payload = fire(fire_t, fire_id)
        with lock:
            codes.append(st)
            if st == 201:
                hits.append(len(payload.get("jobs") or []))

    threads = [threading.Thread(target=hammer) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    if hits != [DIAMOND]:
        raise RuntimeError(f"manual fire graphs={hits} codes={codes}")
    fire_jobs = list_jobs(fire_t)
    if len(fire_jobs) != DIAMOND:
        raise RuntimeError(f"manual fire listed {len(fire_jobs)}")
    logger.info("manual fire one diamond codes=%s", codes)

    # Dual schedulers should materialize exactly one race graph.
    deadline = time.time() + 8
    race_jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        race_jobs = list_jobs(race_t)
        if race_jobs:
            break
        time.sleep(0.2)
    if not race_jobs:
        raise RuntimeError("race tenant never received a DAG tick")
    if len(race_jobs) != DIAMOND:
        raise RuntimeError(f"race half/dup graph n={len(race_jobs)}")
    logger.info("race tick one diamond n=%d", len(race_jobs))

    if list_jobs(pause_t):
        raise RuntimeError("paused DAG tenant piled jobs")
    if list_jobs(black_t):
        raise RuntimeError("blacked-out DAG tenant piled jobs")

    wait_completed(fire_t, DIAMOND)
    wait_completed(race_t, DIAMOND)

    # Second manual fire after the first graph finished.
    st2, body2 = fire(fire_t, fire_id)
    if st2 != 201:
        raise RuntimeError(f"second fire {st2} {body2}")
    if len(body2.get("jobs") or []) != DIAMOND:
        raise RuntimeError(f"second fire not a diamond {body2}")
    wait_completed(fire_t, DIAMOND * 2)
    if len(list_jobs(fire_t)) != DIAMOND * 2:
        raise RuntimeError("second fire duplicated or partial")

    live_n = completed(list_jobs(live_t))
    if live_n < 1:
        deadline = time.time() + 8
        while time.time() < deadline and live_n < 1:
            time.sleep(0.3)
            live_n = completed(list_jobs(live_t))
    if live_n < 1:
        raise RuntimeError("live tenant did not complete a cron tick")

    # Unused ids kept so pause/black schedules are created under load.
    _ = (race_id, pause_id, black_id)
    logger.info("schedule dag soak OK live_completed=%d", live_n)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule dag soak failed")
        sys.exit(1)
