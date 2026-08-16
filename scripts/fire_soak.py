#!/usr/bin/env python3
"""Fleet soak: manual schedule fire under dual coordinators.

  * Dual coordinators + dual schedulers + four workers.
  * Concurrent POST /fire on one schedule → exactly one job.
  * Paused and blacked-out tenants refuse fire; a live tenant still ticks.
  * After the first job completes, a second fire works.
  * Other tenants keep leasing through the churn.
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
logger = logging.getLogger("fire_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


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
        headers["Content-Length"] = str(len(data))
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


def create_sched(tid: str, *, name: str, cron: str = "0 * * * *") -> str:
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": name,
            "cron": cron,
            "payload": {"kind": "render", "ms": JOB_MS, "name": name},
            "misfire_policy": "catch_up_one",
        },
    )
    if code != 201:
        raise RuntimeError(f"create schedule {tid} {code} {body}")
    return str(body["schedule"]["id"])


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid}
    )


def wait_completed(tid: str, minimum: int) -> None:
    deadline = time.time() + MAX_WALL
    n = 0
    last = 0.0
    while time.time() < deadline:
        n = completed(list_jobs(tid))
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s completed=%d need>=%d", tid, n, minimum)
            last = now
        if n >= minimum:
            return
        time.sleep(0.15)
    raise RuntimeError(f"timed out waiting {tid} completed={n} need={minimum}")


def main() -> int:
    logger.info("fire soak")
    wait_coords()
    race_t, live_t, pause_t, black_t, many_t = (
        "ff-race",
        "ff-live",
        "ff-pause",
        "ff-black",
        "ff-many",
    )
    race_id = create_sched(race_t, name="race", cron="0 0 1 1 *")
    live_id = create_sched(live_t, name="live", cron="*/2 * * * * *")
    pause_id = create_sched(pause_t, name="paused", cron="0 0 1 1 *")
    black_id = create_sched(black_t, name="black", cron="0 0 1 1 *")
    many_ids = [
        create_sched(many_t, name=f"many-{i}", cron="0 0 1 1 *") for i in range(8)
    ]

    code, body = http_json("POST", f"/v1/tenants/{pause_t}/pause", {})
    if code != 200:
        raise RuntimeError(f"pause {pause_t} {code} {body}")
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
        raise RuntimeError(f"blackout {black_t} {code} {body}")

    hits: list[str] = []
    codes: list[int] = []
    lock = threading.Lock()

    def hammer() -> None:
        st, payload = fire(race_t, race_id)
        with lock:
            codes.append(st)
            if st == 201:
                hits.append(str(payload["job"]["id"]))

    threads = [threading.Thread(target=hammer) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    if len(hits) != 1:
        raise RuntimeError(f"race fire jobs={len(hits)} codes={codes}")
    if any(c not in (201, 409) for c in codes):
        raise RuntimeError(f"unexpected fire codes {codes}")
    race_jobs = list_jobs(race_t)
    if len(race_jobs) != 1:
        raise RuntimeError(f"race tenant listed {len(race_jobs)} jobs")
    logger.info("race fire one job codes=%s", codes)

    pause_code, pause_body = fire(pause_t, pause_id)
    if pause_code != 409:
        raise RuntimeError(f"paused fire {pause_code} {pause_body}")
    black_code, black_body = fire(black_t, black_id)
    if black_code != 409:
        raise RuntimeError(f"blackout fire {black_code} {black_body}")
    if list_jobs(pause_t) or list_jobs(black_t):
        raise RuntimeError("refused tenants piled jobs")

    many_hits = 0
    many_err: list[str] = []

    def fire_one(sid: str) -> None:
        nonlocal many_hits
        st, payload = fire(many_t, sid)
        if st != 201:
            many_err.append(f"{sid} -> {st} {payload}")
            return
        many_hits += 1

    mthreads = [
        threading.Thread(target=fire_one, args=(sid,)) for sid in many_ids
    ]
    for t in mthreads:
        t.start()
    for t in mthreads:
        t.join(timeout=10)
    if many_err:
        raise RuntimeError(f"many fire errors: {many_err}")
    if many_hits != len(many_ids):
        raise RuntimeError(f"many fire hits={many_hits}")
    if len(list_jobs(many_t)) != len(many_ids):
        raise RuntimeError("many tenant job count mismatch")
    logger.info("many fire %d schedules", many_hits)

    wait_completed(race_t, 1)
    st2, body2 = fire(race_t, race_id)
    if st2 != 201:
        raise RuntimeError(f"second fire after ack {st2} {body2}")
    wait_completed(race_t, 2)
    wait_completed(many_t, len(many_ids))

    live_n = completed(list_jobs(live_t))
    if live_n < 1:
        # live cron is every 2s; give it a moment if the first tick is late
        deadline = time.time() + 8
        while time.time() < deadline and live_n < 1:
            time.sleep(0.3)
            live_n = completed(list_jobs(live_t))
    if live_n < 1:
        raise RuntimeError("live tenant did not complete a cron tick")
    logger.info("fire soak OK live_completed=%d race=%s", live_n, race_id)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("fire soak failed")
        sys.exit(1)
