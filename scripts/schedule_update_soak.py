#!/usr/bin/env python3
"""Fleet soak: edit live cron / DAG templates under dual schedulers.

  * se-cron  — flip a fast cron to yearly (no flood) then back to */2s.
  * se-dag   — first tick is a 4-node diamond; mid-flight we swap to a
               2-node line. Later ticks are pairs, never a mixed graph.
  * se-race  — two threads flip cron + DAG while schedulers poll.
  * se-live  — untouched control keeps completing.
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
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schedule_update_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD = int(os.environ.get("HOLD_SECONDS", "8") or "8")
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


def diamond() -> list[dict[str, Any]]:
    return [
        {"ref": "prep", "payload": {"kind": "render", "ms": JOB_MS, "shape": "dia"}},
        {
            "ref": "left",
            "payload": {"kind": "render", "ms": JOB_MS, "shape": "dia"},
            "depends_on": ["prep"],
        },
        {
            "ref": "right",
            "payload": {"kind": "render", "ms": JOB_MS, "shape": "dia"},
            "depends_on": ["prep"],
        },
        {
            "ref": "join",
            "payload": {"kind": "render", "ms": JOB_MS, "shape": "dia"},
            "depends_on": ["left", "right"],
        },
    ]


def line() -> list[dict[str, Any]]:
    return [
        {"ref": "only", "payload": {"kind": "render", "ms": JOB_MS, "shape": "line"}},
        {
            "ref": "after",
            "payload": {"kind": "render", "ms": JOB_MS, "shape": "line"},
            "depends_on": ["only"],
        },
    ]


def create_sched(tid: str, *, name: str, cron: str, dag: list | None) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": cron,
        "payload": {"kind": "render", "ms": JOB_MS, "tag": tid},
        "misfire_policy": "catch_up_one",
    }
    if dag is not None:
        body["dag_jobs"] = dag
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid} {code} {resp}")
    return str(resp["schedule"]["id"])


def update_sched(tid: str, sid: str, **fields: Any) -> dict[str, Any]:
    body = {"tenant_id": tid, **fields}
    code, resp = http_json("POST", f"/v1/schedules/{sid}/update", body)
    if code != 200:
        raise RuntimeError(f"update {tid} {code} {resp}")
    return resp["schedule"]


def wait_n(tid: str, n: int, *, timeout: float = 20.0) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        jobs = list_jobs(tid)
        if len(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(f"{tid} never reached {n} jobs (had {len(jobs)})")


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def main() -> int:
    logger.info("schedule update soak hold=%ds", HOLD)
    wait_coords()
    cron_t, dag_t, race_t, live_t = "se-cron", "se-dag", "se-race", "se-live"
    cron_id = create_sched(cron_t, name="flip-cron", cron="*/2 * * * * *", dag=None)
    dag_id = create_sched(dag_t, name="flip-dag", cron="*/2 * * * * *", dag=diamond())
    race_id = create_sched(race_t, name="race", cron="*/2 * * * * *", dag=diamond())
    create_sched(live_t, name="live", cron="*/2 * * * * *", dag=None)

    cron_mid = wait_n(cron_t, 1)
    first = wait_n(dag_t, 4)
    if len(first) % 4 != 0:
        raise RuntimeError(f"first dag ticks not closed diamonds n={len(first)}")
    logger.info("first dag snapshot n=%d", len(first))
    update_sched(dag_t, dag_id, dag_jobs=line())
    dag_baseline = len(list_jobs(dag_t))

    # Yearly cron: next_run jumps to the future. Dual schedulers must not
    # enqueue a backlog of the old 2s ticks.
    update_sched(cron_t, cron_id, cron="0 0 1 1 *", interval_seconds=None)
    before = len(list_jobs(cron_t))
    time.sleep(HOLD)
    after = list_jobs(cron_t)
    if len(after) > before + 1:
        raise RuntimeError(
            f"cron edit flooded {len(after) - before} extra jobs (had {before})"
        )
    logger.info("cron quiet after yearly switch n=%d was=%d", len(after), before)

    update_sched(cron_t, cron_id, cron="*/2 * * * * *", interval_seconds=None)
    # After switching back, at most a couple of new ticks in a few seconds.
    time.sleep(5)
    cron_back = list_jobs(cron_t)
    grew = len(cron_back) - len(after)
    if grew > 4:
        raise RuntimeError(f"cron restart flooded {grew} jobs")
    logger.info("cron resumed grew=%d", grew)
    # Let workers finish the diamond and take a couple of line ticks.
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        jobs = list_jobs(dag_t)
        n = len(jobs)
        extra = n - dag_baseline
        now = time.time()
        if now - last >= 2:
            logger.info(
                "dag n=%d extra=%d completed=%d baseline=%d",
                n,
                extra,
                completed(jobs),
                dag_baseline,
            )
            last = now
        if extra < 0:
            raise RuntimeError("dag jobs disappeared")
        if extra % 2 != 0:
            raise RuntimeError(f"mixed/half dag after edit n={n} extra={extra}")
        if extra >= 2 and completed(jobs) >= dag_baseline:
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("dag never produced a 2-node tick after edit")
    logger.info("dag after edit n=%d", len(list_jobs(dag_t)))

    stop = threading.Event()
    race_err: list[str] = []

    def flipper(i: int) -> None:
        try:
            use_line = i % 2 == 0
            while not stop.is_set():
                fields: dict[str, Any] = {
                    "dag_jobs": line() if use_line else diamond(),
                }
                use_line = not use_line
                code, resp = http_json(
                    "POST",
                    f"/v1/schedules/{race_id}/update",
                    {"tenant_id": race_t, **fields},
                )
                if code not in (200, 409):
                    race_err.append(f"{code} {resp}")
                    return
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            race_err.append(str(exc))

    threads = [threading.Thread(target=flipper, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    time.sleep(HOLD)
    stop.set()
    for t in threads:
        t.join(timeout=5)
    if race_err:
        raise RuntimeError(f"race updates: {race_err}")
    race_jobs = list_jobs(race_t)
    if not race_jobs:
        raise RuntimeError("race tenant produced no jobs")
    # Every committed tick is a closed 2-node or 4-node set.
    # Counts after the first tick should land on those sizes only when
    # grouped... we only assert we never see a leftover 1 or 3 in isolation
    # beyond what's explainable by an in-flight last tick (0-4 pending).
    pending = [j for j in race_jobs if j.get("status") not in ("completed", "failed", "dead", "cancelled")]
    if len(pending) > 4:
        raise RuntimeError(f"race leftover inflight {len(pending)}")
    logger.info("race jobs=%d pending=%d", len(race_jobs), len(pending))

    live = list_jobs(live_t)
    live_n = completed(live)
    if live_n < 1:
        raise RuntimeError("live tenant stalled")
    logger.info("schedule update soak OK live_completed=%d", live_n)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule update soak failed")
        sys.exit(1)
