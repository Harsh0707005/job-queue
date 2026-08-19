#!/usr/bin/env python3
"""Fleet soak: move a live schedule across tenants under dual schedulers.

Dual coordinators + dual schedulers + four workers:

  * sm-dag    — freeze a diamond mid-tick, move to dest; graph stays closed
                and dest can decrypt. No second tick while frozen.
  * sm-mutex  — dest already holds group 'blue'; source member of 'blue'
                cannot move. Source sibling keeps firing after a red
                schedule does move.
  * sm-race   — due schedule hammered by move + process-due; dest has at
                most one tick.
  * sm-live   — other tenant keeps completing through the moves.
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
logger = logging.getLogger("schedule_move_soak")

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
DIAMOND = 4
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


def get_sched(sid: str, tid: str) -> tuple[int, dict[str, Any]]:
    return http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")


def create_sched(
    tid: str,
    *,
    name: str,
    group: str | None = None,
    dag: bool = False,
    prep_ms: int | None = None,
    ms: int | None = None,
    enabled: bool = True,
    secret: str | None = None,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    root_ms = prep_ms if prep_ms is not None else job_ms
    payload: dict[str, Any] = {
        "kind": "render",
        "ms": job_ms,
        "name": name,
    }
    if secret is not None:
        payload["secret"] = secret
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{CRON_EVERY} * * * * *",
        "payload": payload,
        "misfire_policy": "catch_up_one",
        "catch_up_max": 3,
        "enabled": enabled,
    }
    if group is not None:
        body["mutex_group"] = group
    if dag:
        body["dag_jobs"] = [
            {
                "ref": "prep",
                "payload": {"step": "prep", "ms": root_ms, "secret": secret},
            },
            {
                "ref": "left",
                "payload": {"step": "left", "ms": job_ms},
                "depends_on": ["prep"],
            },
            {
                "ref": "right",
                "payload": {"step": "right", "ms": job_ms},
                "depends_on": ["prep"],
            },
            {
                "ref": "join",
                "payload": {"step": "join", "ms": job_ms},
                "depends_on": ["left", "right"],
            },
        ]
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def move(tid: str, sid: str, dest: str) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST",
        f"/v1/schedules/{sid}/move",
        {"tenant_id": tid, "to_tenant_id": dest},
    )


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
    logger.info("schedule move soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    dag_src, dag_dst = "sm-dag-src", "sm-dag-dst"
    mx_src, mx_dst = "sm-mx-src", "sm-mx-dst"
    race_src, race_dst = "sm-race-src", "sm-race-dst"
    live_t = "sm-live"

    dag_id = create_sched(
        dag_src, name="dag", dag=True, prep_ms=3000, secret="dag-secret"
    )
    mx_hold = create_sched(mx_dst, name="hold", group="blue", ms=2500)
    mx_in = create_sched(mx_src, name="incoming", group="blue")
    mx_sib = create_sched(mx_src, name="sib", group="blue")
    mx_red = create_sched(mx_src, name="red", group="red", enabled=False)
    race_id = create_sched(race_src, name="race")
    create_sched(live_t, name="live")

    dag_jobs = wait_n(dag_src, DIAMOND, schedule_id=dag_id)
    freezeable = [
        j
        for j in dag_jobs
        if j.get("status") in ("pending", "leased", "awaiting_skills")
    ]
    if not freezeable:
        raise RuntimeError("dag already finished before move")
    hold = freezeable[-1]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{hold['id']}/freeze",
        {"tenant_id": dag_src, "reason": "hold move"},
    )
    if code != 200 or (body.get("job") or {}).get("status") != "frozen":
        raise RuntimeError(f"freeze dag {code} {body}")

    mx_jobs = wait_n(mx_dst, 1, schedule_id=mx_hold)
    code, body = http_json(
        "POST",
        f"/v1/jobs/{mx_jobs[0]['id']}/freeze",
        {"tenant_id": mx_dst, "reason": "hold dest group"},
    )
    if code != 200:
        raise RuntimeError(f"freeze dest mutex {code} {body}")

    c_code, c_body = move(mx_src, mx_in, mx_dst)
    if c_code != 409 or c_body.get("code") != "schedule_error":
        raise RuntimeError(f"busy dest group should 409, got {c_code} {c_body}")
    code, _ = get_sched(mx_in, mx_src)
    if code != 200:
        raise RuntimeError("incoming vanished after refused move")

    r_code, r_body = move(mx_src, mx_red, mx_dst)
    if r_code != 200:
        raise RuntimeError(f"red move {r_code} {r_body}")
    if (r_body.get("schedule") or {}).get("mutex_group") != "red":
        raise RuntimeError(f"red lost mutex {r_body}")
    if (r_body.get("schedule") or {}).get("tenant_id") != mx_dst:
        raise RuntimeError(f"red not on dest {r_body}")

    m_code, m_body = move(dag_src, dag_id, dag_dst)
    if m_code != 200:
        raise RuntimeError(f"dag move {m_code} {m_body}")
    moved = m_body.get("schedule") or {}
    if moved.get("tenant_id") != dag_dst:
        raise RuntimeError(f"dag not on dest {moved}")
    if (moved.get("payload") or {}).get("secret") != "dag-secret":
        raise RuntimeError(f"dag payload not decryptable {moved}")
    code, _ = get_sched(dag_id, dag_src)
    if code != 404:
        raise RuntimeError("dag still on source")
    dest_dag = list_jobs(dag_dst)
    if len(dest_dag) != DIAMOND:
        raise RuntimeError(f"dag graph not closed n={len(dest_dag)}")
    if any(j.get("schedule_id") != dag_id for j in dest_dag):
        raise RuntimeError("dag jobs lost schedule_id")
    if list_jobs(dag_src):
        raise RuntimeError("source still has dag jobs")
    f_code, f_body = fire(dag_dst, dag_id)
    if f_code != 409:
        raise RuntimeError(f"dest dag refire {f_code} {f_body}")
    logger.info("moved frozen dag; dest decrypts; no second tick")

    wait_n(mx_src, 1, schedule_id=mx_sib)
    live_mid = completed(list_jobs(live_t))

    race_codes: list[int] = []

    def hammer_move() -> None:
        c, _b = move(race_src, race_id, race_dst)
        race_codes.append(c)

    threads = [threading.Thread(target=hammer_move) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
    if 200 not in race_codes:
        raise RuntimeError(f"race move never succeeded {race_codes}")
    if any(c not in (200, 404) for c in race_codes):
        raise RuntimeError(f"race move codes {race_codes}")
    code, _ = get_sched(race_id, race_src)
    if code != 404:
        raise RuntimeError("race schedule still on source")
    code, _race_sched = get_sched(race_id, race_dst)
    if code != 200:
        raise RuntimeError(f"race schedule missing on dest {code}")
    http_json(
        "POST",
        f"/v1/schedules/{race_id}/pause",
        {"tenant_id": race_dst},
    )
    race_jobs = list_jobs(race_dst)
    if len(race_jobs) > 1:
        raise RuntimeError(f"race duplicated ticks n={len(race_jobs)}")
    src_race = list_jobs(race_src)
    if src_race:
        raise RuntimeError(f"race left jobs on source {len(src_race)}")
    logger.info(
        "race move codes=%s dest_jobs=%d",
        {c: race_codes.count(c) for c in set(race_codes)},
        len(race_jobs),
    )

    deadline = time.time() + HOLD
    while time.time() < deadline:
        if len(list_jobs(dag_dst)) != DIAMOND:
            raise RuntimeError("dest dag double-fired after move")
        if list_jobs(dag_src):
            raise RuntimeError("source dag grew after move")
        time.sleep(0.25)

    sib_jobs = by_sched(list_jobs(mx_src), mx_sib)
    if len(sib_jobs) < 1:
        raise RuntimeError("source mutex sibling stalled after peer move")
    live_after = completed(list_jobs(live_t))
    if live_after < max(1, live_mid):
        raise RuntimeError("live tenant stalled during moves")

    logger.info(
        "schedule move soak OK sib=%d live=%d race_jobs=%d",
        len(sib_jobs),
        live_after,
        len(race_jobs),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule move soak failed")
        sys.exit(1)
