#!/usr/bin/env python3
"""Fleet soak: delete a schedule without orphaning or double-firing.

Dual coordinators + dual schedulers + four workers:

  * sd-held   — DAG tick frozen mid-flight; fail-closed delete (409)
                until the graph completes, then delete succeeds.
  * sd-sib    — same tenant as held; keeps firing the whole time.
  * sd-park   — skills-parked tick; delete stays 409, no second tick.
  * sd-wait   — leased job; wait=true delete drains then removes.
  * sd-idle   — empty schedule deletes immediately.
  * sd-live   — other tenant keeps completing.
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
logger = logging.getLogger("schedule_delete_soak")

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
WAIT_JOB_MS = int(os.environ.get("WAIT_JOB_MS", "800") or "800")
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


def statuses(jobs: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for j in jobs:
        key = str(j.get("status") or "?")
        out[key] = out.get(key, 0) + 1
    return out


def create_sched(
    tid: str,
    *,
    name: str,
    dag: bool = False,
    skills: list[str] | None = None,
    ms: int | None = None,
    prep_ms: int | None = None,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    root_ms = prep_ms if prep_ms is not None else job_ms
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{CRON_EVERY} * * * * *",
        "payload": {"kind": "render", "ms": job_ms, "name": name},
        "misfire_policy": "catch_up_one",
        "catch_up_max": 3,
    }
    if dag:
        body["dag_jobs"] = [
            {"ref": "prep", "payload": {"step": "prep", "ms": root_ms}},
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
    if skills:
        body["required_skills"] = skills
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def delete(
    tid: str, sid: str, *, wait: bool = False, timeout: float | None = None
) -> tuple[int, dict[str, Any]]:
    body: dict[str, Any] = {"tenant_id": tid}
    if wait:
        body["wait"] = True
    if timeout is not None:
        body["wait_timeout_seconds"] = timeout
    return http_json("POST", f"/v1/schedules/{sid}/delete", body)


def get_sched(sid: str, tid: str) -> tuple[int, dict[str, Any]]:
    return http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")


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
            jobs = [j for j in jobs if j.get("schedule_id") == schedule_id]
        if len(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(f"{tid} never reached {n} jobs (had {len(jobs)})")


def wait_done(
    tid: str,
    n: int,
    *,
    schedule_id: str | None = None,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        jobs = list_jobs(tid)
        if schedule_id is not None:
            jobs = [j for j in jobs if j.get("schedule_id") == schedule_id]
        if completed(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(
        f"{tid} never completed {n} (had {completed(jobs)} of {len(jobs)} "
        f"{statuses(jobs)})"
    )


def main() -> int:
    logger.info(
        "schedule delete soak hold=%ds cron=every %ds", HOLD, CRON_EVERY
    )
    wait_coords()
    held_t, park_t, wait_t, idle_t, live_t = (
        "sd-held",
        "sd-park",
        "sd-wait",
        "sd-idle",
        "sd-live",
    )
    held_id = create_sched(held_t, name="held", dag=True, prep_ms=3000)
    sib_id = create_sched(held_t, name="sib")
    park_id = create_sched(park_t, name="park", dag=True, skills=["tpu"])
    wait_id = create_sched(wait_t, name="wait", ms=WAIT_JOB_MS)
    idle_id = create_sched(idle_t, name="idle")
    create_sched(live_t, name="live")

    code, body = delete(idle_t, idle_id)
    if code != 200 or not body.get("deleted"):
        raise RuntimeError(f"idle delete {code} {body}")
    code, _ = get_sched(idle_id, idle_t)
    if code != 404:
        raise RuntimeError(f"idle schedule still visible {code}")
    logger.info("idle schedule deleted immediately")

    held_nodes = wait_n(held_t, DIAMOND, schedule_id=held_id)
    park_jobs = wait_n(park_t, DIAMOND, schedule_id=park_id)
    if any(j.get("status") != "awaiting_skills" for j in park_jobs):
        raise RuntimeError(f"park not awaiting_skills: {statuses(park_jobs)}")
    wait_n(wait_t, 1, schedule_id=wait_id)

    freezeable = [
        j
        for j in held_nodes
        if j.get("status") in ("pending", "leased", "awaiting_skills")
    ]
    if not freezeable:
        raise RuntimeError(f"held already finished: {statuses(held_nodes)}")
    # Prefer a blocked child so the graph stays inflight after prep finishes.
    hold = freezeable[-1]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{hold['id']}/freeze",
        {"tenant_id": held_t, "reason": "hold delete"},
    )
    if code != 200 or (body.get("job") or {}).get("status") != "frozen":
        raise RuntimeError(f"freeze {code} {body}")
    logger.info("froze %s on held", hold["id"])

    # Fail-closed while frozen / parked. Sibling and live keep moving.
    deadline = time.time() + HOLD
    last = 0.0
    held_codes: dict[int, int] = {}
    park_codes: dict[int, int] = {}
    while time.time() < deadline:
        h_code, h_body = delete(held_t, held_id)
        held_codes[h_code] = held_codes.get(h_code, 0) + 1
        if h_code != 409 or h_body.get("code") != "schedule_error":
            raise RuntimeError(f"held delete expected 409, got {h_code} {h_body}")
        p_code, p_body = delete(park_t, park_id)
        park_codes[p_code] = park_codes.get(p_code, 0) + 1
        if p_code != 409:
            raise RuntimeError(f"park delete expected 409, got {p_code} {p_body}")
        now_held = [j for j in list_jobs(held_t) if j.get("schedule_id") == held_id]
        if len(now_held) != DIAMOND:
            raise RuntimeError(
                f"held double-fired n={len(now_held)} {statuses(now_held)}"
            )
        now_park = list_jobs(park_t)
        if len(now_park) != DIAMOND:
            raise RuntimeError(
                f"park double-fired n={len(now_park)} {statuses(now_park)}"
            )
        now = time.time()
        if now - last >= 2:
            sib_n = len(
                [j for j in list_jobs(held_t) if j.get("schedule_id") == sib_id]
            )
            logger.info(
                "hold held_del=%s park_del=%s sib=%d live=%d",
                held_codes,
                park_codes,
                sib_n,
                completed(list_jobs(live_t)),
            )
            last = now
        time.sleep(0.2)

    sib_mid = [j for j in list_jobs(held_t) if j.get("schedule_id") == sib_id]
    if len(sib_mid) < 1:
        raise RuntimeError("sibling did not fire while peer delete was refused")
    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during held/park refuse window")
    logger.info(
        "refuse window held_del=%s park_del=%s sib=%d live=%d",
        held_codes,
        park_codes,
        len(sib_mid),
        live_mid,
    )

    # Wait-delete a live tick: workers are already chewing the slow job.
    w_code, w_body = delete(wait_t, wait_id, wait=True, timeout=20)
    if w_code != 200 or not w_body.get("deleted"):
        raise RuntimeError(f"wait delete {w_code} {w_body}")
    code, _ = get_sched(wait_id, wait_t)
    if code != 404:
        raise RuntimeError(f"wait schedule still visible {code}")
    wait_jobs = list_jobs(wait_t)
    if not wait_jobs or any(j.get("status") != "completed" for j in wait_jobs):
        raise RuntimeError(f"wait jobs not drained: {statuses(wait_jobs)}")
    if any(j.get("schedule_id") != wait_id for j in wait_jobs):
        raise RuntimeError("wait jobs lost schedule_id history")
    extra_wait = list_jobs(wait_t)
    time.sleep(CRON_EVERY + 1)
    if len(list_jobs(wait_t)) != len(extra_wait):
        raise RuntimeError("wait schedule enqueued after delete")
    logger.info("wait delete drained n=%d", len(wait_jobs))

    # Unfreeze held graph, let it finish, then delete.
    code, body = http_json(
        "POST",
        f"/v1/jobs/{hold['id']}/unfreeze",
        {"tenant_id": held_t},
    )
    if code != 200:
        raise RuntimeError(f"unfreeze {code} {body}")
    wait_done(held_t, DIAMOND, schedule_id=held_id)
    # Sibling jobs may still be in flight; only the held graph must be idle.
    d_code, d_body = delete(held_t, held_id)
    if d_code != 200 or not d_body.get("deleted"):
        raise RuntimeError(f"held delete after drain {d_code} {d_body}")
    code, _ = get_sched(held_id, held_t)
    if code != 404:
        raise RuntimeError(f"held schedule still visible {code}")
    held_after = [j for j in list_jobs(held_t) if j.get("schedule_id") == held_id]
    if len(held_after) != DIAMOND:
        raise RuntimeError(f"held history changed after delete n={len(held_after)}")
    if any(j.get("status") != "completed" for j in held_after):
        raise RuntimeError(f"held history not completed: {statuses(held_after)}")

    # Dual-coordinator delete of an already-gone schedule is 404, not 500.
    race: list[int] = []

    def gone() -> None:
        c, _b = delete(held_t, held_id)
        race.append(c)

    threads = [threading.Thread(target=gone) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    if any(c != 404 for c in race):
        raise RuntimeError(f"duplicate delete codes {race}")

    time.sleep(CRON_EVERY + 1)
    held_final = [j for j in list_jobs(held_t) if j.get("schedule_id") == held_id]
    if len(held_final) != DIAMOND:
        raise RuntimeError(
            f"held refired after delete n={len(held_final)} was {DIAMOND}"
        )
    sib_final = [j for j in list_jobs(held_t) if j.get("schedule_id") == sib_id]
    if len(sib_final) < len(sib_mid):
        raise RuntimeError("sibling went backwards after peer delete")
    park_final = list_jobs(park_t)
    if len(park_final) != DIAMOND:
        raise RuntimeError(f"park grew after hold n={len(park_final)}")
    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")

    # Parked schedule still exists (delete never succeeded).
    code, body = get_sched(park_id, park_t)
    if code != 200:
        raise RuntimeError(f"park schedule missing {code} {body}")

    logger.info(
        "schedule delete soak OK sib=%d live_completed=%d held_hist=%d",
        len(sib_final),
        live_after,
        len(held_final),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule delete soak failed")
        sys.exit(1)
