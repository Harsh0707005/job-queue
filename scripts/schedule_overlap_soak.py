#!/usr/bin/env python3
"""Fleet soak: no second schedule tick while skills-parked or frozen.

Dual coordinators + dual schedulers + four workers (no tpu skill):

  * so-skills  — DAG requires tpu; first tick parks in awaiting_skills.
                 Later process_due polls must not enqueue a second graph.
  * so-freeze  — same park, then freeze one node; still no second graph.
  * so-one     — single-job schedule parked on tpu; still one job.
  * so-live    — no special skills; cron keeps completing.
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
logger = logging.getLogger("schedule_overlap_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD_SECONDS = int(os.environ.get("HOLD_SECONDS", "8") or "8")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "2") or "2")
JOB_MS = int(os.environ.get("JOB_MS", "15") or "15")
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


def statuses(jobs: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for j in jobs:
        st = str(j.get("status") or "?")
        out[st] = out.get(st, 0) + 1
    return out


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


def create_sched(
    tid: str,
    *,
    name: str,
    dag: bool,
    skills: list[str] | None = None,
) -> str:
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{CRON_EVERY} * * * * *",
        "payload": {"kind": "render", "ms": JOB_MS, "tag": tid},
        "misfire_policy": "catch_up_one",
    }
    if dag:
        body["dag_jobs"] = diamond(tid)
    if skills:
        body["required_skills"] = skills
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid} {code} {resp}")
    return str(resp["schedule"]["id"])


def wait_n_jobs(tid: str, n: int, *, timeout: float = 15.0) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    jobs: list[dict[str, Any]] = []
    while time.time() < deadline:
        jobs = list_jobs(tid)
        if len(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(f"{tid} never reached {n} jobs (had {len(jobs)})")


def assert_no_growth(tid: str, expected: int, *, seconds: float) -> None:
    deadline = time.time() + seconds
    last = 0.0
    while time.time() < deadline:
        jobs = list_jobs(tid)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold %s n=%d statuses=%s", tid, len(jobs), statuses(jobs)
            )
            last = now
        if len(jobs) != expected:
            raise RuntimeError(
                f"{tid} double-fired: expected {expected} jobs, have {len(jobs)} "
                f"statuses={statuses(jobs)}"
            )
        if expected > 1 and len(jobs) % DIAMOND != 0:
            raise RuntimeError(f"{tid} half graph n={len(jobs)}")
        time.sleep(0.25)


def main() -> int:
    logger.info(
        "overlap soak hold=%ds cron=every %ds", HOLD_SECONDS, CRON_EVERY
    )
    wait_coords()
    skills_t, freeze_t, one_t, live_t = (
        "so-skills",
        "so-freeze",
        "so-one",
        "so-live",
    )
    create_sched(skills_t, name="skills-dag", dag=True, skills=["tpu"])
    create_sched(freeze_t, name="freeze-dag", dag=True, skills=["tpu"])
    create_sched(one_t, name="skills-one", dag=False, skills=["tpu"])
    create_sched(live_t, name="live", dag=False)

    skills_jobs = wait_n_jobs(skills_t, DIAMOND)
    freeze_jobs = wait_n_jobs(freeze_t, DIAMOND)
    one_jobs = wait_n_jobs(one_t, 1)
    if any(j.get("status") != "awaiting_skills" for j in skills_jobs):
        raise RuntimeError(f"skills dag not parked: {statuses(skills_jobs)}")
    if any(j.get("status") != "awaiting_skills" for j in one_jobs):
        raise RuntimeError(f"single job not parked: {statuses(one_jobs)}")

    # Freeze one parked node so process_due must treat frozen as inflight too.
    hold = freeze_jobs[0]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{hold['id']}/freeze",
        {"tenant_id": freeze_t, "reason": "hold tick"},
    )
    if code != 200:
        raise RuntimeError(f"freeze {code} {body}")
    if (body.get("job") or {}).get("status") != "frozen":
        raise RuntimeError(f"freeze did not stick: {body}")
    logger.info("froze %s on %s", hold["id"], freeze_t)

    # Dual schedulers keep polling a due next_run_at. Must not grow.
    deadline = time.time() + HOLD_SECONDS
    last = 0.0
    while time.time() < deadline:
        skills_now = list_jobs(skills_t)
        freeze_now = list_jobs(freeze_t)
        one_now = list_jobs(one_t)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold skills=%s freeze=%s one=%s",
                statuses(skills_now),
                statuses(freeze_now),
                statuses(one_now),
            )
            last = now
        if len(skills_now) != DIAMOND:
            raise RuntimeError(f"skills dag double-fired n={len(skills_now)}")
        if len(freeze_now) != DIAMOND:
            raise RuntimeError(f"freeze dag double-fired n={len(freeze_now)}")
        if len(one_now) != 1:
            raise RuntimeError(f"single job double-fired n={len(one_now)}")
        time.sleep(0.25)
    freeze_after = list_jobs(freeze_t)
    one_after = list_jobs(one_t)
    frozen_n = sum(1 for j in freeze_after if j.get("status") == "frozen")
    if frozen_n != 1:
        raise RuntimeError(f"expected 1 frozen job, have {statuses(freeze_after)}")

    live = list_jobs(live_t)
    live_done = sum(1 for j in live if j.get("status") == "completed")
    if live_done < 1:
        deadline = time.time() + 8
        while time.time() < deadline and live_done < 1:
            time.sleep(0.3)
            live = list_jobs(live_t)
            live_done = sum(1 for j in live if j.get("status") == "completed")
    if live_done < 1:
        raise RuntimeError("live tenant did not complete a cron tick")

    # Manual fire must also refuse while parked / frozen.
    for tid, sid_path in (
        (skills_t, None),
        (freeze_t, None),
        (one_t, None),
    ):
        code, body = http_json("GET", f"/v1/schedules?tenant_id={tid}")
        if code != 200:
            raise RuntimeError(f"list schedules {tid} {code} {body}")
        sid = body["schedules"][0]["id"]
        st, resp = http_json(
            "POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid}
        )
        if st != 409:
            raise RuntimeError(f"fire {tid} expected 409 got {st} {resp}")

    logger.info(
        "overlap soak OK live_completed=%d skills=%s freeze=%s one=%s",
        live_done,
        statuses(list_jobs(skills_t)),
        statuses(freeze_after),
        statuses(one_after),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("overlap soak failed")
        sys.exit(1)
