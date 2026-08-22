#!/usr/bin/env python3
"""Fleet soak: cancel and replay one fan-out generation as a closed set.

Dual coordinators + dual schedulers + four workers:

  * sg-cx    — cancel generation 1 while waiters are live; no join job,
               payload stays failed, fire of waiter/join is 409.
  * sg-rp    — cancel then replay; waiters+join return for that iteration
               only (no dump).
  * sg-part  — delete a waiter mid-generation; replay refuses (no partial
               join) and dest cannot take a live fan-out move.
  * sg-live  — other tenant keeps completing.
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
logger = logging.getLogger("schedule_generation_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HOLD = int(os.environ.get("HOLD_SECONDS", "10") or "10")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "5") or "5")
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
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


def create_sched(
    tid: str,
    *,
    name: str,
    waits_on: list[str] | None = None,
    joins_on: list[str] | None = None,
    policy: str = "catch_up_one",
    ms: int | None = None,
    cron_every: int | None = None,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    every = cron_every if cron_every is not None else CRON_EVERY
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{every} * * * * *",
        "payload": {"kind": "render", "ms": job_ms, "name": name},
        "misfire_policy": policy,
        "catch_up_max": 3,
        "enabled": True,
    }
    if waits_on is not None:
        body["waits_on"] = waits_on
    if joins_on is not None:
        body["joins_on"] = joins_on
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})


def cancel_gen(tid: str, sid: str, iteration: int = 1) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST",
        f"/v1/schedules/{sid}/cancel-generation",
        {"tenant_id": tid, "iteration": iteration},
    )


def replay_gen(tid: str, sid: str, iteration: int = 1) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST",
        f"/v1/schedules/{sid}/replay-generation",
        {"tenant_id": tid, "iteration": iteration},
    )


def pause(tid: str, sid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/schedules/{sid}/pause", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"pause {tid} {code} {body}")


def park_cron(tid: str, sid: str) -> None:
    """Push next_run far away without pausing (paused upstreams block waiters)."""
    code, body = http_json(
        "POST",
        f"/v1/schedules/{sid}/update",
        {"tenant_id": tid, "cron": "0 0 1 1 *"},
    )
    if code != 200:
        raise RuntimeError(f"park cron {tid} {code} {body}")


def freeze(tid: str, jid: str, reason: str) -> None:
    code, body = http_json(
        "POST", f"/v1/jobs/{jid}/freeze", {"tenant_id": tid, "reason": reason}
    )
    if code != 200:
        raise RuntimeError(f"freeze {tid} {code} {body}")


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


def wait_live(tid: str, sid: str) -> dict[str, Any]:
    deadline = time.time() + 20.0
    while time.time() < deadline:
        live = [
            j
            for j in by_sched(list_jobs(tid), sid)
            if j.get("status") in ("pending", "leased", "frozen")
        ]
        if live:
            return live[0]
        time.sleep(0.1)
    raise RuntimeError(f"{tid}/{sid} never had a live job")


def pipe(tid: str, *, w2_ms: int = 2500) -> tuple[str, str, str, str]:
    up = create_sched(tid, name="up")
    w1 = create_sched(tid, name="w1", waits_on=[up])
    w2 = create_sched(tid, name="w2", waits_on=[up], ms=w2_ms)
    join = create_sched(tid, name="join", joins_on=[w1, w2])
    return up, w1, w2, join


def main() -> int:
    logger.info("schedule generation soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    cx_t, rp_t, part_t, live_t = ("sg-cx", "sg-rp", "sg-part", "sg-live")
    cx_up, cx_w1, cx_w2, cx_j = pipe(cx_t)
    rp_up, rp_w1, rp_w2, rp_j = pipe(rp_t)
    part_up, part_w1, part_w2, part_j = pipe(part_t)
    create_sched(live_t, name="live")

    cx_up_job = wait_live(cx_t, cx_up)
    freeze(cx_t, cx_up_job["id"], "hold up")
    # Let waiters stay quiet until we finish U; cancel the generation first.
    code, body = cancel_gen(cx_t, cx_up, 1)
    if code != 200:
        raise RuntimeError(f"cancel cx expected 200 got {code} {body}")
    if cx_up_job["id"] not in (body.get("cancelled_job_ids") or []):
        raise RuntimeError(f"cancel missed upstream job {body}")
    pause(cx_t, cx_up)

    rp_up_job = wait_live(rp_t, rp_up)
    freeze(rp_t, rp_up_job["id"], "hold rp up")
    code, body = cancel_gen(rp_t, rp_up, 1)
    if code != 200:
        raise RuntimeError(f"cancel rp expected 200 got {code} {body}")
    code, body = replay_gen(rp_t, rp_up, 1)
    if code != 201:
        raise RuntimeError(f"replay rp expected 201 got {code} {body}")
    rp_new = (body.get("job") or {}).get("id")
    if not rp_new:
        raise RuntimeError(f"replay produced no upstream job {body}")
    freeze(rp_t, rp_new, "hold replayed up")
    park_cron(rp_t, rp_up)

    part_up_job = wait_live(part_t, part_up)
    freeze(part_t, part_up_job["id"], "hold part up")
    code, body = http_json(
        "POST",
        f"/v1/schedules/{part_w2}/delete",
        {"tenant_id": part_t},
    )
    if code != 200:
        raise RuntimeError(f"delete waiter expected 200 got {code} {body}")
    code, body = replay_gen(part_t, part_up, 1)
    if code != 409:
        raise RuntimeError(
            f"replay after delete waiter expected 409 got {code} {body}"
        )
    code, body = http_json(
        "POST",
        f"/v1/schedules/{part_up}/move",
        {"tenant_id": part_t, "to_tenant_id": "sg-part-dst"},
    )
    if code != 409:
        raise RuntimeError(
            f"move live fan-out expected 409 got {code} {body}"
        )

    for tid, sid in (
        (cx_t, cx_w1),
        (cx_t, cx_j),
        (rp_t, rp_j),
        (part_t, part_j),
    ):
        code, body = fire(tid, sid)
        if code != 409:
            raise RuntimeError(f"fire {tid} expected 409 got {code} {body}")

    cx_w_at = len(by_sched(list_jobs(cx_t), cx_w1)) + len(
        by_sched(list_jobs(cx_t), cx_w2)
    )
    cx_j_at = len(by_sched(list_jobs(cx_t), cx_j))
    rp_j_at = len(by_sched(list_jobs(rp_t), rp_j))
    part_j_at = len(by_sched(list_jobs(part_t), part_j))

    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        cx_jobs = list_jobs(cx_t)
        if len(by_sched(cx_jobs, cx_w1)) + len(by_sched(cx_jobs, cx_w2)) != cx_w_at:
            raise RuntimeError("cancelled generation produced waiter jobs")
        if len(by_sched(cx_jobs, cx_j)) != cx_j_at:
            raise RuntimeError("cancelled generation produced a join job")
        if len(by_sched(list_jobs(rp_t), rp_j)) != rp_j_at:
            raise RuntimeError("replay join fired before upstream completed")
        if len(by_sched(list_jobs(part_t), part_j)) != part_j_at:
            raise RuntimeError("partial set produced a join job")
        code, body = replay_gen(part_t, part_up, 1)
        if code != 409:
            raise RuntimeError(
                f"replay incomplete during hold expected 409 got {code} {body}"
            )
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold cx_w=%d cx_j=%d rp_j=%d part_j=%d live=%d",
                cx_w_at,
                cx_j_at,
                rp_j_at,
                part_j_at,
                completed(list_jobs(live_t)),
            )
            last = now
        time.sleep(0.2)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during generation hold")

    code, body = http_json(
        "POST", f"/v1/jobs/{rp_new}/unfreeze", {"tenant_id": rp_t}
    )
    if code != 200:
        raise RuntimeError(f"unfreeze replayed up {code} {body}")

    deadline = time.time() + 16
    while time.time() < deadline:
        rp_jobs = list_jobs(rp_t)
        if by_sched(rp_jobs, rp_j):
            break
        time.sleep(0.15)
    pause(rp_t, rp_j)
    pause(rp_t, rp_w1)
    pause(rp_t, rp_w2)
    pause(rp_t, rp_up)
    rp_jobs = list_jobs(rp_t)
    if not by_sched(rp_jobs, rp_w1) or not by_sched(rp_jobs, rp_w2):
        raise RuntimeError("replay waiters did not fire after upstream completed")
    rp_joins = by_sched(rp_jobs, rp_j)
    if not rp_joins:
        raise RuntimeError("replay join did not fire after waiters completed")
    if len(rp_joins) > 1:
        raise RuntimeError(f"replay join dumped {len(rp_joins)}")
    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    logger.info(
        "schedule generation soak OK cx_w=%d cx_j=%d rp_join=%d live=%d",
        cx_w_at,
        cx_j_at,
        len(rp_joins),
        live_after,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule generation soak failed")
        sys.exit(1)
