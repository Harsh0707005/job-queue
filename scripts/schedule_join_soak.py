#!/usr/bin/env python3
"""Fleet soak: upstream fans out to waiters; join waits for the same iteration.

Dual coordinators + dual schedulers + four workers:

  * sj-hold   — freeze one waiter; join stays blocked (409) during the hold.
  * sj-pause  — pause a waiter; join stays blocked until resume + complete.
  * sj-mutex  — waiter shares a mutex with a paused peer; join stays blocked.
  * sj-skip   — join skip does not dump after waiters finish a long hold.
  * sj-cup    — join catch_up behind many waiter generations; at most
                catch_up_max ticks, extras recorded as misses.
  * sj-can    — cancel the upstream tick; that generation gives waiters
                and the join nothing.
  * sj-mv     — move of a live fan-out upstream is refused (no leak).
  * sj-live   — other tenant keeps completing.
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
logger = logging.getLogger("schedule_join_soak")

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


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def by_sched(jobs: list[dict[str, Any]], sid: str) -> list[dict[str, Any]]:
    return [j for j in jobs if j.get("schedule_id") == sid]


def create_sched(
    tid: str,
    *,
    name: str,
    group: str | None = None,
    waits_on: list[str] | None = None,
    joins_on: list[str] | None = None,
    policy: str = "catch_up_one",
    catch_up_max: int = 3,
    cron_every: int | None = None,
    ms: int | None = None,
    enabled: bool = True,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    every = cron_every if cron_every is not None else CRON_EVERY
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{every} * * * * *",
        "payload": {"kind": "render", "ms": job_ms, "name": name},
        "misfire_policy": policy,
        "catch_up_max": catch_up_max,
        "enabled": enabled,
    }
    if group is not None:
        body["mutex_group"] = group
    if waits_on is not None:
        body["waits_on"] = waits_on
    if joins_on is not None:
        body["joins_on"] = joins_on
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


def get_sched(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get {sid} {code} {body}")
    return body.get("schedule") or {}


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


def freeze(tid: str, jid: str, reason: str) -> None:
    code, body = http_json(
        "POST", f"/v1/jobs/{jid}/freeze", {"tenant_id": tid, "reason": reason}
    )
    if code != 200:
        raise RuntimeError(f"freeze {tid} {code} {body}")


def unfreeze(tid: str, jid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/jobs/{jid}/unfreeze", {"tenant_id": tid}
    )
    if code != 200:
        raise RuntimeError(f"unfreeze {tid} {code} {body}")


def assert_join_after_waiters(
    w1: list[dict[str, Any]],
    w2: list[dict[str, Any]],
    join: list[dict[str, Any]],
) -> None:
    done = min(
        sum(1 for j in w1 if j.get("status") == "completed"),
        sum(1 for j in w2 if j.get("status") == "completed"),
    )
    if len(join) > done:
        raise RuntimeError(
            f"join ran {len(join)} times with only {done} waiter generations done"
        )


def main() -> int:
    logger.info("schedule join soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    hold_t, pause_t, mutex_t, skip_t, cup_t, can_t, mv_t, live_t = (
        "sj-hold",
        "sj-pause",
        "sj-mutex",
        "sj-skip",
        "sj-cup",
        "sj-can",
        "sj-mv",
        "sj-live",
    )

    def pipe(tid: str, *, policy: str = "catch_up_one", w2_ms: int = 2500) -> tuple[str, str, str, str]:
        up = create_sched(tid, name="up")
        w1 = create_sched(tid, name="w1", waits_on=[up])
        w2 = create_sched(tid, name="w2", waits_on=[up], ms=w2_ms)
        join = create_sched(tid, name="join", joins_on=[w1, w2], policy=policy)
        return up, w1, w2, join

    hold_up, hold_w1, hold_w2, hold_j = pipe(hold_t)
    pause_up = create_sched(pause_t, name="up")
    pause_w1 = create_sched(pause_t, name="w1", waits_on=[pause_up])
    pause_w2 = create_sched(
        pause_t, name="w2", waits_on=[pause_up], enabled=False
    )
    pause_j = create_sched(
        pause_t, name="join", joins_on=[pause_w1, pause_w2]
    )
    mutex_up = create_sched(mutex_t, name="up")
    mutex_w1 = create_sched(mutex_t, name="w1", waits_on=[mutex_up])
    mutex_w2 = create_sched(
        mutex_t, name="w2", waits_on=[mutex_up], group="pipe"
    )
    mutex_peer = create_sched(
        mutex_t, name="peer", group="pipe", ms=2500
    )
    mutex_j = create_sched(
        mutex_t, name="join", joins_on=[mutex_w1, mutex_w2]
    )
    skip_up, skip_w1, skip_w2, skip_j = pipe(skip_t, policy="skip")
    cup_up = create_sched(cup_t, name="up", cron_every=1, ms=JOB_MS)
    cup_w1 = create_sched(
        cup_t, name="w1", waits_on=[cup_up], cron_every=1, ms=JOB_MS
    )
    cup_w2 = create_sched(
        cup_t, name="w2", waits_on=[cup_up], cron_every=1, ms=JOB_MS
    )
    cup_j = create_sched(
        cup_t,
        name="join",
        joins_on=[cup_w1, cup_w2],
        policy="catch_up",
        catch_up_max=3,
        cron_every=1,
        enabled=False,
    )
    can_up = create_sched(can_t, name="up", ms=2500)
    can_w1 = create_sched(can_t, name="w1", waits_on=[can_up])
    can_w2 = create_sched(can_t, name="w2", waits_on=[can_up])
    can_j = create_sched(can_t, name="join", joins_on=[can_w1, can_w2])
    mv_up = create_sched(mv_t, name="up", ms=2500)
    mv_w1 = create_sched(mv_t, name="w1", waits_on=[mv_up])
    mv_w2 = create_sched(mv_t, name="w2", waits_on=[mv_up])
    mv_j = create_sched(mv_t, name="join", joins_on=[mv_w1, mv_w2])
    create_sched(live_t, name="live")

    def wait_live(tid: str, sid: str) -> dict[str, Any]:
        deadline = time.time() + 20.0
        while time.time() < deadline:
            live = [
                j
                for j in by_sched(list_jobs(tid), sid)
                if j.get("status") in ("pending", "leased")
            ]
            if live:
                return live[0]
            time.sleep(0.1)
        raise RuntimeError(f"{tid}/{sid} never had a live job")

    hold_job = wait_live(hold_t, hold_w2)
    freeze(hold_t, hold_job["id"], "hold waiter")
    skip_job = wait_live(skip_t, skip_w2)
    freeze(skip_t, skip_job["id"], "skip waiter")
    peer_job = wait_live(mutex_t, mutex_peer)
    freeze(mutex_t, peer_job["id"], "mutex peer")
    can_job = wait_live(can_t, can_up)
    freeze(can_t, can_job["id"], "cancel up")
    code, body = http_json(
        "POST", f"/v1/jobs/{can_job['id']}/cancel", {"tenant_id": can_t}
    )
    if code != 200:
        raise RuntimeError(f"cancel upstream {code} {body}")
    pause(can_t, can_up)
    mv_job = wait_live(mv_t, mv_up)
    freeze(mv_t, mv_job["id"], "move up")
    code, body = http_json(
        "POST",
        f"/v1/schedules/{mv_up}/move",
        {"tenant_id": mv_t, "to_tenant_id": "sj-mv-dst"},
    )
    if code != 409:
        raise RuntimeError(f"move live fan-out expected 409 got {code} {body}")

    for tid, sid in (
        (hold_t, hold_j),
        (pause_t, pause_j),
        (mutex_t, mutex_j),
        (skip_t, skip_j),
        (can_t, can_w1),
        (can_t, can_j),
        (mv_t, mv_j),
    ):
        code, body = fire(tid, sid)
        if code != 409:
            raise RuntimeError(f"join fire {tid} expected 409 got {code} {body}")

    hold_at = len(by_sched(list_jobs(hold_t), hold_j))
    pause_at = len(by_sched(list_jobs(pause_t), pause_j))
    mutex_at = len(by_sched(list_jobs(mutex_t), mutex_j))
    skip_at = len(by_sched(list_jobs(skip_t), skip_j))
    can_w_at = len(by_sched(list_jobs(can_t), can_w1)) + len(
        by_sched(list_jobs(can_t), can_w2)
    )
    can_j_at = len(by_sched(list_jobs(can_t), can_j))

    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        if len(by_sched(list_jobs(hold_t), hold_j)) != hold_at:
            raise RuntimeError("hold join fired while a waiter was frozen")
        if len(by_sched(list_jobs(pause_t), pause_j)) != pause_at:
            raise RuntimeError("pause join fired while a waiter was paused")
        if len(by_sched(list_jobs(mutex_t), mutex_j)) != mutex_at:
            raise RuntimeError("mutex join fired while a waiter was stuck")
        if len(by_sched(list_jobs(skip_t), skip_j)) != skip_at:
            raise RuntimeError("skip join fired while a waiter was frozen")
        can_now = len(by_sched(list_jobs(can_t), can_w1)) + len(
            by_sched(list_jobs(can_t), can_w2)
        )
        if can_now != can_w_at:
            raise RuntimeError("cancelled generation produced waiter jobs")
        if len(by_sched(list_jobs(can_t), can_j)) != can_j_at:
            raise RuntimeError("cancelled generation produced a join job")
        code, body = http_json(
            "POST",
            f"/v1/schedules/{mv_up}/move",
            {"tenant_id": mv_t, "to_tenant_id": "sj-mv-dst"},
        )
        if code != 409:
            raise RuntimeError(
                f"move live fan-out during hold expected 409 got {code} {body}"
            )
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold join=%d pause=%d mutex=%d skip=%d cup_w=%d live=%d",
                hold_at,
                pause_at,
                mutex_at,
                skip_at,
                min(
                    completed(by_sched(list_jobs(cup_t), cup_w1)),
                    completed(by_sched(list_jobs(cup_t), cup_w2)),
                ),
                completed(list_jobs(live_t)),
            )
            last = now
        time.sleep(0.2)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during join hold")

    unfreeze(hold_t, hold_job["id"])
    unfreeze(skip_t, skip_job["id"])
    unfreeze(mutex_t, peer_job["id"])
    resume(pause_t, pause_w2)
    resume(cup_t, cup_j)
    pause(mutex_t, mutex_peer)

    expected = max(1, HOLD // CRON_EVERY)
    deadline = time.time() + 16
    while time.time() < deadline:
        if (
            by_sched(list_jobs(hold_t), hold_j)
            and by_sched(list_jobs(pause_t), pause_j)
            and by_sched(list_jobs(mutex_t), mutex_j)
            and len(by_sched(list_jobs(cup_t), cup_j)) >= 3
        ):
            break
        time.sleep(0.15)
    pause(hold_t, hold_j)
    pause(pause_t, pause_j)
    pause(mutex_t, mutex_j)
    pause(skip_t, skip_j)
    pause(cup_t, cup_j)
    pause(cup_t, cup_up)
    pause(cup_t, cup_w1)
    pause(cup_t, cup_w2)
    hold_extra = len(by_sched(list_jobs(hold_t), hold_j))
    pause_extra = len(by_sched(list_jobs(pause_t), pause_j))
    mutex_extra = len(by_sched(list_jobs(mutex_t), mutex_j))
    skip_extra = len(by_sched(list_jobs(skip_t), skip_j))
    if hold_extra < 1:
        raise RuntimeError("hold join did not fire after waiter completed")
    if pause_extra < 1:
        raise RuntimeError("pause join did not fire after waiter resume")
    if mutex_extra < 1:
        raise RuntimeError("mutex join did not fire after peer/waiter cleared")
    if skip_extra > 1:
        raise RuntimeError(f"skip join dumped {skip_extra}")
    if hold_extra > expected + 1:
        raise RuntimeError(f"hold join flooded {hold_extra}")
    cup_jobs = list_jobs(cup_t)
    cup_extra = len(by_sched(cup_jobs, cup_j))
    cup_ready = min(
        completed(by_sched(cup_jobs, cup_w1)),
        completed(by_sched(cup_jobs, cup_w2)),
    )
    if cup_ready < 6:
        raise RuntimeError(
            f"catch_up join not far enough behind waiters (ready={cup_ready})"
        )
    if cup_extra > 3:
        raise RuntimeError(
            f"catch_up join dumped {cup_extra} ticks past catch_up_max=3 "
            f"(waiters completed {cup_ready})"
        )
    if cup_extra < 1:
        raise RuntimeError("catch_up join produced no pickup after resume")
    cup_sched = get_sched(cup_j, cup_t)
    missed = int(cup_sched.get("missed_ticks") or 0)
    if missed < cup_ready - 3:
        raise RuntimeError(
            f"catch_up join missed {missed}, expected at least {cup_ready - 3} "
            f"leftover generations"
        )
    jobs = list_jobs(hold_t)
    assert_join_after_waiters(
        by_sched(jobs, hold_w1), by_sched(jobs, hold_w2), by_sched(jobs, hold_j)
    )
    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    logger.info(
        "schedule join soak OK hold=%d pause=%d mutex=%d skip=%d "
        "cup=%d cup_ready=%d missed=%d live=%d",
        hold_extra,
        pause_extra,
        mutex_extra,
        skip_extra,
        cup_extra,
        cup_ready,
        missed,
        live_after,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule join soak failed")
        sys.exit(1)
