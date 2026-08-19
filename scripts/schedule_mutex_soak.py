#!/usr/bin/env python3
"""Fleet soak: mutually exclusive schedule groups under dual schedulers.

Dual coordinators + dual schedulers + four workers:

  * sg-held   — two DAG members of group 'blue'; freeze the winner so the
                peer cannot tick. Ungrouped sibling on the same tenant
                keeps firing. Delete the waiter; holder still finishes.
  * sg-skip   — peer blocked for several 2s ticks; after release, skip
                does not dump a backlog.
  * sg-one    — same window; catch_up_one is a single pickup.
  * sg-peer   — other tenant, same group name 'blue'; fires independently.
  * sg-live   — ungrouped control tenant keeps completing.
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
logger = logging.getLogger("schedule_mutex_soak")

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


def statuses(jobs: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for j in jobs:
        key = str(j.get("status") or "?")
        out[key] = out.get(key, 0) + 1
    return out


def by_sched(jobs: list[dict[str, Any]], sid: str) -> list[dict[str, Any]]:
    return [j for j in jobs if j.get("schedule_id") == sid]


def get_sched(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get {sid} {code} {body}")
    return body.get("schedule") or {}


def create_sched(
    tid: str,
    *,
    name: str,
    group: str | None = None,
    dag: bool = False,
    policy: str = "catch_up_one",
    prep_ms: int | None = None,
    ms: int | None = None,
    enabled: bool = True,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
    root_ms = prep_ms if prep_ms is not None else job_ms
    body: dict[str, Any] = {
        "tenant_id": tid,
        "name": name,
        "cron": f"*/{CRON_EVERY} * * * * *",
        "payload": {"kind": "render", "ms": job_ms, "name": name},
        "misfire_policy": policy,
        "catch_up_max": 3,
        "enabled": enabled,
    }
    if group is not None:
        body["mutex_group"] = group
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
    code, resp = http_json("POST", "/v1/schedules", body)
    if code != 201:
        raise RuntimeError(f"create {tid}/{name} {code} {resp}")
    return str(resp["schedule"]["id"])


def delete(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/delete", {"tenant_id": tid})


def pause(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/pause", {"tenant_id": tid})


def resume(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/resume", {"tenant_id": tid})


def list_schedules(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/schedules?tenant_id={tid}&limit=100")
    if code != 200:
        raise RuntimeError(f"list schedules {tid} {code} {body}")
    return list(body.get("schedules") or [])


def fire(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid})


def wait_group_winner(
    tid: str, sids: list[str], n: int, *, timeout: float = 20.0
) -> tuple[str, list[dict[str, Any]]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = list_jobs(tid)
        for sid in sids:
            owned = by_sched(jobs, sid)
            if len(owned) >= n:
                return sid, owned
        time.sleep(0.1)
    raise RuntimeError(f"{tid} no mutex winner reached {n} jobs")


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
            jobs = by_sched(jobs, schedule_id)
        if completed(jobs) >= n:
            return jobs
        time.sleep(0.15)
    raise RuntimeError(
        f"{tid} never completed {n} (had {completed(jobs)} of {len(jobs)} "
        f"{statuses(jobs)})"
    )


def main() -> int:
    logger.info(
        "schedule mutex soak hold=%ds cron=every %ds", HOLD, CRON_EVERY
    )
    wait_coords()
    held_t, skip_t, one_t, peer_t, live_t = (
        "sg-held",
        "sg-skip",
        "sg-one",
        "sg-peer",
        "sg-live",
    )
    held_a = create_sched(held_t, name="held-a", group="blue", dag=True, prep_ms=3000)
    held_b = create_sched(held_t, name="held-b", group="blue", dag=True)
    sib_id = create_sched(held_t, name="sib")
    skip_a = create_sched(
        skip_t, name="skip-a", group="gate", policy="skip", ms=2500
    )
    skip_b = create_sched(
        skip_t, name="skip-b", group="gate", policy="skip", ms=2500
    )
    one_a = create_sched(
        one_t, name="one-a", group="gate", policy="catch_up_one", ms=2500
    )
    one_b = create_sched(
        one_t, name="one-b", group="gate", policy="catch_up_one", ms=2500
    )
    create_sched(peer_t, name="peer", group="blue")
    create_sched(live_t, name="live")

    # Export/restore must keep mutex_group. Dest starts disabled so the
    # source cannot race the snapshot; after restore we resume both.
    ex_t, im_t = "sg-ex", "sg-im"
    create_sched(ex_t, name="ex-a", group="pack", enabled=False, ms=2000)
    create_sched(ex_t, name="ex-b", group="pack", enabled=False, ms=2000)
    code, exp = http_json("POST", f"/v1/tenants/{ex_t}/export", {})
    if code != 200:
        raise RuntimeError(f"export {code} {exp}")
    snap_id = str((exp.get("snapshot") or {}).get("id") or "")
    if not snap_id:
        raise RuntimeError(f"export missing snapshot {exp}")
    code, rst = http_json(
        "POST",
        f"/v1/tenants/{im_t}/restore",
        {"snapshot_id": snap_id, "run_until_complete": True, "batch_size": 50},
        timeout=60.0,
    )
    if code != 200:
        raise RuntimeError(f"restore {code} {rst}")
    if (rst.get("restore") or {}).get("status") != "complete":
        raise RuntimeError(f"restore not complete {rst}")
    imported = list_schedules(im_t)
    if len(imported) != 2:
        raise RuntimeError(f"restore schedules {imported}")
    if {s.get("mutex_group") for s in imported} != {"pack"}:
        raise RuntimeError(f"restore dropped mutex_group {imported}")
    for sched in imported:
        r_code, r_body = resume(im_t, str(sched["id"]))
        if r_code != 200:
            raise RuntimeError(f"resume imported {r_code} {r_body}")
    im_ids = [str(s["id"]) for s in imported]
    im_win, im_first = wait_group_winner(im_t, im_ids, 1, timeout=10)
    hold_im = im_first[0]
    fz_code, fz_body = http_json(
        "POST",
        f"/v1/jobs/{hold_im['id']}/freeze",
        {"tenant_id": im_t, "reason": "hold restored"},
    )
    if fz_code != 200 or (fz_body.get("job") or {}).get("status") != "frozen":
        raise RuntimeError(f"freeze restored {fz_code} {fz_body}")
    for sid in im_ids:
        p_code, p_body = pause(im_t, sid)
        if p_code != 200:
            raise RuntimeError(f"pause imported {sid} {p_code} {p_body}")
    im_jobs = list_jobs(im_t)
    im_by: dict[str, int] = {}
    for job in im_jobs:
        sid = str(job.get("schedule_id") or "")
        im_by[sid] = im_by.get(sid, 0) + 1
    if len(im_by) != 1 or im_win not in im_by:
        raise RuntimeError(f"restore lost exclusivity {im_by} winner={im_win}")
    im_waiter = next(sid for sid in im_ids if sid != im_win)
    f_code, f_body = fire(im_t, im_waiter)
    if f_code != 409:
        raise RuntimeError(f"restored waiter fire {f_code} {f_body}")
    logger.info(
        "restored mutex pack winner=%s n=%d waiter_fire=409",
        im_win,
        im_by[im_win],
    )

    winner, held_nodes = wait_group_winner(held_t, [held_a, held_b], DIAMOND)
    waiter = held_b if winner == held_a else held_a
    freezeable = [
        j
        for j in held_nodes
        if j.get("status") in ("pending", "leased", "awaiting_skills")
    ]
    if not freezeable:
        raise RuntimeError(f"held already finished: {statuses(held_nodes)}")
    hold = freezeable[-1]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{hold['id']}/freeze",
        {"tenant_id": held_t, "reason": "hold mutex"},
    )
    if code != 200 or (body.get("job") or {}).get("status") != "frozen":
        raise RuntimeError(f"freeze {code} {body}")
    logger.info("winner=%s froze %s waiter=%s", winner, hold["id"], waiter)

    skip_jobs = wait_n(skip_t, 1)
    skip_winner = str(skip_jobs[0]["schedule_id"])
    skip_waiter = skip_b if skip_winner == skip_a else skip_a
    skip_hold = skip_jobs[0]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{skip_hold['id']}/freeze",
        {"tenant_id": skip_t, "reason": "hold skip"},
    )
    if code != 200:
        raise RuntimeError(f"skip freeze {code} {body}")
    p_code, p_body = pause(skip_t, skip_winner)
    if p_code != 200:
        raise RuntimeError(f"pause skip winner {p_code} {p_body}")

    one_jobs = wait_n(one_t, 1)
    one_winner = str(one_jobs[0]["schedule_id"])
    one_waiter = one_b if one_winner == one_a else one_a
    one_hold = one_jobs[0]
    code, body = http_json(
        "POST",
        f"/v1/jobs/{one_hold['id']}/freeze",
        {"tenant_id": one_t, "reason": "hold one"},
    )
    if code != 200:
        raise RuntimeError(f"one freeze {code} {body}")
    p_code, p_body = pause(one_t, one_winner)
    if p_code != 200:
        raise RuntimeError(f"pause one winner {p_code} {p_body}")

    skip_pol = get_sched(skip_waiter, skip_t).get("misfire_policy")
    one_pol = get_sched(one_waiter, one_t).get("misfire_policy")
    if skip_pol != "skip" or one_pol != "catch_up_one":
        raise RuntimeError(f"policies skip={skip_pol} one={one_pol}")

    skip_at = len(list_jobs(skip_t))
    one_at = len(list_jobs(one_t))

    # Fail-closed fire on the waiter; pause/delete must not hang the tenant.
    f_code, f_body = fire(held_t, waiter)
    if f_code != 409 or f_body.get("code") != "schedule_error":
        raise RuntimeError(f"waiter fire expected 409, got {f_code} {f_body}")
    d_code, d_body = delete(held_t, waiter)
    if d_code != 200 or not d_body.get("deleted"):
        raise RuntimeError(f"delete waiter {d_code} {d_body}")
    p_code, p_body = pause(held_t, winner)
    if p_code != 200:
        raise RuntimeError(f"pause winner {p_code} {p_body}")
    logger.info("deleted waiter, paused winner; fire was 409")

    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        held_now = list_jobs(held_t)
        if len(by_sched(held_now, winner)) != DIAMOND:
            raise RuntimeError(
                f"held double-fired n={len(by_sched(held_now, winner))}"
            )
        if by_sched(held_now, waiter):
            raise RuntimeError("deleted waiter still grew jobs")
        if len(list_jobs(skip_t)) != skip_at:
            raise RuntimeError(
                f"skip group grew while held {len(list_jobs(skip_t))} was {skip_at}"
            )
        if len(list_jobs(one_t)) != one_at:
            raise RuntimeError(
                f"one group grew while held {len(list_jobs(one_t))} was {one_at}"
            )
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold sib=%d peer=%d live=%d skip=%d one=%d",
                len(by_sched(list_jobs(held_t), sib_id)),
                completed(list_jobs(peer_t)),
                completed(list_jobs(live_t)),
                len(list_jobs(skip_t)),
                len(list_jobs(one_t)),
            )
            last = now
        time.sleep(0.2)

    min_live = max(2, HOLD // CRON_EVERY - 1)
    sib_mid = len(by_sched(list_jobs(held_t), sib_id))
    if sib_mid < min_live:
        raise RuntimeError(
            f"ungrouped sibling stalled while mutex held sib={sib_mid}"
        )
    peer_mid = completed(list_jobs(peer_t))
    if peer_mid < min_live:
        raise RuntimeError(
            f"other tenant same group name stalled peer={peer_mid}"
        )
    live_mid = completed(list_jobs(live_t))
    if live_mid < min_live:
        raise RuntimeError(f"live tenant stalled completed={live_mid}")
    logger.info(
        "refuse window sib=%d peer=%d live=%d", sib_mid, peer_mid, live_mid
    )

    # Dual-coordinator fire race against a still-frozen winner.
    race_codes: list[int] = []

    def hammer() -> None:
        c, _b = fire(held_t, winner)
        race_codes.append(c)

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    if any(c not in (409,) for c in race_codes):
        # Winner is paused, so fire is "disabled" (409) or mutex 409.
        bad = [c for c in race_codes if c not in (409,)]
        if bad:
            raise RuntimeError(f"hammer fire codes {race_codes}")

    for tid, jid in (
        (held_t, hold["id"]),
        (skip_t, skip_hold["id"]),
        (one_t, one_hold["id"]),
    ):
        code, body = http_json(
            "POST", f"/v1/jobs/{jid}/unfreeze", {"tenant_id": tid}
        )
        if code != 200:
            raise RuntimeError(f"unfreeze {tid} {code} {body}")

    wait_done(held_t, DIAMOND, schedule_id=winner)
    wait_done(skip_t, 1, schedule_id=skip_winner)
    wait_done(one_t, 1, schedule_id=one_winner)

    # Winners stay paused so they cannot contribute live ticks. Waiters
    # are overdue; misfire must run without dumping a backlog.
    expected = max(1, HOLD // CRON_EVERY)
    deadline = time.time() + 8
    skip_sched: dict[str, Any] = {}
    one_extra = 0
    while time.time() < deadline:
        skip_sched = get_sched(skip_waiter, skip_t)
        one_extra = len(list_jobs(one_t)) - one_at
        missed = int(skip_sched.get("missed_ticks") or 0)
        if missed >= expected - 1 and one_extra >= 1:
            break
        time.sleep(0.15)
    pause(skip_t, skip_waiter)
    pause(one_t, one_waiter)

    extra_skip = len(list_jobs(skip_t)) - skip_at
    if extra_skip > 1:
        raise RuntimeError(
            f"skip dumped {extra_skip} jobs after mutex release "
            f"jobs={statuses(list_jobs(skip_t))} missed={skip_sched.get('missed_ticks')}"
        )
    extra_one = len(list_jobs(one_t)) - one_at
    if extra_one < 1:
        raise RuntimeError("catch_up_one produced no pickup after mutex release")
    if extra_one > expected:
        raise RuntimeError(f"catch_up_one flooded {extra_one}")
    logger.info(
        "misfire extra_skip=%d extra_one=%d skip_missed=%s",
        extra_skip,
        extra_one,
        skip_sched.get("missed_ticks"),
    )

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    peer_after = completed(list_jobs(peer_t))
    if peer_after < peer_mid:
        raise RuntimeError("peer tenant went backwards")
    sib_after = len(by_sched(list_jobs(held_t), sib_id))
    if sib_after < sib_mid:
        raise RuntimeError("sibling went backwards")

    logger.info(
        "schedule mutex soak OK sib=%d peer=%d live=%d extra_one=%d restored=%s",
        sib_after,
        peer_after,
        live_after,
        extra_one,
        im_by,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule mutex soak failed")
        sys.exit(1)
