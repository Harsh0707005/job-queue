#!/usr/bin/env python3
"""Fleet soak: downstream waits for an upstream tick / paused mutex peer.

Dual coordinators + dual schedulers + four workers:

  * sw-up / sw-skip  — freeze upstream; skip downstream does not dump.
  * sw-up / sw-one   — same window; catch_up_one is a single pickup.
  * sw-pipe          — upstream in mutex group 'pipe'; paused peer blocks
                       the waiter. Resume peer, waiter may fire once.
  * sw-due           — both due every cron; downstream never beats a
                       tick the upstream has not started or finished.
  * sw-mv            — freeze upstream, move it off the tenant; waiter
                       stays blocked until that tick completes on dest.
  * sw-del           — freeze upstream; delete is 409 and the waiter
                       stays blocked while the tick is live.
  * sw-can           — cancel the upstream tick; waiter stays blocked
                       until a later tick completes successfully.
  * sw-dead          — dead-letter the frozen upstream tick; same gate.
  * sw-live          — other tenant keeps completing.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("schedule_wait_soak")

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


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


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
    waits_on: list[str] | None = None,
    policy: str = "catch_up_one",
    ms: int | None = None,
    enabled: bool = True,
) -> str:
    job_ms = ms if ms is not None else JOB_MS
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
    if waits_on is not None:
        body["waits_on"] = waits_on
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


def move(tid: str, sid: str, dest: str) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST",
        f"/v1/schedules/{sid}/move",
        {"tenant_id": tid, "to_tenant_id": dest},
    )


def delete(tid: str, sid: str) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST", f"/v1/schedules/{sid}/delete", {"tenant_id": tid}
    )


def cancel_job(tid: str, jid: str) -> tuple[int, dict[str, Any]]:
    return http_json("POST", f"/v1/jobs/{jid}/cancel", {"tenant_id": tid})


def dead_letter(tid: str, jid: str) -> tuple[int, dict[str, Any]]:
    return http_json(
        "POST",
        f"/v1/jobs/{jid}/freeze/dead",
        {"tenant_id": tid, "reason": "soak dead-letter"},
    )


def _ts(raw: Any) -> float:
    if not raw:
        return 0.0
    text = str(raw).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def assert_down_after_up(up_jobs: list[dict[str, Any]], down_jobs: list[dict[str, Any]]) -> None:
    """Each downstream tick must follow a successfully completed upstream tick."""
    downs = sorted(down_jobs, key=lambda j: str(j.get("created_at") or ""))
    finished = sorted(
        (
            j
            for j in up_jobs
            if j.get("status") == "completed" and j.get("completed_at")
        ),
        key=lambda j: str(j.get("completed_at") or ""),
    )
    if len(downs) > len(finished):
        raise RuntimeError(
            "downstream tick exists without a successful upstream complete "
            f"(up={len(up_jobs)} done={len(finished)} down={len(downs)})"
        )
    # Dual schedulers can commit a waiter a few ms around the upstream
    # ack; require a completed tick, not a microsecond wall-clock order.
    slack = 0.05
    for i, down in enumerate(downs):
        if _ts(down.get("created_at")) + slack < _ts(finished[i].get("completed_at")):
            raise RuntimeError(
                "downstream created before the matching upstream tick finished "
                f"({down.get('created_at')} < {finished[i].get('completed_at')})"
            )


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
    logger.info("schedule wait soak hold=%ds cron=every %ds", HOLD, CRON_EVERY)
    wait_coords()
    skip_t, one_t, pipe_t, live_t = "sw-skip", "sw-one", "sw-pipe", "sw-live"
    due_t, mv_t, mv_dst, del_t = "sw-due", "sw-mv", "sw-mv-dst", "sw-del"
    can_t, dead_t = "sw-can", "sw-dead"
    skip_up = create_sched(skip_t, name="up", ms=2500)
    skip_down = create_sched(
        skip_t, name="down", waits_on=[skip_up], policy="skip"
    )
    one_up = create_sched(one_t, name="up", ms=2500)
    one_down = create_sched(
        one_t, name="down", waits_on=[one_up], policy="catch_up_one"
    )
    pipe_up = create_sched(pipe_t, name="up", group="pipe", ms=2500)
    pipe_peer = create_sched(
        pipe_t, name="peer", group="pipe", enabled=False
    )
    pipe_down = create_sched(pipe_t, name="down", waits_on=[pipe_up])
    due_up = create_sched(due_t, name="up")
    due_down = create_sched(due_t, name="down", waits_on=[due_up])
    mv_up = create_sched(mv_t, name="up", ms=2500)
    mv_down = create_sched(mv_t, name="down", waits_on=[mv_up])
    del_up = create_sched(del_t, name="up", ms=2500)
    del_down = create_sched(del_t, name="down", waits_on=[del_up])
    can_up = create_sched(can_t, name="up", ms=2500)
    can_down = create_sched(can_t, name="down", waits_on=[can_up])
    dead_up = create_sched(dead_t, name="up", ms=2500)
    dead_down = create_sched(dead_t, name="down", waits_on=[dead_up])
    create_sched(live_t, name="live")

    skip_jobs = wait_n(skip_t, 1, schedule_id=skip_up)
    one_jobs = wait_n(one_t, 1, schedule_id=one_up)
    pipe_jobs = wait_n(pipe_t, 1, schedule_id=pipe_up)
    mv_jobs = wait_n(mv_t, 1, schedule_id=mv_up)
    del_jobs = wait_n(del_t, 1, schedule_id=del_up)
    can_jobs = wait_n(can_t, 1, schedule_id=can_up)
    dead_jobs = wait_n(dead_t, 1, schedule_id=dead_up)
    for tid, job, reason in (
        (skip_t, skip_jobs[0], "hold skip"),
        (one_t, one_jobs[0], "hold one"),
        (pipe_t, pipe_jobs[0], "hold pipe"),
        (mv_t, mv_jobs[0], "hold move"),
        (del_t, del_jobs[0], "hold delete"),
        (can_t, can_jobs[0], "hold cancel"),
        (dead_t, dead_jobs[0], "hold dead"),
    ):
        code, body = http_json(
            "POST",
            f"/v1/jobs/{job['id']}/freeze",
            {"tenant_id": tid, "reason": reason},
        )
        if code != 200:
            raise RuntimeError(f"freeze {tid} {code} {body}")

    code, body = cancel_job(can_t, can_jobs[0]["id"])
    if code != 200:
        raise RuntimeError(f"cancel upstream {code} {body}")
    code, body = dead_letter(dead_t, dead_jobs[0]["id"])
    if code != 200:
        raise RuntimeError(f"dead-letter upstream {code} {body}")

    code, body = move(mv_t, mv_up, mv_dst)
    if code != 200:
        raise RuntimeError(f"move live upstream {code} {body}")
    code, body = delete(del_t, del_up)
    if code != 409:
        raise RuntimeError(f"delete live upstream expected 409 got {code} {body}")

    skip_at = len(list_jobs(skip_t))
    one_at = len(list_jobs(one_t))
    pipe_at = len(by_sched(list_jobs(pipe_t), pipe_down))
    mv_at = len(by_sched(list_jobs(mv_t), mv_down))
    del_at = len(by_sched(list_jobs(del_t), del_down))

    for tid, sid in (
        (skip_t, skip_down),
        (one_t, one_down),
        (pipe_t, pipe_down),
        (mv_t, mv_down),
        (del_t, del_down),
        (can_t, can_down),
        (dead_t, dead_down),
    ):
        code, body = fire(tid, sid)
        if code != 409:
            raise RuntimeError(f"waiter fire {tid} expected 409 got {code} {body}")

    deadline = time.time() + HOLD
    last = 0.0
    while time.time() < deadline:
        if len(list_jobs(skip_t)) != skip_at:
            raise RuntimeError("skip downstream fired while upstream live")
        if len(list_jobs(one_t)) != one_at:
            raise RuntimeError("catch_up_one downstream fired while upstream live")
        if len(by_sched(list_jobs(pipe_t), pipe_down)) != pipe_at:
            raise RuntimeError("pipe waiter fired while peer paused / up live")
        if len(by_sched(list_jobs(mv_t), mv_down)) != mv_at:
            raise RuntimeError("move waiter fired while dest tick still live")
        if len(by_sched(list_jobs(del_t), del_down)) != del_at:
            raise RuntimeError("delete waiter fired while upstream tick live")
        code, body = delete(del_t, del_up)
        if code != 409:
            raise RuntimeError(
                f"delete live upstream during hold expected 409 got {code} {body}"
            )
        due_jobs = list_jobs(due_t)
        assert_down_after_up(by_sched(due_jobs, due_up), by_sched(due_jobs, due_down))
        can_all = list_jobs(can_t)
        dead_all = list_jobs(dead_t)
        assert_down_after_up(by_sched(can_all, can_up), by_sched(can_all, can_down))
        assert_down_after_up(by_sched(dead_all, dead_up), by_sched(dead_all, dead_down))
        now = time.time()
        if now - last >= 2:
            logger.info(
                "hold skip=%d one=%d pipe_down=%d due_up=%d due_down=%d "
                "mv_down=%d del_down=%d can_down=%d dead_down=%d live=%d",
                len(list_jobs(skip_t)),
                len(list_jobs(one_t)),
                len(by_sched(list_jobs(pipe_t), pipe_down)),
                len(by_sched(due_jobs, due_up)),
                len(by_sched(due_jobs, due_down)),
                len(by_sched(list_jobs(mv_t), mv_down)),
                len(by_sched(list_jobs(del_t), del_down)),
                len(by_sched(can_all, can_down)),
                len(by_sched(dead_all, dead_down)),
                completed(list_jobs(live_t)),
            )
            last = now
        time.sleep(0.2)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError("live tenant stalled during wait hold")

    pause(due_t, due_down)
    pause(due_t, due_up)
    due_jobs = list_jobs(due_t)
    assert_down_after_up(by_sched(due_jobs, due_up), by_sched(due_jobs, due_down))
    if not by_sched(due_jobs, due_up):
        raise RuntimeError("due-order tenant never fired the upstream")

    can_all = list_jobs(can_t)
    dead_all = list_jobs(dead_t)
    assert_down_after_up(by_sched(can_all, can_up), by_sched(can_all, can_down))
    assert_down_after_up(by_sched(dead_all, dead_up), by_sched(dead_all, dead_down))
    live_up = {"pending", "leased", "frozen", "awaiting_approval",
               "awaiting_children", "awaiting_skills"}
    for tid, sid, jobs in (
        (can_t, can_up, by_sched(can_all, can_up)),
        (dead_t, dead_up, by_sched(dead_all, dead_up)),
    ):
        if any(j.get("status") == "completed" for j in jobs):
            continue
        if any(j.get("status") in live_up for j in jobs):
            continue
        code, body = fire(tid, sid)
        if code != 200:
            raise RuntimeError(f"fire {tid} upstream after fail {code} {body}")

    # Stop dest / source upstreams from starting another tick after
    # release. A paused same-tenant upstream still blocks the waiter
    # until delete; dest pause does not, once the moved job completes.
    pause(mv_dst, mv_up)
    pause(del_t, del_up)

    # Unfreeze upstreams and let those ticks finish. Do not pause them
    # first — a paused upstream also blocks the waiter. Pipe peer stays
    # paused so pipe_down remains blocked after up goes idle. Moved
    # tick is frozen on dest; deleted-row tick stays on the source.
    for tid, jid in (
        (skip_t, skip_jobs[0]["id"]),
        (one_t, one_jobs[0]["id"]),
        (pipe_t, pipe_jobs[0]["id"]),
        (mv_dst, mv_jobs[0]["id"]),
        (del_t, del_jobs[0]["id"]),
    ):
        code, body = http_json(
            "POST", f"/v1/jobs/{jid}/unfreeze", {"tenant_id": tid}
        )
        if code != 200:
            raise RuntimeError(f"unfreeze {tid} {code} {body}")

    # Upstream ticks finish; skip/one waiters are overdue. Pipe peer is
    # still paused so pipe_down stays blocked.
    expected = max(1, HOLD // CRON_EVERY)
    deadline = time.time() + 16
    skip_sched: dict[str, Any] = {}
    extra_one = 0
    while time.time() < deadline:
        skip_sched = get_sched(skip_down, skip_t)
        extra_one = len(by_sched(list_jobs(one_t), one_down))
        missed = int(skip_sched.get("missed_ticks") or 0)
        if missed >= expected - 1 and extra_one >= 1:
            break
        time.sleep(0.15)
    if len(by_sched(list_jobs(pipe_t), pipe_down)) != pipe_at:
        raise RuntimeError("pipe waiter fired while mutex peer still paused")

    pause(skip_t, skip_down)
    pause(one_t, one_down)
    extra_skip = len(by_sched(list_jobs(skip_t), skip_down))
    if extra_skip > 1:
        raise RuntimeError(f"skip dumped {extra_skip} after upstream finished")
    extra_one = len(by_sched(list_jobs(one_t), one_down))
    if extra_one < 1:
        raise RuntimeError("catch_up_one produced no pickup after wait")
    if extra_one > expected:
        raise RuntimeError(f"catch_up_one flooded {extra_one}")

    deadline = time.time() + 10
    dest_done = 0
    del_done = 0
    while time.time() < deadline:
        dest_done = completed(by_sched(list_jobs(mv_dst), mv_up))
        del_done = completed(by_sched(list_jobs(del_t), del_up))
        if dest_done >= 1 and del_done >= 1:
            break
        time.sleep(0.15)
    if dest_done < 1:
        raise RuntimeError("moved upstream tick never finished on dest")
    if del_done < 1:
        raise RuntimeError("delete-tenant upstream tick never finished")
    if by_sched(list_jobs(del_t), del_down):
        raise RuntimeError("delete waiter fired while paused upstream still exists")

    code, body = delete(del_t, del_up)
    if code != 200:
        raise RuntimeError(f"delete idle upstream after tick expected 200 got {code} {body}")

    deadline = time.time() + 16
    mv_ready = del_ready = False
    while time.time() < deadline:
        if not mv_ready and by_sched(list_jobs(mv_t), mv_down):
            pause(mv_t, mv_down)
            mv_ready = True
        if not del_ready and by_sched(list_jobs(del_t), del_down):
            pause(del_t, del_down)
            del_ready = True
        if mv_ready and del_ready:
            break
        time.sleep(0.15)
    mv_extra = len(by_sched(list_jobs(mv_t), mv_down))
    del_extra = len(by_sched(list_jobs(del_t), del_down))
    if mv_extra < 1:
        raise RuntimeError("move waiter did not fire after dest tick finished")
    if mv_extra > expected:
        raise RuntimeError(f"move waiter flooded {mv_extra}")
    if del_extra < 1:
        raise RuntimeError("delete waiter did not fire after upstream was removed")
    if del_extra > expected:
        raise RuntimeError(f"delete waiter flooded {del_extra}")

    resume(pipe_t, pipe_peer)
    resume(pipe_t, pipe_up)
    deadline = time.time() + 16
    while time.time() < deadline:
        if by_sched(list_jobs(pipe_t), pipe_down):
            break
        time.sleep(0.15)
    pause(pipe_t, pipe_down)
    pipe_extra = len(by_sched(list_jobs(pipe_t), pipe_down))
    if pipe_extra < 1:
        raise RuntimeError("pipe waiter did not fire after peer resume")
    if pipe_extra > expected:
        raise RuntimeError(f"pipe waiter flooded {pipe_extra}")

    deadline = time.time() + 16
    can_ready = dead_ready = False
    while time.time() < deadline:
        can_all = list_jobs(can_t)
        dead_all = list_jobs(dead_t)
        assert_down_after_up(by_sched(can_all, can_up), by_sched(can_all, can_down))
        assert_down_after_up(by_sched(dead_all, dead_up), by_sched(dead_all, dead_down))
        if not can_ready and by_sched(can_all, can_down):
            pause(can_t, can_down)
            can_ready = True
        if not dead_ready and by_sched(dead_all, dead_down):
            pause(dead_t, dead_down)
            dead_ready = True
        if can_ready and dead_ready:
            break
        time.sleep(0.15)
    can_extra = len(by_sched(list_jobs(can_t), can_down))
    dead_extra = len(by_sched(list_jobs(dead_t), dead_down))
    if can_extra < 1:
        raise RuntimeError("cancel waiter did not fire after a successful tick")
    if can_extra > expected:
        raise RuntimeError(f"cancel waiter flooded {can_extra}")
    if dead_extra < 1:
        raise RuntimeError("dead-letter waiter did not fire after a successful tick")
    if dead_extra > expected:
        raise RuntimeError(f"dead-letter waiter flooded {dead_extra}")

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")
    logger.info(
        "schedule wait soak OK extra_skip=%d extra_one=%d pipe=%d "
        "due_up=%d due_down=%d mv=%d del=%d can=%d dead=%d missed=%s live=%d",
        extra_skip,
        extra_one,
        pipe_extra,
        len(by_sched(due_jobs, due_up)),
        len(by_sched(due_jobs, due_down)),
        mv_extra,
        del_extra,
        can_extra,
        dead_extra,
        skip_sched.get("missed_ticks"),
        live_after,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("schedule wait soak failed")
        sys.exit(1)
