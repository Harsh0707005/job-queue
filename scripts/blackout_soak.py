#!/usr/bin/env python3
"""Fleet soak: tenant + schedule blackouts under dual coordinators.

  * Dual coordinators + dual schedulers + four workers.
  * Tenant windows (skip / catch_up_one / catch_up) enqueue nothing.
  * A live tenant keeps firing through the window.
  * One schedule on a shared tenant is blacked out; its sibling still fires.
  * Overlapping windows are flipped/extended from two threads; no double fire.
  * After windows end, misfire policy applies (skip / one / catch_up cap).
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
logger = logging.getLogger("blackout_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
JOB_MS = int(os.environ.get("JOB_MS", "20") or "20")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10") or "10")
CRON_EVERY = int(os.environ.get("CRON_EVERY_SEC", "2") or "2")
CATCH_MAX = int(os.environ.get("CATCH_UP_MAX", "3") or "3")
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


def get_schedule(sid: str, tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/schedules/{sid}?tenant_id={tid}")
    if code != 200:
        raise RuntimeError(f"get schedule {sid} {code} {body}")
    return body.get("schedule") or {}


def create_sched(tid: str, *, policy: str, catch_up_max: int = 8, name: str | None = None) -> str:
    code, body = http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": tid,
            "name": name or f"{tid}-{policy}",
            "cron": f"*/{CRON_EVERY} * * * * *",
            "payload": {"kind": "render", "ms": JOB_MS, "policy": policy},
            "misfire_policy": policy,
            "catch_up_max": catch_up_max,
        },
    )
    if code != 201:
        raise RuntimeError(f"create schedule {tid} {code} {body}")
    return str(body["schedule"]["id"])


def create_tenant_win(tid: str, *, seconds: int, reason: str) -> str:
    now = _now()
    code, body = http_json(
        "POST",
        f"/v1/tenants/{tid}/blackouts",
        {
            "starts_at": _iso(now - timedelta(seconds=1)),
            "ends_at": _iso(now + timedelta(seconds=seconds)),
            "reason": reason,
        },
    )
    if code != 201:
        raise RuntimeError(f"create tenant blackout {tid} {code} {body}")
    return str(body["blackout"]["id"])


def create_sched_win(tid: str, sid: str, *, seconds: int, reason: str) -> str:
    now = _now()
    code, body = http_json(
        "POST",
        f"/v1/schedules/{sid}/blackouts",
        {
            "tenant_id": tid,
            "starts_at": _iso(now - timedelta(seconds=1)),
            "ends_at": _iso(now + timedelta(seconds=seconds)),
            "reason": reason,
        },
    )
    if code != 201:
        raise RuntimeError(f"create schedule blackout {tid} {code} {body}")
    return str(body["blackout"]["id"])


def delete_tenant_win(tid: str, bid: str) -> None:
    code, body = http_json(
        "POST", f"/v1/tenants/{tid}/blackouts/{bid}/delete", {}
    )
    if code not in (200, 404):
        raise RuntimeError(f"delete tenant blackout {tid} {code} {body}")


def delete_sched_win(tid: str, sid: str, bid: str) -> None:
    code, body = http_json(
        "POST",
        f"/v1/schedules/{sid}/blackouts/{bid}/delete",
        {"tenant_id": tid},
    )
    if code not in (200, 404):
        raise RuntimeError(f"delete schedule blackout {tid} {code} {body}")


def completed(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def wait_completed(tid: str, minimum: int) -> None:
    deadline = time.time() + MAX_WALL
    last = 0.0
    n = 0
    while time.time() < deadline:
        n = completed(list_jobs(tid))
        now = time.time()
        if now - last >= 2:
            logger.info("drain %s completed=%d need>=%d", tid, n, minimum)
            last = now
        if n >= minimum:
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting {tid} completed={n} need={minimum}")


def main() -> int:
    logger.info(
        "blackout soak window=%ds cron=every %ds catch_max=%d",
        WINDOW_SECONDS,
        CRON_EVERY,
        CATCH_MAX,
    )
    wait_coords()
    skip_t, one_t, catch_t, live_t, pair_t, churn_t = (
        "bl-skip",
        "bl-one",
        "bl-catch",
        "bl-live",
        "bl-pair",
        "bl-churn",
    )
    skip_id = create_sched(skip_t, policy="skip")
    one_id = create_sched(one_t, policy="catch_up_one")
    catch_id = create_sched(catch_t, policy="catch_up", catch_up_max=CATCH_MAX)
    create_sched(live_t, policy="catch_up_one")
    blocked_id = create_sched(pair_t, policy="skip", name="pair-blocked")
    sibling_id = create_sched(pair_t, policy="catch_up_one", name="pair-live")
    churn_id = create_sched(churn_t, policy="catch_up_one")

    skip_win = create_tenant_win(skip_t, seconds=WINDOW_SECONDS, reason="deploy-skip")
    one_win = create_tenant_win(one_t, seconds=WINDOW_SECONDS, reason="deploy-one")
    catch_win = create_tenant_win(catch_t, seconds=WINDOW_SECONDS, reason="deploy-catch")
    create_sched_win(pair_t, blocked_id, seconds=WINDOW_SECONDS, reason="sched-only")
    churn_tenant = create_tenant_win(churn_t, seconds=WINDOW_SECONDS + 4, reason="churn-t")
    churn_sched = create_sched_win(
        churn_t, churn_id, seconds=WINDOW_SECONDS + 6, reason="churn-s"
    )

    stop = threading.Event()
    churn_errors: list[str] = []

    def churn() -> None:
        try:
            while not stop.is_set():
                now = _now()
                http_json(
                    "POST",
                    f"/v1/tenants/{churn_t}/blackouts/{churn_tenant}",
                    {"ends_at": _iso(now + timedelta(seconds=WINDOW_SECONDS))},
                )
                extra_code, extra_body = http_json(
                    "POST",
                    f"/v1/tenants/{churn_t}/blackouts",
                    {
                        "starts_at": _iso(now - timedelta(seconds=1)),
                        "ends_at": _iso(now + timedelta(seconds=4)),
                        "reason": "overlap",
                    },
                )
                if extra_code == 201:
                    bid = extra_body["blackout"]["id"]
                    http_json(
                        "POST",
                        f"/v1/tenants/{churn_t}/blackouts/{bid}/delete",
                        {},
                    )
                http_json(
                    "POST",
                    f"/v1/schedules/{churn_id}/blackouts/{churn_sched}",
                    {
                        "tenant_id": churn_t,
                        "ends_at": _iso(now + timedelta(seconds=WINDOW_SECONDS + 2)),
                    },
                )
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            churn_errors.append(str(exc))

    threads = [threading.Thread(target=churn), threading.Thread(target=churn)]
    for t in threads:
        t.start()

    time.sleep(WINDOW_SECONDS)

    live_mid = completed(list_jobs(live_t))
    if live_mid < 1:
        raise RuntimeError(f"live tenant did not fire during blackout: {live_mid}")
    for tid in (skip_t, one_t, catch_t, churn_t):
        piled = list_jobs(tid)
        if piled:
            raise RuntimeError(f"{tid} piled jobs while blacked out: {len(piled)}")
    if list_jobs(pair_t):
        # sibling on pair_t should have fired
        pair_jobs = list_jobs(pair_t)
        blocked_jobs = [j for j in pair_jobs if j.get("schedule_id") == blocked_id]
        sibling_jobs = [j for j in pair_jobs if j.get("schedule_id") == sibling_id]
        if blocked_jobs:
            raise RuntimeError(f"schedule blackout leaked {len(blocked_jobs)} jobs")
        if not sibling_jobs:
            raise RuntimeError("sibling schedule did not fire during schedule blackout")
    else:
        raise RuntimeError("pair tenant produced no sibling jobs")

    stop.set()
    for t in threads:
        t.join(timeout=5)
    if churn_errors:
        raise RuntimeError(f"churn errors: {churn_errors}")

    # Close remaining windows so misfire policy can run.
    delete_tenant_win(skip_t, skip_win)
    delete_tenant_win(one_t, one_win)
    delete_tenant_win(catch_t, catch_win)
    delete_tenant_win(churn_t, churn_tenant)
    delete_sched_win(churn_t, churn_id, churn_sched)
    # leave pair schedule window; we only care it did not leak during the window

    expected_ticks = max(1, WINDOW_SECONDS // CRON_EVERY)
    deadline = time.time() + 20
    skip_sched: dict[str, Any] = {}
    while time.time() < deadline:
        skip_sched = get_schedule(skip_id, skip_t)
        if int(skip_sched.get("missed_ticks") or 0) >= expected_ticks - 1:
            break
        time.sleep(0.15)
    else:
        raise RuntimeError(f"skip did not record misses: {skip_sched}")

    skip_n = len(list_jobs(skip_t))
    if skip_n > 1:
        raise RuntimeError(f"skip flooded {skip_n} jobs after blackout")
    logger.info("skip jobs=%d missed=%s", skip_n, skip_sched.get("missed_ticks"))

    catch_sched: dict[str, Any] = {}
    while time.time() < deadline:
        catch_sched = get_schedule(catch_id, catch_t)
        rem = catch_sched.get("catch_up_remaining")
        missed = int(catch_sched.get("missed_ticks") or 0)
        if rem is None and missed >= 1:
            break
        time.sleep(0.15)
    else:
        raise RuntimeError(f"catch_up did not finish drain: {catch_sched}")

    for tid in (skip_t, one_t, catch_t, churn_t):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"re-pause {tid} {code} {body}")

    one_jobs = list_jobs(one_t)
    one_sched = get_schedule(one_id, one_t)
    if len(one_jobs) > expected_ticks:
        raise RuntimeError(f"catch_up_one flooded {len(one_jobs)}")
    if len(one_jobs) < 1:
        raise RuntimeError("catch_up_one produced no pickup")
    logger.info(
        "catch_up_one jobs=%d missed=%s",
        len(one_jobs),
        one_sched.get("missed_ticks"),
    )

    catch_jobs = list_jobs(catch_t)
    if len(catch_jobs) > CATCH_MAX + 2:
        raise RuntimeError(f"catch_up flooded {len(catch_jobs)} (max={CATCH_MAX})")
    if len(catch_jobs) < 1:
        raise RuntimeError("catch_up produced no jobs")
    logger.info(
        "catch_up jobs=%d missed=%s max=%d remaining=%s",
        len(catch_jobs),
        catch_sched.get("missed_ticks"),
        CATCH_MAX,
        catch_sched.get("catch_up_remaining"),
    )

    churn_jobs = list_jobs(churn_t)
    if len(churn_jobs) > 2:
        raise RuntimeError(f"churn double-fired {len(churn_jobs)} jobs")
    if len(churn_jobs) < 1:
        raise RuntimeError("churn produced no pickup after windows ended")
    logger.info("churn jobs=%d", len(churn_jobs))

    live_after = completed(list_jobs(live_t))
    if live_after < live_mid:
        raise RuntimeError("live tenant went backwards")

    wait_completed(live_t, live_after)
    if one_jobs:
        wait_completed(one_t, 1)
    if catch_jobs:
        wait_completed(catch_t, 1)
    logger.info(
        "blackout soak OK live_completed=%d sibling_id=%s",
        live_after,
        sibling_id,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("blackout soak failed")
        sys.exit(1)
