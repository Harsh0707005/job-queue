#!/usr/bin/env python3
"""Fleet soak: one tenant trips a breaker; the rest of the fleet keeps draining.

  * Dual coordinators, four workers, several tenants.
  * Poison tenant nacks until the breaker opens; leftover jobs stay pending.
  * Healthy tenants keep completing before and after the trip.
  * Extra poison work after the trip is not leased. Reset drains leftovers.
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
logger = logging.getLogger("breaker_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HEALTHY_TENANTS = int(os.environ.get("HEALTHY_TENANTS", "4") or "4")
POISON_EACH = int(os.environ.get("POISON_EACH", "30") or "30")
HEALTHY_EACH = int(os.environ.get("HEALTHY_EACH", "16") or "16")
EXTRA_HEALTHY = int(os.environ.get("EXTRA_HEALTHY", "8") or "8")
EXTRA_POISON = int(os.environ.get("EXTRA_POISON", "10") or "10")
THRESHOLD = int(os.environ.get("BREAKER_THRESHOLD", "8") or "8")
WINDOW = int(os.environ.get("BREAKER_WINDOW_SECONDS", "120") or "120")
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


def get_breaker(tid: str) -> dict[str, Any]:
    code, body = http_json("GET", f"/v1/tenants/{tid}/breaker")
    if code != 200:
        raise RuntimeError(f"breaker {tid} {code}: {body}")
    return body


def enqueue_jobs(tid: str, n: int, *, kind: str, prefix: str) -> None:
    for i in range(n):
        payload: dict[str, Any] = {"kind": kind, "ms": 10, "i": i, "p": prefix}
        body: dict[str, Any] = {"tenant_id": tid, "payload": payload}
        if kind == "fail":
            body["max_attempts"] = 1
            body["retry_backoff_base_seconds"] = 0
        code, resp = http_json("POST", "/v1/jobs", body)
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")


def counts(tids: list[str]) -> dict[str, int]:
    out = {
        "pending": 0,
        "leased": 0,
        "completed": 0,
        "dead": 0,
        "failed": 0,
        "other": 0,
    }
    for tid in tids:
        for j in list_jobs(tid):
            st = j.get("status")
            if st in out:
                out[st] += 1
            else:
                out["other"] += 1
    return out


def main() -> int:
    poison = "brk-poison"
    healthy = [f"brk-ok-{i:02d}" for i in range(HEALTHY_TENANTS)]
    logger.info(
        "breaker soak poison=%s healthy=%s threshold=%d",
        poison,
        ",".join(healthy),
        THRESHOLD,
    )
    wait_coords()

    code, body = http_json(
        "POST",
        f"/v1/tenants/{poison}/breaker",
        {
            "failure_threshold": THRESHOLD,
            "window_seconds": WINDOW,
        },
    )
    if code != 200:
        raise RuntimeError(f"set breaker {code} {body}")

    enqueue_jobs(poison, POISON_EACH, kind="fail", prefix="p0")
    for tid in healthy:
        enqueue_jobs(tid, HEALTHY_EACH, kind="work", prefix="h0")
    expect_healthy = HEALTHY_TENANTS * HEALTHY_EACH
    logger.info(
        "enqueued poison=%d healthy=%d",
        POISON_EACH,
        expect_healthy,
    )

    deadline = time.time() + MAX_WALL
    last = 0.0
    tripped_at: float | None = None
    while time.time() < deadline:
        br = get_breaker(poison)
        h = counts(healthy)
        p = counts([poison])
        now = time.time()
        if now - last >= 2:
            logger.info(
                "healthy_done=%d/%d poison_dead=%d pending=%d leased=%d open=%s fails=%s",
                h["completed"],
                expect_healthy,
                p["dead"],
                p["pending"],
                p["leased"],
                br.get("open"),
                br.get("failures"),
            )
            last = now
        if br.get("open") and tripped_at is None:
            tripped_at = now
            logger.info("breaker opened dead=%d pending=%d", p["dead"], p["pending"])
        if br.get("open") and h["completed"] == expect_healthy and h["leased"] == 0:
            break
        time.sleep(0.4)
    else:
        raise RuntimeError("timed out waiting for trip + healthy drain")

    br = get_breaker(poison)
    if not br.get("open"):
        raise RuntimeError(f"breaker did not stay open: {br}")
    p = counts([poison])
    if p["pending"] < 1:
        raise RuntimeError(f"poison leftover should stay pending: {p}")
    if p["dead"] < THRESHOLD:
        raise RuntimeError(f"expected at least {THRESHOLD} poison dead, got {p}")
    leftover_before_extra = p["pending"]

    for tid in healthy:
        enqueue_jobs(tid, EXTRA_HEALTHY, kind="work", prefix="h1")
    enqueue_jobs(poison, EXTRA_POISON, kind="fail", prefix="p1")
    expect_healthy += HEALTHY_TENANTS * EXTRA_HEALTHY
    logger.info(
        "post-trip extra healthy=%d poison=%d leftover=%d",
        HEALTHY_TENANTS * EXTRA_HEALTHY,
        EXTRA_POISON,
        leftover_before_extra,
    )

    while time.time() < deadline:
        h = counts(healthy)
        p = counts([poison])
        br = get_breaker(poison)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "post-trip healthy=%d/%d poison_pending=%d open=%s",
                h["completed"],
                expect_healthy,
                p["pending"],
                br.get("open"),
            )
            last = now
        if h["completed"] == expect_healthy and h["leased"] == 0:
            break
        time.sleep(0.4)
    else:
        raise RuntimeError("healthy extra did not drain after trip")

    p = counts([poison])
    want_pending = leftover_before_extra + EXTRA_POISON
    if p["pending"] != want_pending:
        raise RuntimeError(
            f"poison leased after trip: pending={p['pending']} want {want_pending} {p}"
        )
    if not get_breaker(poison).get("open"):
        raise RuntimeError("breaker closed before reset")

    dead_before_reset = p["dead"]
    code, reset = http_json("POST", f"/v1/tenants/{poison}/breaker/reset", {})
    if code != 200 or reset.get("open"):
        raise RuntimeError(f"reset failed {code} {reset}")
    logger.info(
        "breaker reset; waiting for leftovers to be leased again (dead was %d)",
        dead_before_reset,
    )

    progressed = False
    while time.time() < deadline:
        p = counts([poison])
        br = get_breaker(poison)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "reset drain pending=%d dead=%d leased=%d open=%s",
                p["pending"],
                p["dead"],
                p["leased"],
                br.get("open"),
            )
            last = now
        if p["dead"] > dead_before_reset or p["leased"] > 0:
            progressed = True
            break
        time.sleep(0.4)
    if not progressed:
        raise RuntimeError("reset did not resume poison work")

    # Leftover fail jobs may trip the breaker again; that is expected.
    # Raise the cap so the remainder can finish without stalling the check.
    code, body = http_json(
        "POST",
        f"/v1/tenants/{poison}/breaker",
        {"failure_threshold": 10_000, "window_seconds": WINDOW},
    )
    if code != 200:
        raise RuntimeError(f"raise breaker {code} {body}")
    if get_breaker(poison).get("open"):
        code, reset = http_json("POST", f"/v1/tenants/{poison}/breaker/reset", {})
        if code != 200 or reset.get("open"):
            raise RuntimeError(f"second reset failed {code} {reset}")

    while time.time() < deadline:
        p = counts([poison])
        br = get_breaker(poison)
        now = time.time()
        if now - last >= 2:
            logger.info(
                "final drain pending=%d dead=%d leased=%d open=%s",
                p["pending"],
                p["dead"],
                p["leased"],
                br.get("open"),
            )
            last = now
        if p["pending"] == 0 and p["leased"] == 0:
            break
        time.sleep(0.4)
    else:
        raise RuntimeError("poison leftovers did not drain after reset")

    h = counts(healthy)
    p = counts([poison])
    if h["completed"] != expect_healthy:
        raise RuntimeError(f"healthy lost work: {h} want {expect_healthy}")
    if p["dead"] != POISON_EACH + EXTRA_POISON:
        raise RuntimeError(
            f"poison not all dead after reset: {p} want {POISON_EACH + EXTRA_POISON}"
        )
    logger.info(
        "breaker soak OK healthy=%d poison_dead=%d leftover_held=%d",
        h["completed"],
        p["dead"],
        leftover_before_extra,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("breaker soak failed")
        sys.exit(1)
