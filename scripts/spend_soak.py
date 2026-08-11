#!/usr/bin/env python3
"""Fleet soak: per-tenant spend caps under concurrent multi-tenant load.

  * Capped tenants are flooded through dual coordinators (many extra submits).
  * Exactly *cap* jobs may land per capped tenant (no double-fire overshoot).
  * Cancel does not free quota (tenants cannot manipulate the meter).
  * Uncapped tenants still accept all of their work.
  * Spend cursor matches usage enqueues and job counts.
  * Workers drain accepted work.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("spend_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
CAPPED_TENANTS = int(os.environ.get("CAPPED_TENANTS", "6") or "6")
UNCAPPED_TENANTS = int(os.environ.get("UNCAPPED_TENANTS", "2") or "2")
ENQUEUE_CAP = int(os.environ.get("ENQUEUE_CAP", "20") or "20")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "120") or "120")
FLOOD_PER_TENANT = int(os.environ.get("FLOOD_PER_TENANT", "80") or "80")
UNCAPPED_JOBS = int(os.environ.get("UNCAPPED_JOBS", "24") or "24")
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


def cap_tid(i: int) -> str:
    return f"spend-c{i:02d}"


def free_tid(i: int) -> str:
    return f"spend-u{i:02d}"


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def main() -> int:
    logger.info(
        "spend soak capped=%d uncapped=%d cap=%d flood=%d",
        CAPPED_TENANTS,
        UNCAPPED_TENANTS,
        ENQUEUE_CAP,
        FLOOD_PER_TENANT,
    )
    wait_coords()

    for i in range(CAPPED_TENANTS):
        code, body = http_json(
            "POST",
            f"/v1/tenants/{cap_tid(i)}/spend-cap",
            {
                "window_seconds": WINDOW_SECONDS,
                "max_enqueues": ENQUEUE_CAP,
            },
        )
        if code != 200:
            raise RuntimeError(f"set cap {cap_tid(i)} {code} {body}")

    created = 0
    refused = 0
    errors: list[str] = []

    work: list[tuple[str, int]] = []
    for i in range(CAPPED_TENANTS):
        for n in range(FLOOD_PER_TENANT):
            work.append((cap_tid(i), n))
    for i in range(UNCAPPED_TENANTS):
        for n in range(UNCAPPED_JOBS):
            work.append((free_tid(i), n))

    def submit(item: tuple[str, int]) -> str:
        tid, n = item
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {"tenant_id": tid, "payload": {"kind": "work", "ms": 10, "n": n}},
        )
        if code == 201:
            return "ok"
        if code == 429 and body.get("code") == "spend_capped":
            return "cap"
        return f"{tid}/{n} -> {code} {body}"

    with ThreadPoolExecutor(max_workers=24) as pool:
        futs = [pool.submit(submit, w) for w in work]
        for f in as_completed(futs):
            res = f.result()
            if res == "ok":
                created += 1
            elif res == "cap":
                refused += 1
            else:
                errors.append(res)
    if errors:
        raise RuntimeError("enqueue errors: " + "; ".join(errors[:6]))

    expect_capped = CAPPED_TENANTS * ENQUEUE_CAP
    expect_free = UNCAPPED_TENANTS * UNCAPPED_JOBS
    expect_refused = CAPPED_TENANTS * (FLOOD_PER_TENANT - ENQUEUE_CAP)
    if created != expect_capped + expect_free:
        raise RuntimeError(
            f"created {created} want {expect_capped + expect_free}"
        )
    if refused != expect_refused:
        raise RuntimeError(f"refused {refused} want {expect_refused}")
    logger.info("flood created=%d refused=%d", created, refused)

    # Cancel half of each capped tenant; quota must not come back.
    for i in range(CAPPED_TENANTS):
        jobs = list_jobs(cap_tid(i))
        if len(jobs) != ENQUEUE_CAP:
            raise RuntimeError(
                f"{cap_tid(i)} jobs={len(jobs)} want {ENQUEUE_CAP}"
            )
        cancelable = [
            j
            for j in jobs
            if j.get("status")
            in (
                "pending",
                "leased",
                "awaiting_approval",
                "awaiting_children",
                "awaiting_skills",
            )
        ]
        for j in cancelable[: ENQUEUE_CAP // 2]:
            ccode, cbody = http_json(
                "POST",
                f"/v1/jobs/{j['id']}/cancel",
                {"tenant_id": cap_tid(i)},
            )
            if ccode == 409:
                continue
            if ccode != 200:
                raise RuntimeError(f"cancel {j['id']} {ccode} {cbody}")
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {"tenant_id": cap_tid(i), "payload": {"kind": "work", "ms": 10}},
        )
        if code != 429 or body.get("code") != "spend_capped":
            raise RuntimeError(
                f"{cap_tid(i)} cancel-then-enqueue {code} {body}"
            )
        cap = http_json("GET", f"/v1/tenants/{cap_tid(i)}/spend-cap")[1]
        usage = http_json("GET", f"/v1/usage?tenant_id={cap_tid(i)}")[1]
        billed = int((usage.get("usage") or {}).get("enqueues") or 0)
        if int(cap.get("enqueues") or 0) != ENQUEUE_CAP:
            raise RuntimeError(f"{cap_tid(i)} spend enqueues {cap}")
        if billed != ENQUEUE_CAP:
            raise RuntimeError(f"{cap_tid(i)} usage enqueues={billed}")
        if not cap.get("exhausted"):
            raise RuntimeError(f"{cap_tid(i)} should be exhausted")

    for i in range(UNCAPPED_TENANTS):
        jobs = list_jobs(free_tid(i))
        if len(jobs) != UNCAPPED_JOBS:
            raise RuntimeError(
                f"{free_tid(i)} jobs={len(jobs)} want {UNCAPPED_JOBS}"
            )

    expect_jobs = expect_capped + expect_free
    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        done = 0
        cancelled = 0
        pending = 0
        tenants = [cap_tid(i) for i in range(CAPPED_TENANTS)] + [
            free_tid(i) for i in range(UNCAPPED_TENANTS)
        ]
        for tid in tenants:
            for j in list_jobs(tid):
                st = j.get("status")
                if st == "completed":
                    done += 1
                elif st == "cancelled":
                    cancelled += 1
                elif st in ("pending", "leased"):
                    pending += 1
        now = time.time()
        if now - last >= 2:
            logger.info(
                "completed=%d cancelled=%d pending=%d total=%d",
                done,
                cancelled,
                pending,
                expect_jobs,
            )
            last = now
        if pending == 0 and done + cancelled == expect_jobs:
            logger.info("spend soak OK created=%d refused=%d", created, refused)
            return 0
        time.sleep(0.4)
    raise RuntimeError("spend soak timed out waiting for drain")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("spend soak failed")
        sys.exit(1)
