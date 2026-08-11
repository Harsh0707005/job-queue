#!/usr/bin/env python3
"""Fleet soak: poison jobs that hop across workers get quarantined.

  * Dual coordinators, four workers, several tenants.
  * Poison jobs (kind=fail) with high max_attempts would otherwise bounce
    forever. Hop caps of 2 stop them: dead-letter or park (release).
  * Healthy jobs on the same tenants still complete.
  * Distinct workers are recorded; jobs do not stay pending.
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
logger = logging.getLogger("quarantine_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DEAD_TENANTS = int(os.environ.get("DEAD_TENANTS", "3") or "3")
REL_TENANTS = int(os.environ.get("REL_TENANTS", "3") or "3")
POISON_EACH = int(os.environ.get("POISON_EACH", "4") or "4")
HEALTHY_EACH = int(os.environ.get("HEALTHY_EACH", "6") or "6")
HOPS = int(os.environ.get("MAX_WORKER_HOPS", "2") or "2")
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


def main() -> int:
    logger.info(
        "quarantine soak dead_tenants=%d rel_tenants=%d hops=%d",
        DEAD_TENANTS,
        REL_TENANTS,
        HOPS,
    )
    wait_coords()

    dead_ids = [f"qdead-{i:02d}" for i in range(DEAD_TENANTS)]
    rel_ids = [f"qrel-{i:02d}" for i in range(REL_TENANTS)]
    for tid in dead_ids:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/quarantine",
            {"max_worker_hops": HOPS, "action": "dead"},
        )
        if code != 200:
            raise RuntimeError(f"set quarantine {tid} {code} {body}")
    for tid in rel_ids:
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/quarantine",
            {"max_worker_hops": HOPS, "action": "release"},
        )
        if code != 200:
            raise RuntimeError(f"set quarantine {tid} {code} {body}")

    expect_poison = (DEAD_TENANTS + REL_TENANTS) * POISON_EACH
    expect_healthy = (DEAD_TENANTS + REL_TENANTS) * HEALTHY_EACH
    for tid in dead_ids + rel_ids:
        for i in range(POISON_EACH):
            code, body = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "fail", "ms": 15, "error": "poison"},
                    "max_attempts": 20,
                    "retry_backoff_base_seconds": 0,
                },
            )
            if code != 201:
                raise RuntimeError(f"poison enqueue {tid} {code} {body}")
        for i in range(HEALTHY_EACH):
            code, body = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 10, "i": i},
                },
            )
            if code != 201:
                raise RuntimeError(f"healthy enqueue {tid} {code} {body}")
    logger.info("enqueued poison=%d healthy=%d", expect_poison, expect_healthy)

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        healthy_done = 0
        poison_dead = 0
        poison_parked = 0
        hopping = 0
        for tid in dead_ids + rel_ids:
            for j in list_jobs(tid):
                st = j.get("status")
                hops = j.get("failed_workers") or []
                payload = j.get("payload") or {}
                # list redacts payload; use last_error / status.
                if st == "completed":
                    healthy_done += 1
                elif st == "dead":
                    poison_dead += 1
                    if len(hops) > HOPS:
                        raise RuntimeError(f"{tid} job hopped past cap: {hops}")
                elif st == "quarantined":
                    poison_parked += 1
                    if len(hops) > HOPS:
                        raise RuntimeError(f"{tid} job hopped past cap: {hops}")
                elif st in ("pending", "leased"):
                    hopping += 1
        now = time.time()
        if now - last >= 2:
            logger.info(
                "healthy=%d/%d dead=%d parked=%d hopping=%d",
                healthy_done,
                expect_healthy,
                poison_dead,
                poison_parked,
                hopping,
            )
            last = now
        if (
            healthy_done == expect_healthy
            and hopping == 0
            and poison_dead + poison_parked == expect_poison
        ):
            want_dead = DEAD_TENANTS * POISON_EACH
            want_park = REL_TENANTS * POISON_EACH
            if poison_dead != want_dead or poison_parked != want_park:
                raise RuntimeError(
                    f"actions mixed dead={poison_dead} want {want_dead} "
                    f"parked={poison_parked} want {want_park}"
                )
            logger.info(
                "quarantine soak OK healthy=%d dead=%d parked=%d",
                healthy_done,
                poison_dead,
                poison_parked,
            )
            return 0
        time.sleep(0.4)
    raise RuntimeError("quarantine soak timed out")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("quarantine soak failed")
        sys.exit(1)
