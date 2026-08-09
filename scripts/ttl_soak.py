#!/usr/bin/env python3
"""Fleet soak: job TTL expires unmatched work as cancel or dead-letter.

  * Tenants enqueue skilled jobs nobody can run, with short TTLs.
  * Mix of per-job dead / cancel and a tenant-default TTL.
  * Unskilled load jobs still complete on the live worker fleet.
  * After TTL, parked jobs must terminate with the configured action.
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
logger = logging.getLogger("ttl_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
DEAD_N = int(os.environ.get("DEAD_JOBS", "16") or "16")
CANCEL_N = int(os.environ.get("CANCEL_JOBS", "16") or "16")
DEFAULT_N = int(os.environ.get("DEFAULT_JOBS", "12") or "12")
LOAD_N = int(os.environ.get("LOAD_JOBS", "20") or "20")
TTL_SECONDS = int(os.environ.get("TTL_SECONDS", "4") or "4")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120") or "120")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDS)
    _rr += 1
    bases = COORDS[start:] + COORDS[:start]
    last: Exception | None = None
    for base in bases:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            raise RuntimeError(f"{base}{path} -> {exc.code}: {payload}") from exc
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    return list(
        http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500").get("jobs") or []
    )


def main() -> int:
    logger.info(
        "ttl soak dead=%d cancel=%d default=%d load=%d ttl=%ds",
        DEAD_N,
        CANCEL_N,
        DEFAULT_N,
        LOAD_N,
        TTL_SECONDS,
    )
    wait_coords()

    http_json(
        "POST",
        "/v1/tenants/ttl-default/job-ttl",
        {"ttl_seconds": TTL_SECONDS, "expiry_action": "dead"},
    )

    def enq(item: tuple[str, dict[str, Any]]) -> None:
        tid, body = item
        payload = {"tenant_id": tid, **body}
        http_json("POST", "/v1/jobs", payload)

    jobs: list[tuple[str, dict[str, Any]]] = []
    for i in range(DEAD_N):
        jobs.append(
            (
                "ttl-dead",
                {
                    "payload": {"kind": "orphan", "i": i},
                    "required_skills": ["no-such-skill"],
                    "ttl_seconds": TTL_SECONDS,
                    "expiry_action": "dead",
                },
            )
        )
    for i in range(CANCEL_N):
        jobs.append(
            (
                "ttl-cancel",
                {
                    "payload": {"kind": "orphan", "i": i},
                    "required_skills": ["no-such-skill"],
                    "ttl_seconds": TTL_SECONDS,
                    "expiry_action": "cancel",
                },
            )
        )
    for i in range(DEFAULT_N):
        jobs.append(
            (
                "ttl-default",
                {
                    "payload": {"kind": "orphan", "i": i},
                    "required_skills": ["no-such-skill"],
                },
            )
        )
    for i in range(LOAD_N):
        jobs.append(
            (
                "ttl-load",
                {"payload": {"kind": "work", "i": i, "ms": 20}},
            )
        )

    with ThreadPoolExecutor(max_workers=16) as pool:
        futs = [pool.submit(enq, j) for j in jobs]
        for f in as_completed(futs):
            f.result()
    logger.info("enqueued %d jobs", len(jobs))

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        http_json("POST", "/v1/jobs/process-expired", {"limit": 200})
        dead = list_jobs("ttl-dead")
        cancel = list_jobs("ttl-cancel")
        default = list_jobs("ttl-default")
        load = list_jobs("ttl-load")
        d_dead = sum(1 for j in dead if j.get("status") == "dead")
        c_can = sum(1 for j in cancel if j.get("status") == "cancelled")
        def_dead = sum(1 for j in default if j.get("status") == "dead")
        load_done = sum(1 for j in load if j.get("status") == "completed")
        leftover = [
            (tid, j.get("id"), j.get("status"))
            for tid, rows, bad in (
                ("ttl-dead", dead, {"pending", "awaiting_skills", "leased"}),
                ("ttl-cancel", cancel, {"pending", "awaiting_skills", "leased"}),
                ("ttl-default", default, {"pending", "awaiting_skills", "leased"}),
            )
            for j in rows
            if j.get("status") in bad
        ]
        now = time.time()
        if now - last >= 2:
            logger.info(
                "dead=%d/%d cancel=%d/%d default=%d/%d load=%d/%d leftover=%d",
                d_dead,
                DEAD_N,
                c_can,
                CANCEL_N,
                def_dead,
                DEFAULT_N,
                load_done,
                LOAD_N,
                len(leftover),
            )
            last = now
        if (
            d_dead == DEAD_N
            and c_can == CANCEL_N
            and def_dead == DEFAULT_N
            and load_done >= LOAD_N
            and not leftover
        ):
            logger.info("ttl soak OK")
            return 0
        time.sleep(0.4)
    raise RuntimeError("ttl soak timed out waiting for expiry + load drain")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("ttl soak failed")
        sys.exit(1)
