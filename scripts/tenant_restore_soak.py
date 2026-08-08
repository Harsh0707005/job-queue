#!/usr/bin/env python3
"""Fleet soak: export/restore a tenant under load; survive coordinator kill.

  * Several load tenants keep producing and completing work.
  * One source tenant is snapshotted (pending + schedule + webhook).
  * Restore into an empty dest tenant while workers keep running.
  * Coordinators are killed mid-restore; advance resumes on survivors.
  * Dest jobs become claimable after restore completes and finish.
  * Source tenant is never paused by export.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tenant_restore_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "4") or "4")
JOBS_PER_LOAD = int(os.environ.get("JOBS_PER_LOAD", "25") or "25")
SRC_JOBS = int(os.environ.get("SRC_JOBS", "18") or "18")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
SRC = os.environ.get("SOURCE_TENANT", "src-export")
DST = os.environ.get("DEST_TENANT", "dst-restore")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        start = _rr % len(COORDS)
        _rr += 1
        bases = COORDS[start:] + COORDS[:start]
        targets = [b + path for b in bases]  # type: ignore[operator]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")


def tenant_count(tid: str, status: str | None = None) -> int:
    try:
        q = f"/v1/admin/stats?tenant_id={tid}"
        st = http_json("GET", q)
    except Exception:
        return -1
    # fallback: list jobs
    try:
        body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
        jobs = body.get("jobs") or []
        if status:
            jobs = [j for j in jobs if j.get("status") == status]
        return len(jobs)
    except Exception:
        return -1


def main() -> int:
    logger.info("tenant restore soak load=%d src_jobs=%d", LOAD_TENANTS, SRC_JOBS)
    wait_coords()

    for i in range(LOAD_TENANTS):
        tid = f"load-{i:02d}"
        for n in range(JOBS_PER_LOAD):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 40, "i": n, "t": tid},
                    "queue_name": "default",
                },
            )

    http_json(
        "POST",
        "/v1/webhooks",
        {
            "tenant_id": SRC,
            "url": WEBHOOK_URL,
            "events": ["job.completed"],
            "secret": "restore-soak",
        },
    )
    http_json(
        "POST",
        "/v1/schedules",
        {
            "tenant_id": SRC,
            "name": "src-tick",
            "payload": {"from": "schedule", "ms": 20},
            "interval_seconds": 30,
        },
    )
    for n in range(SRC_JOBS):
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": SRC,
                "payload": {"secret": f"src-secret-{n}", "n": n, "ms": 30},
                "delay_seconds": 2 if n % 3 == 0 else 0,
            },
        )

    exp = http_json("POST", f"/v1/tenants/{SRC}/export", {})
    snap = exp["snapshot"]
    logger.info("exported snapshot=%s counts=%s", snap["id"], snap.get("counts"))
    if snap.get("status") != "ready":
        raise RuntimeError(f"export not ready: {snap}")

    rst = http_json(
        "POST",
        f"/v1/tenants/{DST}/restore",
        {"snapshot_id": snap["id"], "batch_size": 2},
    )["restore"]
    logger.info("restore started id=%s status=%s", rst["id"], rst["status"])
    # Leave the restore mid-flight so chaos can kill a coordinator.
    time.sleep(10)

    deadline = time.time() + MAX_WALL
    while time.time() < deadline:
        try:
            adv = http_json(
                "POST",
                f"/v1/tenants/{DST}/restore/advance",
                {"batch_size": 2},
            )["restore"]
        except Exception as exc:  # noqa: BLE001
            logger.warning("advance via tenant path: %s — trying admin", exc)
            try:
                http_json(
                    "POST",
                    "/v1/admin/restores/advance",
                    {"limit": 5, "batch_size": 5},
                )
                got = http_json("GET", f"/v1/tenants/{DST}/restore")
                adv = got["restore"]
            except Exception as exc2:  # noqa: BLE001
                logger.warning("advance failed (coord kill?): %s", exc2)
                time.sleep(1.5)
                continue
        logger.info(
            "restore status=%s applied=%s kind_index=%s",
            adv.get("status"),
            adv.get("applied"),
            adv.get("kind_index"),
        )
        if adv.get("status") == "complete":
            rst = adv
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"restore did not complete: {rst}")

    # Dest work should drain via the live worker fleet.
    while time.time() < deadline:
        listed = http_json("GET", f"/v1/jobs?tenant_id={DST}&limit=500")
        jobs = listed.get("jobs") or []
        pending = sum(1 for j in jobs if j.get("status") in ("pending", "leased"))
        completed = sum(1 for j in jobs if j.get("status") == "completed")
        logger.info("dst jobs total=%d pending/leased=%d completed=%d", len(jobs), pending, completed)
        if jobs and pending == 0 and completed >= min(SRC_JOBS, len(jobs)) * 0.8:
            break
        # keep load tenants moving
        http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": "load-00",
                "payload": {"kind": "work", "ms": 20, "i": int(time.time())},
            },
        )
        time.sleep(1.5)
    else:
        raise RuntimeError("dest tenant did not drain after restore")

    src_listed = http_json("GET", f"/v1/jobs?tenant_id={SRC}&limit=500")
    if not (src_listed.get("jobs") or []):
        raise RuntimeError("source tenant lost all jobs after export")

    load_ok = False
    for i in range(LOAD_TENANTS):
        tid = f"load-{i:02d}"
        lj = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500").get("jobs") or []
        done = sum(1 for j in lj if j.get("status") == "completed")
        logger.info("load tenant %s completed=%d total=%d", tid, done, len(lj))
        if done > 0:
            load_ok = True
    if not load_ok:
        raise RuntimeError("load tenants made no progress during restore")

    logger.info("tenant restore soak OK dest_completed and load still running")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("tenant restore soak failed")
        sys.exit(1)
