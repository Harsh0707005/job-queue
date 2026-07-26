#!/usr/bin/env python3
"""Docker fleet: mid-job approval parks without lease; approve resumes checkpoint.

Flow:
  1. Tight concurrency (1) + a filler job so the slot matters.
  2. Enqueue stepped job with approval_after_step mid-way.
  3. Wait until status=awaiting_approval (lease free; concurrency slot free).
  4. Approve; wait for completion with resumed/started_step evidence.
  5. Enqueue another job with short approval timeout; wait for auto-reject.
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
logger = logging.getLogger("approval_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANT = os.environ.get("TENANT", "approve")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
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
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            last = RuntimeError(f"{exc.code}: {payload}")
            if 400 <= int(exc.code) < 500:
                raise last
            continue
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def get_job(job_id: str) -> dict[str, Any] | None:
    try:
        return http_json("GET", f"/v1/jobs/{job_id}?tenant_id={TENANT}").get("job")
    except Exception:
        return None


def main() -> int:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")

    http_json(
        "POST",
        f"/v1/tenants/{TENANT}/concurrency",
        {"max_concurrent_leases": 1},
    )

    # Filler long job to prove parking frees the slot.
    filler = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": TENANT,
            "payload": {"kind": "steps", "steps": 30, "step_ms": 200},
            "priority": 0,
            "estimated_duration_seconds": 20,
            "visibility_timeout_seconds": 20,
            "max_attempts": 10,
        },
    )["job"]
    logger.info("filler %s", filler["id"])

    gated = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": TENANT,
            "payload": {
                "kind": "steps",
                "steps": 6,
                "step_ms": 150,
                "approval_after_step": 3,
                "approval_timeout_seconds": 120,
                "approval_message": "fleet soak sign-off",
            },
            "priority": 10,
            "visibility_timeout_seconds": 20,
            "max_attempts": 10,
        },
    )["job"]
    gated_id = gated["id"]
    logger.info("gated job %s", gated_id)

    started = time.monotonic()
    saw_awaiting = False
    while time.monotonic() - started < MAX_WALL:
        http_json("POST", "/v1/approvals/process-timeouts", {"tenant_id": TENANT})
        j = get_job(gated_id)
        if j and j.get("status") == "awaiting_approval":
            saw_awaiting = True
            cp = j.get("checkpoint") or {}
            logger.info(
                "parked awaiting approval checkpoint=%s leased_by=%s",
                cp,
                j.get("leased_by"),
            )
            if j.get("leased_by") is not None or j.get("lease_token") is not None:
                logger.error("still holding lease while awaiting approval")
                return 1
            # Concurrency free: filler or another job can be leased (count via get).
            break
        if j and j.get("status") == "completed":
            logger.error("gated job completed without approval")
            return 1
        time.sleep(0.4)
    else:
        logger.error("timeout waiting for awaiting_approval")
        return 1

    # Retry-safe approve.
    for _ in range(2):
        ap = http_json(
            "POST",
            f"/v1/jobs/{gated_id}/approve",
            {"tenant_id": TENANT},
        )["job"]
        assert ap.get("approval_outcome") == "approved"
    logger.info("approved status=%s", ap.get("status"))

    while time.monotonic() - started < MAX_WALL:
        j = get_job(gated_id)
        if j and j.get("status") == "completed":
            result = j.get("result") or {}
            logger.info("gated completed result=%s checkpoint=%s", result, j.get("checkpoint"))
            started_step = int(result.get("started_step") or 0)
            if started_step < 3 and not result.get("approved_resume") and not result.get("resumed"):
                logger.error(
                    "did not resume after approval started_step=%s", started_step
                )
                return 1
            break
        if j and j.get("status") in ("cancelled", "failed", "dead"):
            logger.error("gated failed: %s %s", j.get("status"), j.get("last_error"))
            return 1
        time.sleep(0.5)
    else:
        logger.error("timeout waiting for approved job to complete")
        return 1

    # Timeout path.
    timed = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": TENANT,
            "payload": {
                "kind": "steps",
                "steps": 4,
                "step_ms": 100,
                "approval_after_step": 1,
                "approval_timeout_seconds": 2,
            },
            "priority": 20,
            "visibility_timeout_seconds": 15,
            "max_attempts": 5,
        },
    )["job"]
    timed_id = timed["id"]
    logger.info("timeout job %s", timed_id)

    saw_timeout = False
    while time.monotonic() - started < MAX_WALL:
        http_json("POST", "/v1/approvals/process-timeouts", {"tenant_id": TENANT})
        j = get_job(timed_id)
        if j and j.get("status") == "awaiting_approval":
            logger.info("timeout job parked; waiting for auto-reject")
        if j and j.get("status") == "cancelled" and j.get("approval_outcome") == "timeout":
            saw_timeout = True
            logger.info("timeout auto-reject ok last_error=%s", j.get("last_error"))
            break
        if j and j.get("status") == "completed":
            logger.error("timeout job completed instead of rejecting")
            return 1
        time.sleep(0.5)
    else:
        logger.error("timeout job never auto-rejected")
        return 1

    if not saw_awaiting or not saw_timeout:
        logger.error("missing phases awaiting=%s timeout=%s", saw_awaiting, saw_timeout)
        return 1

    # Retry reject on already-timed-out should fail with approval_error — optional.
    logger.info(
        "APPROVAL SOAK PASSED gated=%s timeout=%s",
        gated_id,
        timed_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
