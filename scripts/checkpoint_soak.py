#!/usr/bin/env python3
"""Docker fleet proof: long job checkpoints, worker killed, resume on reclaim.

Flow:
  1. Enqueue one multi-step job (short visibility timeout).
  2. Wait until a worker leases it and has checkpointed step >= MIN_STEP.
  3. Kill the worker that holds the lease (docker.sock).
  4. Wait for another worker to reclaim, finish, and ack.
  5. Assert result.resumed and started_step >= MIN_STEP, final checkpoint == steps.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("checkpoint_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANT = os.environ.get("TENANT", "ckpt")
STEPS = int(os.environ.get("STEPS", "10"))
STEP_MS = int(os.environ.get("STEP_MS", "400"))
MIN_STEP = int(os.environ.get("MIN_CHECKPOINT_STEP", "3"))
VT = int(os.environ.get("VISIBILITY_TIMEOUT", "3"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "job-queue")

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
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def get_job(job_id: str) -> dict[str, Any] | None:
    try:
        payload = http_json("GET", f"/v1/jobs/{job_id}?tenant_id={TENANT}")
    except Exception as exc:  # noqa: BLE001
        logger.debug("get_job error: %s", exc)
        return None
    return payload.get("job")


def docker_ids(name_substr: str) -> list[str]:
    try:
        r = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={name_substr}"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("docker ps failed: %s", exc)
        return []
    return [x for x in r.stdout.split() if x]


def docker_kill(name_substr: str) -> bool:
    ids = docker_ids(name_substr)
    if not ids:
        # try with project prefix
        ids = docker_ids(f"{PROJECT}")
        ids = [
            i
            for i in ids
            if name_substr in (
                subprocess.run(
                    ["docker", "inspect", "-f", "{{.Name}}", i],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                ).stdout
                or ""
            )
        ]
    ok = False
    for cid in ids:
        r = subprocess.run(
            ["docker", "kill", cid],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        logger.info("docker kill %s -> rc=%s %s", cid, r.returncode, r.stderr.strip()[:120])
        if r.returncode == 0:
            ok = True
    return ok


def main() -> int:
    logger.info(
        "checkpoint soak steps=%d step_ms=%d min_step=%d vt=%ds",
        STEPS,
        STEP_MS,
        MIN_STEP,
        VT,
    )
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready: {b}")

    # Single job; short VT so kill → reclaim is quick.
    enq = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": TENANT,
            "payload": {
                "kind": "steps",
                "steps": STEPS,
                "step_ms": STEP_MS,
            },
            "visibility_timeout_seconds": VT,
            "max_runtime_seconds": 600,
            "max_attempts": 10,
            "retry_backoff_base_seconds": 0,
            "retry_backoff_max_seconds": 1,
        },
    )
    job = enq["job"]
    job_id = job["id"]
    logger.info("enqueued job %s", job_id)

    started = time.monotonic()
    saw_step = 0
    holder: str | None = None

    # Phase 1: wait for checkpoint progress.
    while time.monotonic() - started < MAX_WALL:
        j = get_job(job_id)
        if j is None:
            time.sleep(0.3)
            continue
        status = j.get("status")
        cp = j.get("checkpoint") or {}
        step = 0
        if isinstance(cp, dict) and cp.get("step") is not None:
            try:
                step = int(cp["step"])
            except (TypeError, ValueError):
                step = 0
        if step > saw_step:
            saw_step = step
            holder = j.get("leased_by")
            logger.info(
                "progress status=%s step=%s holder=%s",
                status,
                step,
                holder,
            )
        if step >= MIN_STEP and status == "leased" and holder:
            logger.info("reached min checkpoint step=%d holder=%s — killing holder", step, holder)
            break
        if status == "completed":
            logger.error("job completed before kill; checkpoint never forced reclaim")
            return 1
        time.sleep(0.25)
    else:
        logger.error("timeout waiting for checkpoint (last step=%s)", saw_step)
        return 1

    checkpoint_before_kill = saw_step
    holder_name = holder or "worker-1"
    # Map worker_id to compose service name (worker-1, worker-2, ...).
    kill_substr = holder_name if holder_name.startswith("worker-") else "worker-1"
    if not docker_kill(kill_substr):
        # Fall back: kill worker-1 container by common compose names.
        for cand in ("worker-1", f"{PROJECT}-worker-1", "checkpoint-worker-1"):
            if docker_kill(cand):
                kill_substr = cand
                break
        else:
            logger.error("failed to kill any worker container for %s", holder_name)
            return 1
    logger.info("killed %s; waiting for reclaim + complete", kill_substr)

    # Phase 2: complete with resume evidence.
    while time.monotonic() - started < MAX_WALL:
        j = get_job(job_id)
        if j is None:
            time.sleep(0.3)
            continue
        status = j.get("status")
        if status == "completed":
            result = j.get("result") or {}
            cp = j.get("checkpoint") or {}
            logger.info("completed result=%s checkpoint=%s", result, cp)
            resumed = bool(result.get("resumed"))
            started_step = int(result.get("started_step") or 0)
            finisher = result.get("worker_id")
            final_step = int(cp.get("step") or 0) if isinstance(cp, dict) else 0

            if not resumed and started_step < MIN_STEP:
                logger.error(
                    "did not resume from checkpoint: resumed=%s started_step=%s "
                    "(need started_step>=%s)",
                    resumed,
                    started_step,
                    MIN_STEP,
                )
                return 1
            if started_step < checkpoint_before_kill and not resumed:
                logger.error(
                    "started_step %s < pre-kill checkpoint %s",
                    started_step,
                    checkpoint_before_kill,
                )
                return 1
            if final_step < STEPS:
                logger.error("final checkpoint step %s < total %s", final_step, STEPS)
                return 1
            if finisher and finisher == holder_name:
                # Possible if container restarted and same id — still ok if resumed.
                logger.warning(
                    "same worker_id finished (%s); checking resume flags only",
                    finisher,
                )
            if started_step < 1 and not resumed:
                logger.error("no evidence of resume")
                return 1
            # Strong check: started after kill must be > 0 and ideally >= min.
            if started_step < MIN_STEP:
                logger.error(
                    "resume started_step=%s expected >= %s (pre-kill cp=%s)",
                    started_step,
                    MIN_STEP,
                    checkpoint_before_kill,
                )
                return 1
            logger.info(
                "CHECKPOINT SOAK PASSED job=%s resumed_from=%s finisher=%s "
                "pre_kill_step=%s final_step=%s",
                job_id,
                started_step,
                finisher,
                checkpoint_before_kill,
                final_step,
            )
            return 0
        if status in ("dead", "failed", "cancelled"):
            logger.error("job ended badly: %s error=%s", status, j.get("last_error"))
            return 1
        logger.info(
            "wait-complete status=%s step=%s holder=%s",
            status,
            (j.get("checkpoint") or {}).get("step") if isinstance(j.get("checkpoint"), dict) else None,
            j.get("leased_by"),
        )
        time.sleep(0.5)

    logger.error("timeout waiting for completion")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
