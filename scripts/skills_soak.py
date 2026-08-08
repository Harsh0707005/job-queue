#!/usr/bin/env python3
"""Fleet soak: job required_skills vs worker capabilities under load.

  * Load tenants enqueue unskilled work that any worker may finish.
  * Skill tenants enqueue gpu / cpu / gpu+cuda jobs.
  * GPU workers (worker-1, worker-2) must never complete cpu-only jobs.
  * CPU workers (worker-3, worker-4) must never complete gpu jobs.
  * gpu+cuda jobs may only complete on worker-1.
  * Fair multi-tenant leasing and partitions stay in effect.
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
logger = logging.getLogger("skills_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "3") or "3")
JOBS_PER_LOAD = int(os.environ.get("JOBS_PER_LOAD", "12") or "12")
SKILL_TENANTS = int(os.environ.get("SKILL_TENANTS", "3") or "3")
JOBS_PER_SKILL = int(os.environ.get("JOBS_PER_SKILL", "8") or "8")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0

GPU_WORKERS = {"worker-1", "worker-2"}
CPU_WORKERS = {"worker-3", "worker-4"}
CUDA_WORKERS = {"worker-1"}


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


def list_jobs(tid: str) -> list[dict[str, Any]]:
    body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    return list(body.get("jobs") or [])


def completed_by(tid: str, job_id: str) -> str | None:
    """Ack clears leased_by; the completer is on the audit 'completed' event."""
    body = http_json(
        "GET", f"/v1/jobs/{job_id}/audit?tenant_id={tid}&limit=200"
    )
    for ev in reversed(body.get("events") or []):
        if ev.get("event_type") == "completed" and ev.get("actor"):
            return str(ev["actor"])
    return None


def main() -> int:
    logger.info(
        "skills soak load=%d skill_tenants=%d", LOAD_TENANTS, SKILL_TENANTS
    )
    wait_coords()

    for i in range(LOAD_TENANTS):
        tid = f"load-{i:02d}"
        for n in range(JOBS_PER_LOAD):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 25, "i": n, "t": tid},
                },
            )

    expected = {"gpu": 0, "cpu": 0, "cuda": 0, "any": 0}
    for i in range(SKILL_TENANTS):
        tid = f"skill-{i:02d}"
        for n in range(JOBS_PER_SKILL):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "gpu", "n": n, "ms": 20},
                    "required_skills": ["gpu"],
                },
            )
            expected["gpu"] += 1
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "cpu", "n": n, "ms": 20},
                    "required_skills": ["cpu"],
                },
            )
            expected["cpu"] += 1
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "cuda", "n": n, "ms": 20},
                    "required_skills": ["gpu", "cuda"],
                },
            )
            expected["cuda"] += 1
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "any", "n": n, "ms": 15},
                },
            )
            expected["any"] += 1

    logger.info("enqueued skill mix %s", expected)
    deadline = time.time() + MAX_WALL
    last_log = 0.0
    while time.time() < deadline:
        load_pending = 0
        skill_stats: dict[str, dict[str, int]] = {
            k: {"completed": 0, "pending": 0, "other": 0} for k in expected
        }
        for i in range(LOAD_TENANTS):
            for j in list_jobs(f"load-{i:02d}"):
                if j.get("status") in ("pending", "leased"):
                    load_pending += 1
        for i in range(SKILL_TENANTS):
            for j in list_jobs(f"skill-{i:02d}"):
                skills = set(j.get("required_skills") or [])
                if "cuda" in skills:
                    kind = "cuda"
                elif skills == {"gpu"}:
                    kind = "gpu"
                elif skills == {"cpu"}:
                    kind = "cpu"
                else:
                    kind = "any"
                st = j.get("status")
                if st == "completed":
                    skill_stats[kind]["completed"] += 1
                elif st in ("pending", "leased"):
                    skill_stats[kind]["pending"] += 1
                else:
                    skill_stats[kind]["other"] += 1
        now = time.time()
        if now - last_log >= 2:
            logger.info(
                "load_pending=%d skills=%s", load_pending, skill_stats
            )
            last_log = now
        # Skilled jobs must fully drain even when hashed onto a worker that
        # cannot run them (cross-shard claim by capable peers).
        if (
            load_pending == 0
            and all(skill_stats[k]["pending"] == 0 for k in expected)
            and all(
                skill_stats[k]["completed"] >= expected[k] for k in expected
            )
        ):
            logger.info("skills soak progressing to final assertions")
            break
        time.sleep(1)
    else:
        raise RuntimeError("skills soak timed out waiting for drain")

    # Final mismatch + progress checks after a short settle.
    time.sleep(1.5)
    cuda_by: set[str] = set()
    gpu_by: set[str] = set()
    cpu_by: set[str] = set()
    any_done = 0
    for i in range(SKILL_TENANTS):
        for j in list_jobs(f"skill-{i:02d}"):
            if j.get("status") != "completed":
                continue
            by = completed_by(f"skill-{i:02d}", str(j.get("id")))
            skills = set(j.get("required_skills") or [])
            if "cuda" in skills:
                if by not in CUDA_WORKERS:
                    raise RuntimeError(f"cuda completed by {by}")
                cuda_by.add(str(by))
            elif skills == {"gpu"}:
                if by not in GPU_WORKERS:
                    raise RuntimeError(f"gpu completed by {by}")
                gpu_by.add(str(by))
            elif skills == {"cpu"}:
                if by not in CPU_WORKERS:
                    raise RuntimeError(f"cpu completed by {by}")
                cpu_by.add(str(by))
            elif not skills:
                any_done += 1
    remaining = []
    for i in range(SKILL_TENANTS):
        for j in list_jobs(f"skill-{i:02d}"):
            if j.get("status") in ("pending", "leased"):
                remaining.append(
                    (j.get("id"), j.get("required_skills"), j.get("status"))
                )
    if remaining:
        raise RuntimeError(f"skilled jobs still pending: {remaining[:8]}")
    if any_done < expected["any"]:
        raise RuntimeError(f"unskilled skill-tenant jobs incomplete {any_done}")
    if not cuda_by or not gpu_by or not cpu_by:
        raise RuntimeError(
            f"missing skilled completions cuda={cuda_by} gpu={gpu_by} cpu={cpu_by}"
        )

    load_ok = False
    for i in range(LOAD_TENANTS):
        jobs = list_jobs(f"load-{i:02d}")
        done = sum(1 for j in jobs if j.get("status") == "completed")
        logger.info("load tenant load-%02d completed=%d total=%d", i, done, len(jobs))
        if done >= JOBS_PER_LOAD * 0.8:
            load_ok = True
    if not load_ok:
        raise RuntimeError("load tenants did not complete unskilled work")

    logger.info(
        "skills soak OK cuda_by=%s gpu_by=%s cpu_by=%s",
        sorted(cuda_by),
        sorted(gpu_by),
        sorted(cpu_by),
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("skills soak failed")
        sys.exit(1)
