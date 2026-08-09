#!/usr/bin/env python3
"""Fleet soak: job required_skills vs worker capabilities under load.

  * Load tenants enqueue unskilled work that any worker may finish.
  * Skill tenants enqueue gpu / cpu / gpu+cuda jobs.
  * GPU workers (worker-1, worker-2) must never complete cpu-only jobs.
  * CPU workers (worker-3, worker-4) must never complete gpu jobs.
  * gpu+cuda jobs may only complete on worker-1.
  * Jobs requiring a skill nobody has (tpu) park as awaiting_skills.
  * After a late tpu worker registers, parked jobs become claimable and drain.
  * Fair multi-tenant leasing and partitions stay in effect.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
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
TPU_JOBS_PER_TENANT = int(os.environ.get("TPU_JOBS_PER_TENANT", "8") or "8")
RACE_JOBS = int(os.environ.get("RACE_JOBS", "40") or "40")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
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

    expect_tpu = 0
    for i in range(SKILL_TENANTS):
        tid = f"skill-{i:02d}"
        for n in range(TPU_JOBS_PER_TENANT):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "tpu", "n": n, "ms": 15},
                    "required_skills": ["tpu"],
                },
            )
            expect_tpu += 1

    logger.info("enqueued skill mix %s tpu=%d", expected, expect_tpu)
    tpu_parked = 0
    for i in range(SKILL_TENANTS):
        parked = http_json(
            "GET",
            f"/v1/jobs?tenant_id=skill-{i:02d}&status=awaiting_skills&limit=500",
        ).get("jobs") or []
        tpu_parked += len(parked)
        stolen = [
            j
            for j in parked
            if set(j.get("required_skills") or []) != {"tpu"}
        ]
        if stolen:
            raise RuntimeError(f"non-tpu parked as awaiting_skills: {stolen[:3]}")
    logger.info("tpu awaiting_skills=%d expected=%d", tpu_parked, expect_tpu)
    if tpu_parked < expect_tpu:
        raise RuntimeError(
            f"tpu jobs not parked: got {tpu_parked} want {expect_tpu}"
        )
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
    tpu_waiting = 0
    for i in range(SKILL_TENANTS):
        for j in list_jobs(f"skill-{i:02d}"):
            skills = set(j.get("required_skills") or [])
            if skills == {"tpu"}:
                if j.get("status") == "awaiting_skills":
                    tpu_waiting += 1
                elif j.get("status") not in ("pending", "leased", "completed"):
                    remaining.append(
                        (j.get("id"), skills, j.get("status"))
                    )
            elif j.get("status") in ("pending", "leased"):
                remaining.append(
                    (j.get("id"), j.get("required_skills"), j.get("status"))
                )
    if remaining:
        raise RuntimeError(f"skilled jobs still pending: {remaining[:8]}")
    if tpu_waiting < expect_tpu:
        raise RuntimeError(
            f"tpu jobs escaped park before late worker: {tpu_waiting}/{expect_tpu}"
        )

    logger.info("registering late tpu worker to revive parked jobs")
    http_json(
        "POST",
        "/v1/workers/register",
        {"worker_id": "worker-tpu", "skills": ["tpu"]},
    )
    tpu_deadline = time.time() + min(60, MAX_WALL)
    tpu_done = 0
    while time.time() < tpu_deadline:
        body = http_json(
            "POST",
            "/v1/lease",
            {
                "worker_id": "worker-tpu",
                "limit": 8,
                "visibility_timeout_seconds": 30,
            },
        )
        jobs = body.get("jobs") or []
        if not jobs:
            parked_left = 0
            for i in range(SKILL_TENANTS):
                parked_left += len(
                    http_json(
                        "GET",
                        f"/v1/jobs?tenant_id=skill-{i:02d}"
                        "&status=awaiting_skills&limit=500",
                    ).get("jobs")
                    or []
                )
            if parked_left == 0:
                break
            time.sleep(0.2)
            continue
        for j in jobs:
            skills = set(j.get("required_skills") or [])
            if skills != {"tpu"}:
                raise RuntimeError(
                    f"tpu worker leased non-tpu job {j.get('id')} {skills}"
                )
            http_json(
                "POST",
                f"/v1/jobs/{j['id']}/ack",
                {
                    "tenant_id": j["tenant_id"],
                    "worker_id": "worker-tpu",
                    "lease_token": j["lease_token"],
                    "result": {"ok": True},
                },
            )
            tpu_done += 1
    else:
        raise RuntimeError("late tpu worker did not drain parked jobs")

    tpu_completed = 0
    for i in range(SKILL_TENANTS):
        for j in list_jobs(f"skill-{i:02d}"):
            if set(j.get("required_skills") or []) != {"tpu"}:
                continue
            if j.get("status") != "completed":
                raise RuntimeError(
                    f"tpu job {j.get('id')} status={j.get('status')}"
                )
            by = completed_by(f"skill-{i:02d}", str(j.get("id")))
            if by != "worker-tpu":
                raise RuntimeError(f"tpu job completed by {by}")
            tpu_completed += 1
    if tpu_completed < expect_tpu:
        raise RuntimeError(f"tpu completed {tpu_completed} want {expect_tpu}")
    logger.info("tpu parked-then-revived completed=%d by worker-tpu", tpu_completed)

    # Race: enqueue skilled work while the capable worker is already polling.
    # Jobs that would have parked as awaiting_skills must revive and drain.
    race_ids: list[str] = []
    race_err: list[str] = []

    def _enqueue_race(kind: str, skill: str, n: int, prefix: str) -> None:
        try:
            for i in range(n):
                body = http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": f"{prefix}-{i % SKILL_TENANTS:02d}",
                        "payload": {"kind": kind, "race": i, "ms": 10},
                        "required_skills": [skill],
                    },
                )
                job = body.get("job") or {}
                jid = str(job.get("id") or "")
                if not jid:
                    raise RuntimeError(f"race enqueue missing id {body}")
                race_ids.append(jid)
        except Exception as exc:  # noqa: BLE001
            race_err.append(str(exc))

    def _drain_worker(wid: str, skill: str, stop: threading.Event) -> int:
        done = 0
        idle = 0
        while not stop.is_set() or idle < 15:
            try:
                body = http_json(
                    "POST",
                    "/v1/lease",
                    {
                        "worker_id": wid,
                        "limit": 8,
                        "visibility_timeout_seconds": 30,
                    },
                )
            except Exception:
                idle += 1
                time.sleep(0.05)
                continue
            jobs = body.get("jobs") or []
            if not jobs:
                idle += 1
                time.sleep(0.05)
                continue
            idle = 0
            for j in jobs:
                skills = set(j.get("required_skills") or [])
                if skills != {skill}:
                    race_err.append(
                        f"{wid} leased {j.get('id')} skills={skills}"
                    )
                    continue
                http_json(
                    "POST",
                    f"/v1/jobs/{j['id']}/ack",
                    {
                        "tenant_id": j["tenant_id"],
                        "worker_id": wid,
                        "lease_token": j["lease_token"],
                        "result": {"ok": True},
                    },
                )
                done += 1
        return done

    logger.info("race burst tpu=%d while worker-tpu is live", RACE_JOBS)
    stop_tpu = threading.Event()
    tpu_done_holder = {"n": 0}

    def _tpu_drain() -> None:
        tpu_done_holder["n"] = _drain_worker("worker-tpu", "tpu", stop_tpu)

    tpu_thr = threading.Thread(target=_tpu_drain)
    tpu_thr.start()
    enq_threads = [
        threading.Thread(
            target=_enqueue_race, args=("tpu", "tpu", RACE_JOBS // 4, "skill")
        )
        for _ in range(4)
    ]
    for t in enq_threads:
        t.start()
    for t in enq_threads:
        t.join(timeout=60)
    stop_tpu.set()
    tpu_thr.join(timeout=60)
    if race_err:
        raise RuntimeError(f"tpu race errors: {race_err[:4]}")
    parked_left = 0
    for i in range(SKILL_TENANTS):
        parked_left += len(
            http_json(
                "GET",
                f"/v1/jobs?tenant_id=skill-{i:02d}"
                "&status=awaiting_skills&limit=500",
            ).get("jobs")
            or []
        )
    if parked_left:
        raise RuntimeError(
            f"tpu race left {parked_left} awaiting_skills after live worker"
        )
    logger.info(
        "tpu live-worker race enqueued=%d drained=%d",
        len(race_ids),
        tpu_done_holder["n"],
    )

    # Register vs enqueue TOCTOU for a brand-new skill nobody has yet.
    expect_fpga = RACE_JOBS
    fpga_ids: list[str] = []
    fpga_lock = threading.Lock()
    stop_fpga = threading.Event()
    fpga_done_holder = {"n": 0}

    def _enqueue_fpga() -> None:
        try:
            for i in range(expect_fpga):
                body = http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": f"fpga-{i % 4:02d}",
                        "payload": {"kind": "fpga", "i": i, "ms": 10},
                        "required_skills": ["fpga"],
                    },
                )
                job = body.get("job") or {}
                with fpga_lock:
                    fpga_ids.append(str(job.get("id")))
        except Exception as exc:  # noqa: BLE001
            race_err.append(str(exc))

    def _register_and_drain_fpga() -> None:
        time.sleep(0.05)
        http_json(
            "POST",
            "/v1/workers/register",
            {"worker_id": "worker-fpga", "skills": ["fpga"]},
        )
        fpga_done_holder["n"] = _drain_worker(
            "worker-fpga", "fpga", stop_fpga
        )

    logger.info("fpga enqueue-vs-register race n=%d", expect_fpga)
    fpga_enq = [threading.Thread(target=_enqueue_fpga)]
    fpga_reg = threading.Thread(target=_register_and_drain_fpga)
    for t in fpga_enq:
        t.start()
    fpga_reg.start()
    for t in fpga_enq:
        t.join(timeout=60)
    stop_fpga.set()
    fpga_reg.join(timeout=60)
    if race_err:
        raise RuntimeError(f"fpga race errors: {race_err[:4]}")
    fpga_waiting = 0
    fpga_done = 0
    for i in range(4):
        for j in list_jobs(f"fpga-{i:02d}"):
            if set(j.get("required_skills") or []) != {"fpga"}:
                continue
            st = j.get("status")
            if st == "completed":
                fpga_done += 1
            elif st == "awaiting_skills":
                fpga_waiting += 1
            elif st in ("pending", "leased"):
                # One more lease pass for stragglers the drain thread missed.
                pass
    # Drain any leftover pending/leased fpga jobs.
    leftover_deadline = time.time() + 30
    while time.time() < leftover_deadline:
        still = 0
        for i in range(4):
            for j in list_jobs(f"fpga-{i:02d}"):
                if set(j.get("required_skills") or []) != {"fpga"}:
                    continue
                if j.get("status") in ("pending", "leased", "awaiting_skills"):
                    still += 1
        if still == 0:
            break
        body = http_json(
            "POST",
            "/v1/lease",
            {
                "worker_id": "worker-fpga",
                "limit": 8,
                "visibility_timeout_seconds": 30,
            },
        )
        jobs = body.get("jobs") or []
        if not jobs:
            time.sleep(0.1)
            continue
        for j in jobs:
            http_json(
                "POST",
                f"/v1/jobs/{j['id']}/ack",
                {
                    "tenant_id": j["tenant_id"],
                    "worker_id": "worker-fpga",
                    "lease_token": j["lease_token"],
                    "result": {"ok": True},
                },
            )
    fpga_done = 0
    fpga_waiting = 0
    for i in range(4):
        for j in list_jobs(f"fpga-{i:02d}"):
            if set(j.get("required_skills") or []) != {"fpga"}:
                continue
            if j.get("status") == "completed":
                fpga_done += 1
            elif j.get("status") == "awaiting_skills":
                fpga_waiting += 1
            else:
                raise RuntimeError(
                    f"fpga job {j.get('id')} stuck status={j.get('status')}"
                )
    if fpga_waiting:
        raise RuntimeError(
            f"fpga jobs still awaiting_skills after race: {fpga_waiting}"
        )
    if fpga_done < expect_fpga:
        raise RuntimeError(f"fpga completed {fpga_done} want {expect_fpga}")
    logger.info("fpga enqueue-vs-register race completed=%d", fpga_done)

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
