#!/usr/bin/env python3
"""Fleet soak: worker maintenance drain / resume under load.

  * Drain a skilled worker: it finishes held work and takes nothing new.
  * Health lists it as draining; heartbeat keeps skills.
  * Resume: it leases again (including exclusive cuda work) with skills intact.
  * Multi-tenant churn: rotate drain/resume across the fleet; everything finishes.
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
logger = logging.getLogger("worker_maintenance_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
LONG_MS = int(os.environ.get("LONG_MS", "1800") or "1800")
SHORT_MS = int(os.environ.get("SHORT_MS", "40") or "40")
STAGE1_JOBS = int(os.environ.get("STAGE1_JOBS", "8") or "8")
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "3") or "3")
JOBS_PER_LOAD = int(os.environ.get("JOBS_PER_LOAD", "16") or "16")
CHURN_ROUNDS = int(os.environ.get("CHURN_ROUNDS", "3") or "3")
_rr = 0

CUDA_ONLY = "worker-1"
ALL_WORKERS = ("worker-1", "worker-2", "worker-3", "worker-4")
EXPECTED_SKILLS = {
    "worker-1": {"cuda", "gpu"},
    "worker-2": {"gpu"},
    "worker-3": {"cpu"},
    "worker-4": {"cpu", "highmem"},
}


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
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            payload["_http"] = int(exc.code)
            return payload
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


def wait_workers(timeout: float = 60.0) -> dict[str, dict[str, Any]]:
    deadline = time.time() + timeout
    last: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        body = http_json("GET", "/v1/workers")
        last = {w["worker_id"]: w for w in body.get("workers") or []}
        if all(w in last and last[w].get("status") == "active" for w in ALL_WORKERS):
            return last
        time.sleep(0.5)
    raise RuntimeError(f"workers not active: {sorted(last)}")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    return list(body.get("jobs") or [])


def completed_by(tid: str, job_id: str) -> str | None:
    body = http_json("GET", f"/v1/jobs/{job_id}/audit?tenant_id={tid}&limit=200")
    for ev in reversed(body.get("events") or []):
        if ev.get("event_type") == "completed" and ev.get("actor"):
            return str(ev["actor"])
    return None


def completed_event_count(tid: str, job_id: str) -> int:
    body = http_json("GET", f"/v1/jobs/{job_id}/audit?tenant_id={tid}&limit=200")
    return sum(
        1
        for ev in (body.get("events") or [])
        if ev.get("event_type") == "completed"
    )


def enqueue(
    tenant_id: str,
    payload: dict[str, Any],
    *,
    required_skills: list[str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"tenant_id": tenant_id, "payload": payload}
    if required_skills is not None:
        body["required_skills"] = required_skills
    out = http_json("POST", "/v1/jobs", body)
    job = out.get("job") if isinstance(out.get("job"), dict) else out
    if not job or not job.get("id"):
        raise RuntimeError(f"enqueue failed: {out}")
    return job


def health_draining() -> list[str]:
    h = http_json("GET", "/health")
    return [str(x) for x in (h.get("draining_workers") or [])]


def assert_skills_intact() -> None:
    workers = {w["worker_id"]: w for w in http_json("GET", "/v1/workers").get("workers") or []}
    for wid, want in EXPECTED_SKILLS.items():
        got = set(workers[wid].get("skills") or [])
        if got != want:
            raise RuntimeError(f"{wid} skills {got} != {want}")


def wait_until(pred, *, timeout: float, msg: str, interval: float = 0.25) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return
        time.sleep(interval)
    raise RuntimeError(f"{msg} (last={last})")


def main() -> int:
    started = time.time()
    logger.info("worker maintenance soak coords=%s", COORDS)
    wait_coords()
    wait_workers()
    assert_skills_intact()

    # --- Stage 1: drain stops new leases; held work finishes ---
    logger.info("stage1: drain %s under inflight cuda work", CUDA_ONLY)
    s1 = "maint-s1"
    held_ids: set[str] = set()
    for n in range(STAGE1_JOBS):
        enqueue(
            s1,
            {"kind": "cuda", "n": n, "ms": LONG_MS, "stage": 1},
            required_skills=["gpu", "cuda"],
        )
    wait_until(
        lambda: any(j.get("leased_by") == CUDA_ONLY for j in list_jobs(s1)),
        timeout=20,
        msg="worker-1 never leased stage1 cuda work",
    )
    before = list_jobs(s1)
    held_ids = {
        str(j["id"])
        for j in before
        if j.get("leased_by") == CUDA_ONLY and j.get("status") == "leased"
    }
    if not held_ids:
        raise RuntimeError("no inflight cuda jobs on worker-1 at drain")
    drained = http_json("POST", f"/v1/workers/{CUDA_ONLY}/drain")
    if drained.get("status") != "draining":
        raise RuntimeError(f"drain failed: {drained}")
    wait_until(
        lambda: CUDA_ONLY in health_draining(),
        timeout=10,
        msg="health missing draining worker-1",
    )
    w1 = http_json("GET", f"/v1/workers/{CUDA_ONLY}")
    if w1.get("status") != "draining" or set(w1.get("skills") or []) != EXPECTED_SKILLS[CUDA_ONLY]:
        raise RuntimeError(f"drain snapshot bad: {w1}")

    extra_ids = []
    for n in range(STAGE1_JOBS):
        extra_ids.append(
            enqueue(
                s1,
                {"kind": "cuda", "n": 100 + n, "ms": SHORT_MS, "stage": "1b"},
                required_skills=["gpu", "cuda"],
            )["id"]
        )

    wait_until(
        lambda: all(
            j.get("status") == "completed"
            for j in list_jobs(s1)
            if str(j["id"]) in held_ids
        ),
        timeout=30,
        msg="held jobs did not finish after drain",
    )
    time.sleep(1.2)
    extras = [j for j in list_jobs(s1) if str(j["id"]) in {str(x) for x in extra_ids}]
    if any(j.get("status") == "completed" for j in extras):
        raise RuntimeError("draining worker completed post-drain cuda jobs")
    if any(j.get("leased_by") == CUDA_ONLY and j.get("status") == "leased" for j in extras):
        raise RuntimeError("draining worker took a new lease")
    hb = http_json("POST", "/v1/workers/heartbeat", {"worker_id": CUDA_ONLY})
    if hb.get("status") != "draining":
        raise RuntimeError(f"heartbeat cleared drain: {hb}")
    if set(hb.get("skills") or []) != EXPECTED_SKILLS[CUDA_ONLY]:
        raise RuntimeError(f"heartbeat lost skills: {hb}")
    logger.info("stage1 ok held=%d extras_waiting=%d", len(held_ids), len(extras))

    # --- Stage 2: resume leases again, exclusive cuda drains ---
    logger.info("stage2: resume %s", CUDA_ONLY)
    resumed = http_json("POST", f"/v1/workers/{CUDA_ONLY}/resume")
    if resumed.get("status") != "active":
        raise RuntimeError(f"resume failed: {resumed}")
    if set(resumed.get("skills") or []) != EXPECTED_SKILLS[CUDA_ONLY]:
        raise RuntimeError(f"resume lost skills: {resumed}")
    wait_until(
        lambda: CUDA_ONLY not in health_draining(),
        timeout=10,
        msg="health still lists worker-1 after resume",
    )
    wait_until(
        lambda: all(j.get("status") == "completed" for j in list_jobs(s1)),
        timeout=40,
        msg="stage1 extras did not drain after resume",
    )
    for j in list_jobs(s1):
        by = completed_by(s1, str(j["id"]))
        if by != CUDA_ONLY:
            raise RuntimeError(f"cuda job {j['id']} completed by {by}")
    assert_skills_intact()
    logger.info("stage2 ok extras drained by %s", CUDA_ONLY)

    # --- Stage 3: multi-tenant churn ---
    logger.info("stage3: multi-tenant drain/resume churn")
    tenants = [f"load-{i:02d}" for i in range(LOAD_TENANTS)] + ["skill-gpu", "skill-cpu"]
    expected = 0
    for i, tid in enumerate(tenants):
        if tid.startswith("skill-gpu"):
            for n in range(JOBS_PER_LOAD):
                enqueue(
                    tid,
                    {"kind": "gpu", "n": n, "ms": SHORT_MS, "t": tid},
                    required_skills=["gpu"],
                )
                expected += 1
        elif tid.startswith("skill-cpu"):
            for n in range(JOBS_PER_LOAD):
                enqueue(
                    tid,
                    {"kind": "cpu", "n": n, "ms": SHORT_MS, "t": tid},
                    required_skills=["cpu"],
                )
                expected += 1
        else:
            for n in range(JOBS_PER_LOAD):
                enqueue(tid, {"kind": "any", "n": n, "ms": SHORT_MS, "t": tid})
                expected += 1

    rotation = list(ALL_WORKERS)
    for rnd in range(CHURN_ROUNDS):
        wid = rotation[rnd % len(rotation)]
        d = http_json("POST", f"/v1/workers/{wid}/drain")
        if d.get("status") != "draining":
            raise RuntimeError(f"churn drain {wid}: {d}")
        wait_until(
            lambda w=wid: w in health_draining(),
            timeout=8,
            msg=f"health missing {wid} during churn",
        )
        time.sleep(0.8)
        snap = http_json("GET", f"/v1/workers/{wid}")
        if set(snap.get("skills") or []) != EXPECTED_SKILLS[wid]:
            raise RuntimeError(f"churn lost skills {wid}: {snap}")
        r = http_json("POST", f"/v1/workers/{wid}/resume")
        if r.get("status") != "active":
            raise RuntimeError(f"churn resume {wid}: {r}")
        logger.info("churn round %d drained+resumed %s", rnd, wid)

    deadline = time.time() + max(30.0, MAX_WALL - (time.time() - started))
    while time.time() < deadline:
        pending = 0
        done = 0
        for tid in tenants:
            for j in list_jobs(tid):
                if j.get("status") == "completed":
                    done += 1
                elif j.get("status") in ("pending", "leased", "awaiting_skills"):
                    pending += 1
        if pending == 0 and done >= expected:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("stage3 timed out waiting for fleet drain")

    for tid in ("skill-gpu",):
        for j in list_jobs(tid):
            if j.get("status") != "completed":
                continue
            by = completed_by(tid, str(j["id"]))
            if by not in {"worker-1", "worker-2"}:
                raise RuntimeError(f"gpu job completed by {by}")
    for j in list_jobs("skill-cpu"):
        if j.get("status") != "completed":
            continue
        by = completed_by("skill-cpu", str(j["id"]))
        if by not in {"worker-3", "worker-4"}:
            raise RuntimeError(f"cpu job completed by {by}")

    leftover = health_draining()
    if leftover:
        raise RuntimeError(f"workers still draining: {leftover}")
    assert_skills_intact()

    # --- Stage 4: release held leases so peers pick up long work ---
    drop = "worker-2"
    rel_tid = "maint-rel"
    logger.info("stage4: drain %s with release_held", drop)
    rel_ids = []
    for n in range(STAGE1_JOBS):
        rel_ids.append(
            enqueue(
                rel_tid,
                {"kind": "gpu", "n": n, "ms": LONG_MS, "stage": 4},
                required_skills=["gpu"],
            )["id"]
        )
    wait_until(
        lambda: any(j.get("leased_by") == drop for j in list_jobs(rel_tid)),
        timeout=20,
        msg="worker-2 never leased stage4 gpu work",
    )
    held_then = {
        str(j["id"])
        for j in list_jobs(rel_tid)
        if j.get("leased_by") == drop and j.get("status") == "leased"
    }
    if not held_then:
        raise RuntimeError("no inflight gpu jobs on worker-2 at release drain")
    dropped = http_json(
        "POST", f"/v1/workers/{drop}/drain", {"release_held": True}
    )
    if dropped.get("status") != "draining":
        raise RuntimeError(f"release drain failed: {dropped}")
    if int(dropped.get("released") or 0) < 1:
        raise RuntimeError(f"expected released>0: {dropped}")
    released_ids = {str(x) for x in (dropped.get("released_job_ids") or [])}
    if not released_ids:
        raise RuntimeError(f"no released_job_ids: {dropped}")
    after = {str(j["id"]): j for j in list_jobs(rel_tid)}
    for jid in released_ids:
        row = after.get(jid)
        if row is None:
            raise RuntimeError(f"released job {jid} disappeared")
        if row.get("status") == "completed":
            raise RuntimeError(f"released job {jid} completed by the drop")
        if row.get("leased_by") == drop:
            raise RuntimeError(f"released job {jid} still held by {drop}")
        if row.get("status") not in ("pending", "leased"):
            raise RuntimeError(f"released job {jid} status={row.get('status')}")
    extra_rel = enqueue(
        rel_tid,
        {"kind": "gpu", "n": 99, "ms": SHORT_MS, "stage": "4b"},
        required_skills=["gpu"],
    )["id"]
    time.sleep(0.6)
    extra_row = next(j for j in list_jobs(rel_tid) if str(j["id"]) == str(extra_rel))
    if extra_row.get("leased_by") == drop:
        raise RuntimeError("draining worker took a new lease after release")
    wait_until(
        lambda: all(j.get("status") == "completed" for j in list_jobs(rel_tid)),
        timeout=40,
        msg="released gpu jobs did not finish on peers",
    )
    for j in list_jobs(rel_tid):
        jid = str(j["id"])
        n_done = completed_event_count(rel_tid, jid)
        if n_done != 1:
            raise RuntimeError(f"job {jid} completed {n_done} times")
        if jid in released_ids:
            by = completed_by(rel_tid, jid)
            if by == drop:
                raise RuntimeError(f"released job {jid} completed by draining {drop}")
            if by not in {"worker-1", "worker-2"}:
                raise RuntimeError(f"gpu job {jid} completed by {by}")
            if by == "worker-2":
                raise RuntimeError(f"released job completed by {drop}")
    resumed = http_json("POST", f"/v1/workers/{drop}/resume")
    if resumed.get("status") != "active":
        raise RuntimeError(f"stage4 resume failed: {resumed}")
    if set(resumed.get("skills") or []) != EXPECTED_SKILLS[drop]:
        raise RuntimeError(f"stage4 resume lost skills: {resumed}")
    logger.info(
        "stage4 ok released=%d extras_completed_on_peer",
        len(released_ids),
    )

    leftover = health_draining()
    if leftover:
        raise RuntimeError(f"workers still draining: {leftover}")
    assert_skills_intact()
    live = {w["worker_id"]: w for w in http_json("GET", "/v1/workers").get("workers") or []}
    for wid in ALL_WORKERS:
        if live[wid].get("status") != "active":
            raise RuntimeError(f"{wid} not active at end: {live[wid]}")

    logger.info(
        "worker maintenance soak ok in %.1fs jobs=%d",
        time.time() - started,
        STAGE1_JOBS * 2 + expected + STAGE1_JOBS + 1,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("worker maintenance soak failed")
        sys.exit(1)
