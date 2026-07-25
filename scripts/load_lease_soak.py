#!/usr/bin/env python3
"""Lease load test: 20 tenants × thousands of ready jobs, fair multi-tenant drain.

Measures enqueue wall time and drain throughput (jobs/sec) under concurrent
fair-lease workers. Used as baseline (Python lease path) vs optimized Go lease.
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
logger = logging.getLogger("load_lease_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or os.environ.get("COORDINATOR_URL")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
# Optional separate lease endpoint (Go). Defaults to same as coordinators.
LEASE_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("LEASE_URLS")
        or os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]

TENANTS = int(os.environ.get("TENANTS", "20"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "250"))  # 20*250 = 5000
ENQUEUE_WORKERS = int(os.environ.get("ENQUEUE_WORKERS", "16"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "600"))
POLL = float(os.environ.get("POLL_SECONDS", "1.0"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/tmp/load_lease_results.json")
LABEL = os.environ.get("LOAD_LABEL", "baseline")

_rr = 0
_lease_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    bases: list[str] | None = None,
    absolute: str | None = None,
    timeout: float = 60.0,
) -> tuple[int, dict[str, Any]]:
    global _rr, _lease_rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    if absolute:
        targets = [absolute]
    else:
        assert path is not None
        pool = bases if bases is not None else COORDINATOR_URLS
        # Separate RR for lease pool vs control-plane pool.
        if bases is LEASE_URLS:
            start = _lease_rr % len(pool)
            _lease_rr += 1
        else:
            start = _rr % len(pool)
            _rr += 1
        ordered = pool[start:] + pool[:start]
        targets = [b + path for b in ordered]

    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                code = resp.status
                if code == 204 or not raw:
                    return code, {}
                return code, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            if exc.code >= 500:
                last = RuntimeError(f"{url} -> {exc.code}: {payload}")
                continue
            return exc.code, payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"{method} {absolute or path} failed: {last}")


def wait_ready(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    pools = list({*COORDINATOR_URLS, *LEASE_URLS})
    while time.time() < deadline:
        ok = 0
        for b in pools:
            try:
                code, h = http_json("GET", absolute=b + "/health")
                if code == 200 and h.get("ok"):
                    ok += 1
            except Exception:
                pass
        if ok >= 1:
            logger.info("ready %d/%d endpoints", ok, len(pools))
            return
        time.sleep(1)
    raise RuntimeError("stack not ready")


def backlog() -> dict[str, Any]:
    # Prefer lease service if it exposes backlog (Go), else control plane.
    for pool in (LEASE_URLS, COORDINATOR_URLS):
        try:
            code, payload = http_json("GET", "/v1/admin/backlog", bases=pool)
            if code == 200:
                return payload
        except Exception:
            continue
    raise RuntimeError("backlog unavailable")


def enqueue_one(tid: str, i: int, priority: int) -> None:
    code, payload = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": tid,
            "priority": priority,
            "payload": {"tenant": tid, "i": i, "lane": "load"},
            "max_attempts": 3,
            "visibility_timeout_seconds": 30,
            "max_runtime_seconds": 120,
        },
        bases=COORDINATOR_URLS,
        timeout=30.0,
    )
    if code not in (200, 201):
        raise RuntimeError(f"enqueue failed {code}: {payload}")


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    tenants = [f"load-t{i:02d}" for i in range(TENANTS)]
    logger.info(
        "LOAD lease soak label=%s tenants=%d jobs/tenant=%d total=%d "
        "enqueue_workers=%d coords=%s lease=%s",
        LABEL,
        TENANTS,
        JOBS_PER_TENANT,
        total,
        ENQUEUE_WORKERS,
        COORDINATOR_URLS,
        LEASE_URLS,
    )
    wait_ready()

    # Pause so backlog piles up before drain starts (all ready together).
    for tid in tenants:
        try:
            http_json("POST", f"/v1/tenants/{tid}/pause", {}, bases=COORDINATOR_URLS)
        except Exception as exc:  # noqa: BLE001
            logger.warning("pause %s: %s", tid, exc)

    t_enq0 = time.perf_counter()
    errors = 0
    done = 0

    def task(args: tuple[str, int, int]) -> None:
        enqueue_one(*args)

    work: list[tuple[str, int, int]] = []
    for ti, tid in enumerate(tenants):
        for i in range(JOBS_PER_TENANT):
            # Mix priorities so lanes are exercised.
            prio = 100 if (i % 5 == 0) else (10 if i % 2 == 0 else 1)
            work.append((tid, i, prio))

    with ThreadPoolExecutor(max_workers=ENQUEUE_WORKERS) as pool:
        futs = [pool.submit(task, w) for w in work]
        for fut in as_completed(futs):
            try:
                fut.result()
                done += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if errors < 10:
                    logger.error("enqueue error: %s", exc)
            if done % 500 == 0 and done:
                logger.info("enqueued %d/%d errors=%d", done, total, errors)

    t_enq1 = time.perf_counter()
    enqueue_s = t_enq1 - t_enq0
    logger.info(
        "enqueue complete jobs=%d errors=%d wall=%.2fs rate=%.1f jobs/s",
        done,
        errors,
        enqueue_s,
        done / enqueue_s if enqueue_s > 0 else 0,
    )
    if errors:
        logger.error("aborting: enqueue errors")
        return 1
    if done != total:
        logger.error("enqueued %d != expected %d", done, total)
        return 1

    bl = backlog()
    logger.info("pre-drain backlog %s", bl)

    # Resume all → workers fair-lease drain.
    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/resume", {}, bases=COORDINATOR_URLS)

    t_drain0 = time.perf_counter()
    first_complete_s: float | None = None
    samples: list[dict[str, Any]] = []
    last_completed = 0

    while True:
        elapsed = time.perf_counter() - t_drain0
        if elapsed > MAX_WALL:
            logger.error("drain wall timeout after %.1fs: %s", elapsed, backlog())
            _write_results(
                {
                    "label": LABEL,
                    "ok": False,
                    "error": "timeout",
                    "tenants": TENANTS,
                    "jobs_per_tenant": JOBS_PER_TENANT,
                    "total_jobs": total,
                    "enqueue_seconds": enqueue_s,
                    "enqueue_jobs_per_sec": done / enqueue_s if enqueue_s else 0,
                    "drain_seconds": elapsed,
                    "samples": samples,
                    "final_backlog": backlog(),
                }
            )
            return 1
        try:
            bl = backlog()
        except Exception as exc:  # noqa: BLE001
            logger.warning("backlog poll: %s", exc)
            time.sleep(POLL)
            continue
        jobs = bl.get("jobs") or bl
        completed = int(jobs.get("completed") or 0)
        pending = int(jobs.get("pending") or 0)
        leased = int(jobs.get("leased") or 0)
        if first_complete_s is None and completed > 0:
            first_complete_s = elapsed
        delta = completed - last_completed
        last_completed = completed
        inst = delta / POLL if POLL > 0 else 0
        samples.append(
            {
                "t": round(elapsed, 3),
                "completed": completed,
                "pending": pending,
                "leased": leased,
                "inst_jobs_per_sec": round(inst, 2),
            }
        )
        logger.info(
            "drain t=%.1fs completed=%d/%d pending=%d leased=%d ~%.0f jobs/s",
            elapsed,
            completed,
            total,
            pending,
            leased,
            completed / elapsed if elapsed > 0 else 0,
        )
        if completed >= total and pending == 0 and leased == 0:
            # Stability check
            time.sleep(1.0)
            bl2 = backlog()
            j2 = bl2.get("jobs") or bl2
            if (
                int(j2.get("completed") or 0) >= total
                and int(j2.get("pending") or 0) == 0
                and int(j2.get("leased") or 0) == 0
            ):
                break
        time.sleep(POLL)

    t_drain1 = time.perf_counter()
    drain_s = t_drain1 - t_drain0
    thr = total / drain_s if drain_s > 0 else 0
    result = {
        "label": LABEL,
        "ok": True,
        "tenants": TENANTS,
        "jobs_per_tenant": JOBS_PER_TENANT,
        "total_jobs": total,
        "enqueue_seconds": round(enqueue_s, 4),
        "enqueue_jobs_per_sec": round(done / enqueue_s, 2) if enqueue_s else 0,
        "drain_seconds": round(drain_s, 4),
        "drain_jobs_per_sec": round(thr, 2),
        "time_to_first_complete_seconds": (
            round(first_complete_s, 4) if first_complete_s is not None else None
        ),
        "coords": COORDINATOR_URLS,
        "lease_urls": LEASE_URLS,
        "samples": samples,
        "final_backlog": backlog(),
    }
    _write_results(result)
    logger.info(
        "LOAD LEASE SOAK PASSED label=%s total=%d enqueue=%.2fs (%.0f/s) "
        "drain=%.2fs (%.0f jobs/s) ttf=%.2fs",
        LABEL,
        total,
        enqueue_s,
        done / enqueue_s if enqueue_s else 0,
        drain_s,
        thr,
        first_complete_s or -1,
    )
    # Machine-readable summary line for easy grepping.
    print(
        "RESULT "
        + json.dumps(
            {
                "label": LABEL,
                "ok": True,
                "total_jobs": total,
                "enqueue_seconds": result["enqueue_seconds"],
                "enqueue_jobs_per_sec": result["enqueue_jobs_per_sec"],
                "drain_seconds": result["drain_seconds"],
                "drain_jobs_per_sec": result["drain_jobs_per_sec"],
                "time_to_first_complete_seconds": result[
                    "time_to_first_complete_seconds"
                ],
            }
        ),
        flush=True,
    )
    return 0


def _write_results(payload: dict[str, Any]) -> None:
    try:
        with open(RESULTS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info("wrote results %s", RESULTS_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not write results: %s", exc)


if __name__ == "__main__":
    raise SystemExit(main())
