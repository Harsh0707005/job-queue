#!/usr/bin/env python3
"""Chaos fleet: multi-tenant side effects run once under reclaim.

Enqueues jobs with several named effects. Workers POST to effect-sink then
complete_side_effect. Chaos kills workers mid-job so leases reclaim.
Success = every (job, effect) applied exactly once at the sink (unique_effects
== expected), and queue markers match.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("side_effect_chaos_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SINK = os.environ.get("EFFECT_SINK_URL", "http://effect-sink:8091").rstrip("/")
TENANTS = int(os.environ.get("TENANTS", "8"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "15"))
EFFECTS_PER_JOB = int(os.environ.get("EFFECTS_PER_JOB", "3"))
EFFECT_MS = int(os.environ.get("EFFECT_MS", "120"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300"))
_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 20.0,
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


def sink_stats() -> dict[str, Any]:
    req = urllib.request.Request(SINK + "/stats", method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def main() -> int:
    effects = [f"fx-{i}" for i in range(EFFECTS_PER_JOB)]
    total_jobs = TENANTS * JOBS_PER_TENANT
    expected_unique = total_jobs * EFFECTS_PER_JOB
    logger.info(
        "side-effect chaos tenants=%d jobs/t=%d effects=%s expected_unique=%d",
        TENANTS,
        JOBS_PER_TENANT,
        effects,
        expected_unique,
    )
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"coord not ready {b}")
    for _ in range(60):
        try:
            if sink_stats().get("unique_effects", 0) >= 0:
                break
        except Exception:
            time.sleep(1)

    def seed(t: int) -> list[tuple[str, str]]:
        tid = f"se-{t:02d}"
        out: list[tuple[str, str]] = []
        for i in range(JOBS_PER_TENANT):
            try:
                resp = http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": {
                            "kind": "side_effects",
                            "effects": effects,
                            "effect_ms": EFFECT_MS,
                        },
                        "priority": i % 3,
                        "visibility_timeout_seconds": 4,
                        "max_attempts": 12,
                        "retry_backoff_base_seconds": 0,
                        "max_runtime_seconds": 120,
                    },
                )
                jid = (resp.get("job") or {}).get("id") or resp.get("id")
                if jid:
                    out.append((tid, str(jid)))
            except Exception as exc:  # noqa: BLE001
                logger.warning("seed %s: %s", tid, exc)
        return out

    job_ids: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(8, TENANTS)) as pool:
        for f in as_completed([pool.submit(seed, t) for t in range(TENANTS)]):
            job_ids.extend(f.result())
    seeded = len(job_ids)
    logger.info("seeded %d", seeded)

    started = time.monotonic()
    while time.monotonic() - started < MAX_WALL:
        b = backlog()
        try:
            st = sink_stats()
        except Exception as exc:  # noqa: BLE001
            st = {"error": str(exc)}
        logger.info("progress backlog=%s sink=%s", b, st)
        if (
            int(b.get("completed") or 0) >= seeded
            and int(b.get("pending") or 0) == 0
            and int(b.get("leased") or 0) == 0
        ):
            time.sleep(2)
            b2 = backlog()
            if int(b2.get("leased") or 0) == 0 and int(b2.get("pending") or 0) == 0:
                break
        time.sleep(2)

    st = sink_stats()
    b = backlog()
    unique = int(st.get("unique_effects") or 0)
    total_posts = int(st.get("total_posts") or 0)
    multi_workers = int(st.get("effects_seen_by_multiple_workers") or 0)
    logger.info(
        "final backlog=%s unique_effects=%d total_posts=%d multi_worker_effects=%d",
        b,
        unique,
        total_posts,
        multi_workers,
    )

    # Verify durable markers for a sample of jobs (and all if small).
    sample = job_ids if len(job_ids) <= 40 else job_ids[:: max(1, len(job_ids) // 30)]
    marker_fail = 0
    for tid, jid in sample:
        try:
            resp = http_json(
                "GET", f"/v1/jobs/{jid}/side-effects?tenant_id={tid}", timeout=15.0
            )
            keys = {
                str(e.get("effect_key"))
                for e in (resp.get("effects") or [])
                if e.get("status") == "completed"
            }
            missing = set(effects) - keys
            if missing:
                logger.error(
                    "job %s tenant %s missing markers %s have=%s",
                    jid,
                    tid,
                    sorted(missing),
                    sorted(keys),
                )
                marker_fail += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("list side-effects %s: %s", jid, exc)
            marker_fail += 1

    if int(b.get("completed") or 0) < seeded * 0.95:
        logger.error("too few completed %s < %s", b.get("completed"), seeded)
        return 1
    if unique < expected_unique:
        logger.error(
            "unique effects %d < expected %d (effects not all applied)",
            unique,
            expected_unique,
        )
        return 1
    if unique > expected_unique:
        logger.error(
            "unique effects %d > expected %d (duplicate logical effects)",
            unique,
            expected_unique,
        )
        return 1
    if marker_fail:
        logger.error("marker verification failed on %d sampled jobs", marker_fail)
        return 1
    # Retries (total_posts > unique) are OK under reclaim if sink is idempotent.
    # multi_worker_effects is OK (reclaim) as long as unique count is exact.
    logger.info(
        "SIDE EFFECT CHAOS SOAK PASSED unique=%d posts=%d retries=%d multi_worker=%d markers_ok=%d",
        unique,
        total_posts,
        total_posts - unique,
        multi_workers,
        len(sample),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
