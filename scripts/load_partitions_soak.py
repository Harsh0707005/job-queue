#!/usr/bin/env python3
"""Fleet soak: multi-tenant multi-worker queue partitions + rebalance edges.

Validates:
  * fleet workers register and own exclusive partition shards
  * full partition coverage with no double-assignment
  * join/leave rebalance via phantom workers
  * heartbeat timeout marks dead + rebalances
  * multi-tenant jobs drain under partitioned leasing
  * dead workers cannot steal work after rebalance
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
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("load_partitions_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "8"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "40"))
ENQUEUE_WORKERS = int(os.environ.get("ENQUEUE_WORKERS", "12"))
PARTITION_COUNT = int(os.environ.get("PARTITION_COUNT", "16"))
MIN_FLEET_WORKERS = int(os.environ.get("MIN_FLEET_WORKERS", "4"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "300"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/results/partitions-fleet.json")
LABEL = os.environ.get("LOAD_LABEL", "partitions-fleet")
_rr = 0


def http_json(
    method: str, path: str, body: dict[str, Any] | None = None, timeout: float = 60.0
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDINATOR_URLS)
    _rr += 1
    ordered = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
    last: Exception | None = None
    for base in ordered:
        url = base + path
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return resp.status, {}
                return resp.status, json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode("utf-8", errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{url} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except urllib.error.URLError as exc:
            last = exc
            continue
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = 0
        for base in COORDINATOR_URLS:
            try:
                code, h = http_json("GET", "/health", timeout=5)
                if code == 200 and h.get("ok"):
                    ok += 1
                    break
            except Exception:  # noqa: BLE001
                pass
        if ok:
            return
        time.sleep(1)
    raise RuntimeError("not healthy")


def get_assignments() -> dict[str, Any]:
    code, payload = http_json("GET", "/v1/partitions")
    if code != 200:
        raise RuntimeError(f"partitions list failed {code}: {payload}")
    return payload


def ownership_map(payload: dict[str, Any]) -> dict[str, set[int]]:
    by_worker: dict[str, set[int]] = {}
    for a in payload.get("assignments") or []:
        wid = a.get("worker_id")
        if not wid:
            continue
        by_worker.setdefault(str(wid), set()).add(int(a["partition_id"]))
    return by_worker


def assert_exclusive_full_cover(
    by_worker: dict[str, set[int]], *, partition_count: int, min_workers: int
) -> None:
    if len(by_worker) < min_workers:
        raise AssertionError(
            f"expected >= {min_workers} active owners, got {sorted(by_worker)}"
        )
    seen: set[int] = set()
    for wid, parts in by_worker.items():
        if not parts:
            raise AssertionError(f"worker {wid} owns no partitions")
        overlap = seen & parts
        if overlap:
            raise AssertionError(f"double-owned partitions {sorted(overlap)}")
        seen |= parts
    expected = set(range(partition_count))
    if seen != expected:
        missing = expected - seen
        extra = seen - expected
        raise AssertionError(
            f"coverage gap missing={sorted(missing)} extra={sorted(extra)}"
        )


def wait_fleet_registered(
    *, partition_count: int, min_workers: int, timeout: float = 120.0
) -> dict[str, set[int]]:
    deadline = time.time() + timeout
    last: dict[str, set[int]] = {}
    while time.time() < deadline:
        payload = get_assignments()
        last = ownership_map(payload)
        try:
            assert_exclusive_full_cover(
                last, partition_count=partition_count, min_workers=min_workers
            )
            logger.info(
                "fleet registered workers=%s parts=%s sample=%s",
                len(last),
                partition_count,
                {w: sorted(p)[:4] for w, p in sorted(last.items())[:4]},
            )
            return last
        except AssertionError as exc:
            logger.info("waiting for fleet register: %s", exc)
            time.sleep(1.5)
    raise RuntimeError(f"fleet not partitioned in time: {last}")


def backlog() -> dict[str, Any]:
    code, payload = http_json("GET", "/v1/admin/backlog")
    if code != 200:
        raise RuntimeError(f"backlog failed {code}: {payload}")
    return payload


def enqueue_one(tid: str, i: int) -> None:
    code, payload = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": tid,
            "priority": 10 if i % 3 == 0 else 1,
            "payload": {"tenant": tid, "i": i, "lane": "partitions"},
            "max_attempts": 3,
            "visibility_timeout_seconds": 30,
            "max_runtime_seconds": 120,
        },
        timeout=30.0,
    )
    if code not in (200, 201):
        raise RuntimeError(f"enqueue failed {code}: {payload}")


def edge_join_leave(partition_count: int, baseline: dict[str, set[int]]) -> dict[str, Any]:
    """Register a phantom worker, verify rebalance, then leave."""
    phantom = f"phantom-join-{int(time.time())}"
    code, reg = http_json(
        "POST", "/v1/workers/register", {"worker_id": phantom}
    )
    if code != 200:
        raise RuntimeError(f"register phantom failed {code}: {reg}")
    parts = set(int(p) for p in (reg.get("partitions") or []))
    if not parts:
        raise AssertionError("phantom got no partitions on join")

    after = ownership_map(get_assignments())
    assert_exclusive_full_cover(
        after, partition_count=partition_count, min_workers=len(baseline) + 1
    )
    if phantom not in after:
        raise AssertionError("phantom missing from assignments after join")
    # Existing fleet workers should have shrunk (or at least total owners grew).
    if len(after) <= len(baseline):
        raise AssertionError(
            f"join did not increase owner count: before={len(baseline)} after={len(after)}"
        )

    code, der = http_json(
        "POST", "/v1/workers/deregister", {"worker_id": phantom}
    )
    if code != 200:
        raise RuntimeError(f"deregister phantom failed {code}: {der}")

    after_leave = ownership_map(get_assignments())
    if phantom in after_leave:
        raise AssertionError("phantom still owns partitions after leave")
    assert_exclusive_full_cover(
        after_leave,
        partition_count=partition_count,
        min_workers=max(1, len(baseline)),
    )
    return {
        "phantom": phantom,
        "parts_on_join": sorted(parts),
        "owners_after_join": len(after),
        "owners_after_leave": len(after_leave),
    }


def edge_death_timeout(partition_count: int, min_workers: int) -> dict[str, Any]:
    """Register a short-TTL phantom, starve heartbeats, expire + rebalance."""
    phantom = f"phantom-die-{int(time.time())}"
    ttl = 2
    code, reg = http_json(
        "POST",
        "/v1/workers/register",
        {"worker_id": phantom, "heartbeat_ttl_seconds": ttl},
    )
    if code != 200:
        raise RuntimeError(f"register die-phantom failed {code}: {reg}")
    if not reg.get("partitions"):
        raise AssertionError("die-phantom got no partitions")

    time.sleep(ttl + 1.0)
    code, exp = http_json(
        "POST",
        "/v1/workers/process-timeouts",
        {"heartbeat_ttl_seconds": ttl},
    )
    if code != 200:
        raise RuntimeError(f"process-timeouts failed {code}: {exp}")
    expired = int(exp.get("expired") or 0)
    if expired < 1:
        # Real fleet workers keep heartbeating; only phantom should expire.
        # Retry once in case clock skew / register lag.
        time.sleep(1.0)
        code, exp = http_json(
            "POST",
            "/v1/workers/process-timeouts",
            {"heartbeat_ttl_seconds": ttl},
        )
        expired = int((exp or {}).get("expired") or 0)
    if expired < 1:
        raise AssertionError(f"expected phantom timeout, expired={expired}")

    after = ownership_map(get_assignments())
    if phantom in after:
        raise AssertionError("dead phantom still owns partitions")
    assert_exclusive_full_cover(
        after, partition_count=partition_count, min_workers=min_workers
    )
    return {"phantom": phantom, "expired": expired, "owners_after": len(after)}


def edge_dead_cannot_steal(partition_count: int) -> dict[str, Any]:
    """Deregistered phantom must not be able to lease (empty filter)."""
    phantom = f"phantom-zombie-{int(time.time())}"
    tid = "part-zombie-edge"
    code, reg = http_json(
        "POST", "/v1/workers/register", {"worker_id": phantom}
    )
    if code != 200:
        raise RuntimeError(f"zombie register failed {code}: {reg}")
    code, _ = http_json("POST", "/v1/workers/deregister", {"worker_id": phantom})
    if code != 200:
        raise RuntimeError("zombie deregister failed")

    # Enqueue a job and try to lease as the dead phantom via fair lease.
    code, enq = http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": tid,
            "payload": {"edge": "zombie"},
            "max_attempts": 2,
            "visibility_timeout_seconds": 30,
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"zombie enqueue failed {code}: {enq}")

    # Dead worker lease batch must return empty (200 with [] or 204 no content).
    stolen = 0
    for _ in range(3):
        code, lease = http_json(
            "POST",
            "/v1/lease",
            {"worker_id": phantom, "limit": 5, "tenant_id": tid},
        )
        if code == 204:
            continue
        if code != 200:
            raise RuntimeError(f"zombie lease failed {code}: {lease}")
        jobs = lease.get("jobs") or ([] if not lease.get("job") else [lease["job"]])
        stolen += len(jobs)
    if stolen != 0:
        raise AssertionError(f"dead worker stole {stolen} jobs")
    return {"phantom": phantom, "stolen": stolen, "partition_count": partition_count}


def wait_drain(total: int, timeout: float) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = backlog()
        jobs = last.get("jobs") if isinstance(last.get("jobs"), dict) else last
        pending = int(jobs.get("pending") or 0)
        leased = int(jobs.get("leased") or 0)
        completed = int(jobs.get("completed") or 0)
        dead = int(jobs.get("dead") or 0)
        failed = int(jobs.get("failed") or 0)
        logger.info(
            "drain pending=%s leased=%s completed=%s dead=%s failed=%s",
            pending,
            leased,
            completed,
            dead,
            failed,
        )
        if pending == 0 and leased == 0 and (completed + dead) >= total:
            return {
                "pending": pending,
                "leased": leased,
                "completed": completed,
                "dead": dead,
                "failed": failed,
                "raw": last,
            }
        time.sleep(2)
    raise RuntimeError(f"drain timed out: {last}")


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    tenants = [f"pt-t{i:02d}" for i in range(TENANTS)]
    logger.info(
        "PARTITIONS soak label=%s tenants=%s jobs/tenant=%s total=%s "
        "partition_count=%s min_workers=%s",
        LABEL,
        TENANTS,
        JOBS_PER_TENANT,
        total,
        PARTITION_COUNT,
        MIN_FLEET_WORKERS,
    )
    wait_healthy()

    code, sc = http_json(
        "POST", "/v1/partitions/count", {"partition_count": PARTITION_COUNT}
    )
    if code != 200:
        raise RuntimeError(f"set partition count failed {code}: {sc}")
    logger.info("partition_count set to %s", sc.get("partition_count"))

    baseline = wait_fleet_registered(
        partition_count=PARTITION_COUNT, min_workers=MIN_FLEET_WORKERS
    )

    edges: dict[str, Any] = {}
    edges["join_leave"] = edge_join_leave(PARTITION_COUNT, baseline)
    logger.info("edge join/leave ok: %s", edges["join_leave"])

    # Re-snapshot fleet after leave (phantoms gone).
    baseline = wait_fleet_registered(
        partition_count=PARTITION_COUNT, min_workers=MIN_FLEET_WORKERS, timeout=60
    )
    edges["death_timeout"] = edge_death_timeout(PARTITION_COUNT, MIN_FLEET_WORKERS)
    logger.info("edge death timeout ok: %s", edges["death_timeout"])

    edges["dead_cannot_steal"] = edge_dead_cannot_steal(PARTITION_COUNT)
    logger.info("edge dead-cannot-steal ok: %s", edges["dead_cannot_steal"])

    # Multi-tenant drain under partitioned ownership.
    t0 = time.perf_counter()
    errors = 0
    done = 0

    def task(args: tuple[str, int]) -> None:
        enqueue_one(*args)

    work = [(tid, i) for tid in tenants for i in range(JOBS_PER_TENANT)]
    with ThreadPoolExecutor(max_workers=ENQUEUE_WORKERS) as pool:
        futs = [pool.submit(task, w) for w in work]
        for fut in as_completed(futs):
            try:
                fut.result()
                done += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.warning("enqueue error: %s", exc)
    enq_s = time.perf_counter() - t0
    if errors:
        raise RuntimeError(f"enqueue errors={errors} done={done}")
    logger.info("enqueued %s jobs in %.1fs", done, enq_s)

    # +1 for zombie-edge job
    drain = wait_drain(total + 1, timeout=MAX_WALL)
    drain_s = time.perf_counter() - t0

    final = ownership_map(get_assignments())
    assert_exclusive_full_cover(
        final, partition_count=PARTITION_COUNT, min_workers=MIN_FLEET_WORKERS
    )

    if int(drain.get("dead") or 0) > 0:
        raise AssertionError(f"unexpected dead letters: {drain}")

    result = {
        "label": LABEL,
        "ok": True,
        "tenants": TENANTS,
        "jobs_per_tenant": JOBS_PER_TENANT,
        "total_jobs": total,
        "partition_count": PARTITION_COUNT,
        "fleet_workers": len(final),
        "ownership": {w: sorted(p) for w, p in sorted(final.items())},
        "enqueue_seconds": round(enq_s, 3),
        "drain_seconds": round(drain_s, 3),
        "edges": edges,
        "drain": {k: v for k, v in drain.items() if k != "raw"},
    }
    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("PARTITIONS fleet GREEN: %s", json.dumps(result, default=str)[:800])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logger.exception("PARTITIONS fleet FAILED")
        raise SystemExit(1)
