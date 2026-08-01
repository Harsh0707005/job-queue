#!/usr/bin/env python3
"""Fleet load soak that verifies durable metrics counters are not lost.

Enqueues N tenants × M jobs, drains via remote workers already running in the
compose fleet, then compares:

  * completed jobs (backlog / list_tenant stats path)
  * metric bucket sums for lease + ack

Fails if ack counters disagree with completed job count beyond a small
tolerance (reclaims can add extra leases).
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
logger = logging.getLogger("load_metrics_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or os.environ.get("COORDINATOR_URL")
        or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "10"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "100"))
ENQUEUE_WORKERS = int(os.environ.get("ENQUEUE_WORKERS", "16"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "600"))
POLL = float(os.environ.get("POLL_SECONDS", "1.0"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/tmp/load_metrics_results.json")
LABEL = os.environ.get("LOAD_LABEL", "metrics-fleet")
# Allow leases >= acks (reclaims); acks must match completed.
LEASE_SLACK_RATIO = float(os.environ.get("LEASE_SLACK_RATIO", "2.5"))

_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 60.0,
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
                code = resp.status
                if code == 204 or not raw:
                    return code, {}
                return code, json.loads(raw.decode("utf-8"))
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
        try:
            code, body = http_json("GET", "/health", timeout=5)
            if code == 200 and body.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not healthy")


def enqueue_all() -> float:
    jobs = [(f"m{t}", i) for t in range(TENANTS) for i in range(JOBS_PER_TENANT)]

    def one(item: tuple[str, int]) -> None:
        tenant, i = item
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tenant,
                "payload": {"n": i, "load": LABEL},
                "max_attempts": 3,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"enqueue failed {code}: {body}")

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=ENQUEUE_WORKERS) as pool:
        futs = [pool.submit(one, j) for j in jobs]
        for f in as_completed(futs):
            f.result()
    return time.time() - t0


def backlog_completed() -> int:
    code, body = http_json("GET", "/v1/admin/backlog")
    if code != 200:
        raise RuntimeError(f"backlog {code}: {body}")
    return int((body.get("jobs") or {}).get("completed") or 0)


def metrics_snapshot() -> dict[str, Any]:
    q = urlencode({"window_seconds": max(int(MAX_WALL), 3600), "format": "json"})
    code, body = http_json("GET", f"/v1/metrics/snapshot?{q}")
    if code != 200:
        raise RuntimeError(f"metrics snapshot {code}: {body}")
    return body


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    logger.info(
        "metrics soak label=%s tenants=%s jobs_per=%s total=%s coords=%s",
        LABEL,
        TENANTS,
        JOBS_PER_TENANT,
        total,
        COORDINATOR_URLS,
    )
    wait_healthy()
    enq_s = enqueue_all()
    logger.info("enqueued %s jobs in %.2fs", total, enq_s)

    t0 = time.time()
    last_done = 0
    while True:
        done = backlog_completed()
        if done != last_done:
            logger.info("completed=%s/%s", done, total)
            last_done = done
        if done >= total:
            break
        if time.time() - t0 > MAX_WALL:
            snap = metrics_snapshot()
            result = {
                "ok": False,
                "error": "timeout",
                "completed": done,
                "expected": total,
                "metrics": snap,
            }
            _write(result)
            return 1
        time.sleep(POLL)

    drain_s = time.time() - t0
    # Persist gauge history mid-run endpoint.
    http_json("POST", "/v1/metrics/sample", {})
    snap = metrics_snapshot()
    thr = snap.get("throughput") or {}
    leases = int((thr.get("lease") or {}).get("count") or 0)
    acks = int((thr.get("ack") or {}).get("count") or 0)
    nacks = int((thr.get("nack") or {}).get("count") or 0)
    completed = backlog_completed()

    # Sample CSV/Parquet paths still reachable under load.
    code_csv, _ = http_json("GET", "/v1/metrics/history?format=json")
    code_noisy, noisy = http_json("GET", "/v1/metrics/noisy?min_rate=0")

    ack_ok = acks == completed == total
    # Leases can exceed acks when reclaim/preempt happens; still must be >= acks.
    lease_ok = leases >= acks and leases <= int(acks * LEASE_SLACK_RATIO) + TENANTS
    ok = ack_ok and lease_ok and code_csv == 200 and code_noisy == 200

    result = {
        "ok": ok,
        "label": LABEL,
        "tenants": TENANTS,
        "jobs_per_tenant": JOBS_PER_TENANT,
        "expected": total,
        "completed": completed,
        "enqueue_seconds": enq_s,
        "drain_seconds": drain_s,
        "jobs_per_sec": (total / drain_s) if drain_s > 0 else 0,
        "metrics": {
            "lease_count": leases,
            "ack_count": acks,
            "nack_count": nacks,
            "ack_matches_completed": ack_ok,
            "lease_sane": lease_ok,
            "noisy_tenant_count": len((noisy or {}).get("noisy_tenants") or []),
        },
        "snapshot_gauges": (snap.get("gauges") or {}),
        "throughput": thr,
    }
    _write(result)
    if ok:
        logger.info(
            "PASS metrics: ack=%s lease=%s completed=%s j/s=%.1f",
            acks,
            leases,
            completed,
            result["jobs_per_sec"],
        )
        return 0
    logger.error("FAIL metrics integrity: %s", json.dumps(result["metrics"]))
    return 1


def _write(result: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("wrote %s", RESULTS_PATH)


if __name__ == "__main__":
    sys.exit(main())
