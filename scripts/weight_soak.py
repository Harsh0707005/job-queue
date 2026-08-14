#!/usr/bin/env python3
"""Fleet soak: weighted fair lease under multi-tenant multi-worker load.

  * Dual coordinators, four workers, four tenants.
  * Equal backlogs; weights 4 / 2 / 1 / 1.
  * Mid-drain: high-weight tenant has more completions than each low,
    and neither low is starved.
  * Every job still completes.
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
logger = logging.getLogger("weight_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
EACH = int(os.environ.get("JOBS_EACH", "60") or "60")
JOB_MS = int(os.environ.get("JOB_MS", "80") or "80")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
# Sample once total completed is in this window so we see share-under-load.
MID_MIN = int(os.environ.get("MID_MIN", "80") or "80")
MID_MAX = int(os.environ.get("MID_MAX", "160") or "160")
WEIGHTS = {
    "wt-heavy": 4,
    "wt-mid": 2,
    "wt-low-a": 1,
    "wt-low-b": 1,
}
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    start = _rr % len(COORDS)
    _rr += 1
    last: Exception | None = None
    for base in COORDS[start:] + COORDS[:start]:
        req = urllib.request.Request(
            base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                payload = json.loads(raw.decode()) if raw else {}
                return int(resp.status), payload
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            return int(exc.code), payload
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            code, body = http_json("GET", "/health")
            if code == 200 and body.get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def completed_of(jobs: list[dict[str, Any]]) -> int:
    return sum(1 for j in jobs if j.get("status") == "completed")


def enqueue(tid: str, n: int) -> None:
    for i in range(n):
        body: dict[str, Any] = {
            "tenant_id": tid,
            "payload": {"kind": "work", "ms": JOB_MS, "i": i, "t": tid},
        }
        code, resp = http_json("POST", "/v1/jobs", body)
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {resp}")


def tenant_counts() -> dict[str, int]:
    return {tid: completed_of(list_jobs(tid)) for tid in WEIGHTS}


def assert_mid(counts: dict[str, int]) -> None:
    heavy = counts["wt-heavy"]
    mid = counts["wt-mid"]
    low_a = counts["wt-low-a"]
    low_b = counts["wt-low-b"]
    lows = (low_a, low_b)
    if low_a < 1 or low_b < 1:
        raise RuntimeError(f"low-weight tenant starved at mid-drain: {counts}")
    if heavy <= max(lows):
        raise RuntimeError(
            f"heavy tenant did not outpace lows at mid-drain: {counts}"
        )
    if heavy < mid:
        raise RuntimeError(f"heavy fell behind mid at mid-drain: {counts}")
    # 4 workers racing makes the ratio noisy; require a clear tilt, not 4.00.
    ratio = heavy / max(max(lows), 1)
    if ratio < 1.4:
        raise RuntimeError(
            f"heavy/low ratio {ratio:.2f} too close to even: {counts}"
        )
    logger.info(
        "mid-drain OK heavy=%d mid=%d low_a=%d low_b=%d ratio=%.2f",
        heavy,
        mid,
        low_a,
        low_b,
        ratio,
    )


def main() -> int:
    tenants = list(WEIGHTS)
    logger.info(
        "weight soak tenants=%s weights=%s each=%d ms=%d",
        tenants,
        WEIGHTS,
        EACH,
        JOB_MS,
    )
    wait_coords()

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
        code, body = http_json(
            "POST",
            f"/v1/tenants/{tid}/lease-weight",
            {"lease_weight": WEIGHTS[tid]},
        )
        if code != 200 or int(body.get("lease_weight") or 0) != WEIGHTS[tid]:
            raise RuntimeError(f"set weight {tid} {code} {body}")
        code, got = http_json("GET", f"/v1/tenants/{tid}/lease-weight")
        if code != 200 or int(got.get("lease_weight") or 0) != WEIGHTS[tid]:
            raise RuntimeError(f"get weight {tid} {code} {got}")
        enqueue(tid, EACH)

    for tid in tenants:
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    deadline = time.time() + MAX_WALL
    last_log = 0.0
    mid_checked = False
    while time.time() < deadline:
        counts = tenant_counts()
        total = sum(counts.values())
        now = time.time()
        if now - last_log >= 2:
            logger.info("drain total=%d %s", total, counts)
            last_log = now
        if not mid_checked and MID_MIN <= total <= MID_MAX:
            assert_mid(counts)
            mid_checked = True
        if total == EACH * len(tenants):
            if not mid_checked:
                # Drained through the window between polls — use last snapshot
                # only if it still shows unfinished work is impossible; fail
                # rather than rubber-stamp a full-drain sample.
                raise RuntimeError(
                    f"drained too fast to sample mid-window "
                    f"(total={total} last={counts})"
                )
            logger.info("weight soak OK final=%s", counts)
            return 0
        time.sleep(0.15)
    raise RuntimeError(
        f"timed out mid_checked={mid_checked} last={tenant_counts()}"
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("weight soak failed")
        sys.exit(1)
