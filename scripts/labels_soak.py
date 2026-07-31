#!/usr/bin/env python3
"""Fleet soak: job labels for list + bulk cancel across tenants/workers.

  1. Enqueue labeled long-running jobs across tenants.
  2. List by tenant + label + status; assert isolation and filters.
  3. cancel-matching one label per tenant while workers hold leases.
  4. Assert matching jobs cancelled, other labels untouched, no hung leases.
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
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("labels_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "6") or "6")
LONG_MS = int(os.environ.get("LONG_MS", "8000") or "8000")
# Force multi-batch cancel sweeps (default cancel batch is 500).
CANCEL_BATCH = int(os.environ.get("CANCEL_BATCH", "75") or "75")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "90") or "90")
_rr = 0


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
                payload = {"error": raw.decode(errors="replace")}
            last = RuntimeError(f"{exc.code}: {payload}")
            if 400 <= int(exc.code) < 500:
                raise last
            continue
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    return {k: int(j.get(k) or 0) for k in j}


def list_jobs(tenant_id: str, **filters: Any) -> list[dict[str, Any]]:
    # Page through results so stress runs > default list limit still verify.
    page_size = int(filters.get("page_size") or 500)
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        q: list[tuple[str, str]] = [
            ("tenant_id", tenant_id),
            ("limit", str(page_size)),
            ("offset", str(offset)),
        ]
        for lab in filters.get("labels") or []:
            q.append(("label", lab))
        status = filters.get("status")
        if status:
            if isinstance(status, str):
                q.append(("status", status))
            else:
                for s in status:
                    q.append(("status", s))
        payload = http_json("GET", f"/v1/jobs?{urlencode(q)}")
        batch = list(payload.get("jobs") or [])
        out.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
        if offset > 50_000:
            break
    return out


def main() -> int:
    logger.info(
        "labels soak tenants=%d jobs/tenant=%d long_ms=%d",
        TENANTS,
        JOBS_PER_TENANT,
        LONG_MS,
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

    wave_ids: dict[str, list[str]] = {}
    keep_ids: dict[str, list[str]] = {}
    for i in range(TENANTS):
        tid = f"lbl-{i:02d}"
        wave_ids[tid] = []
        keep_ids[tid] = []
        for j in range(JOBS_PER_TENANT):
            # Majority: wave:canary — will bulk cancel
            resp = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": LONG_MS, "i": j},
                    "labels": ["wave:canary", f"tenant:{tid}", "kind:long"],
                    "priority": 0,
                    "max_attempts": 3,
                    "visibility_timeout_seconds": 45,
                },
            )
            wave_ids[tid].append((resp.get("job") or {})["id"])
        # Survivors: different wave label
        for j in range(2):
            resp = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 100, "keep": j},
                    "labels": ["wave:keep", f"tenant:{tid}"],
                    "priority": 5,
                    "max_attempts": 2,
                    "visibility_timeout_seconds": 30,
                },
            )
            keep_ids[tid].append((resp.get("job") or {})["id"])
        logger.info(
            "tenant %s wave=%d keep=%d",
            tid,
            len(wave_ids[tid]),
            len(keep_ids[tid]),
        )

    # Wait until some work is leased.
    leased_peak = 0
    for _ in range(40):
        b = backlog()
        leased_peak = max(leased_peak, int(b.get("leased") or 0))
        logger.info("pre-cancel backlog=%s", b)
        if leased_peak >= min(4, TENANTS):
            break
        time.sleep(0.4)
    if leased_peak < 1:
        logger.error("no jobs leased before cancel")
        return 1

    # List filter checks before cancel.
    tid0 = "lbl-00"
    listed = list_jobs(tid0, labels=["wave:canary"])
    if len(listed) != JOBS_PER_TENANT:
        logger.error(
            "list filter wrong count tenant=%s got=%d want=%d",
            tid0,
            len(listed),
            JOBS_PER_TENANT,
        )
        return 1
    if any(j.get("tenant_id") != tid0 for j in listed):
        logger.error("list leaked other tenant")
        return 1
    both = list_jobs(tid0, labels=["wave:canary", "kind:long"])
    if len(both) != JOBS_PER_TENANT:
        logger.error("AND label filter failed: %d", len(both))
        return 1

    # Bulk cancel wave:canary per tenant (includes leased).
    # Small batch_size stresses multi-batch drain (must cancel ALL, not one page).
    cancelled_total = 0
    for i in range(TENANTS):
        tid = f"lbl-{i:02d}"
        res = http_json(
            "POST",
            "/v1/jobs/cancel-matching",
            {
                "tenant_id": tid,
                "labels": ["wave:canary"],
                "batch_size": CANCEL_BATCH,
                # No max_jobs — must fully drain even when > batch_size.
            },
        )
        n = int(res.get("cancelled") or 0)
        complete = bool(res.get("complete", False))
        batches = int(res.get("batches") or 0)
        remaining = int(res.get("remaining_estimate") or 0)
        cancelled_total += n
        logger.info(
            "cancel-matching tenant=%s cancelled=%d complete=%s batches=%d remaining=%d",
            tid,
            n,
            complete,
            batches,
            remaining,
        )
        if n != JOBS_PER_TENANT or not complete or remaining != 0:
            logger.error(
                "expected full drain for %s: cancelled=%d want=%d complete=%s remaining=%d",
                tid,
                n,
                JOBS_PER_TENANT,
                complete,
                remaining,
            )
            return 1
        if JOBS_PER_TENANT > CANCEL_BATCH and batches < 2:
            logger.error(
                "expected multi-batch cancel for %s (jobs=%d batch=%d batches=%d)",
                tid,
                JOBS_PER_TENANT,
                CANCEL_BATCH,
                batches,
            )
            return 1

    # Verify cancelled state + keep survivors.
    for i in range(TENANTS):
        tid = f"lbl-{i:02d}"
        cancelled = list_jobs(tid, labels=["wave:canary"], status="cancelled")
        if len(cancelled) != JOBS_PER_TENANT:
            logger.error(
                "post-cancel list cancelled=%d tenant=%s",
                len(cancelled),
                tid,
            )
            return 1
        keep = list_jobs(tid, labels=["wave:keep"])
        if len(keep) != 2:
            logger.error("keep jobs missing tenant=%s: %s", tid, keep)
            return 1
        if any(j.get("status") == "cancelled" for j in keep):
            logger.error("keep jobs wrongly cancelled: %s", keep)
            return 1

    # Wait for keep jobs to complete (short).
    deadline = time.monotonic() + MAX_WALL
    while time.monotonic() < deadline:
        b = backlog()
        logger.info("post-cancel backlog=%s", b)
        keep_done = 0
        keep_total = TENANTS * 2
        for i in range(TENANTS):
            tid = f"lbl-{i:02d}"
            for j in list_jobs(tid, labels=["wave:keep"]):
                if j.get("status") == "completed":
                    keep_done += 1
        if keep_done >= keep_total and int(b.get("leased") or 0) == 0:
            break
        time.sleep(0.5)

    b = backlog()
    if int(b.get("leased") or 0) != 0:
        logger.error("leases remain after cancel: %s", b)
        return 1
    if cancelled_total != TENANTS * JOBS_PER_TENANT:
        logger.error("cancelled_total=%d", cancelled_total)
        return 1

    logger.info(
        "LABELS SOAK PASSED cancelled=%d keep_tenants=%d backlog=%s",
        cancelled_total,
        TENANTS,
        b,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
