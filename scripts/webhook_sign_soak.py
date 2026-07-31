#!/usr/bin/env python3
"""Fleet: signed webhooks + secret rotation mid-flight.

  1. Register endpoints with secret-v1 (multi-tenant).
  2. Complete jobs so deliveries enqueue (signing_secret snapshotted).
  3. Rotate secrets to v2 while deliveries drain.
  4. Assert sink verifies posts (accepts v1 and v2 for rotation window)
     and backlog clears without stuck deliveries.
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
logger = logging.getLogger("webhook_sign_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
SINK = (os.environ.get("WEBHOOK_SINK_URL") or "http://webhook-sink:8090").rstrip("/")
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
JOBS = int(os.environ.get("JOBS_PER_TENANT", "20") or "20")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "120") or "120")
SECRET_V1 = os.environ.get("SECRET_V1", "fleet-secret-v1")
SECRET_V2 = os.environ.get("SECRET_V2", "fleet-secret-v2")
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


def backlog() -> dict[str, int]:
    try:
        bl = http_json("GET", "/v1/admin/backlog")
        wh = http_json("GET", "/v1/admin/webhook-stats")
    except Exception as exc:  # noqa: BLE001
        logger.warning("backlog: %s", exc)
        return {}
    j = bl.get("jobs") or {}
    w = wh.get("by_status") or {}
    return {
        **{k: int(j.get(k) or 0) for k in j},
        "wh_pending": int(w.get("pending") or 0),
        "wh_delivering": int(w.get("delivering") or 0),
        "wh_succeeded": int(w.get("succeeded") or 0),
        "wh_dead": int(w.get("dead") or 0),
    }


def sink_stats() -> dict[str, Any]:
    req = urllib.request.Request(SINK + "/stats", method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def main() -> int:
    logger.info(
        "webhook sign soak tenants=%d jobs=%d secret rotate mid-flight",
        TENANTS,
        JOBS,
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

    endpoint_ids: list[tuple[str, str]] = []
    for i in range(TENANTS):
        tid = f"wsig-{i:02d}"
        resp = http_json(
            "POST",
            "/v1/webhooks",
            {
                "tenant_id": tid,
                "url": SINK + "/hook",
                "secret": SECRET_V1,
                "events": ["job.completed"],
                "max_attempts": 8,
            },
        )
        ep = resp.get("endpoint") or {}
        endpoint_ids.append((tid, ep["id"]))
        logger.info("registered webhook %s tenant=%s", ep["id"], tid)

    # Enqueue + complete work (workers drain).
    for i in range(TENANTS):
        tid = f"wsig-{i:02d}"
        for j in range(JOBS):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"kind": "work", "ms": 50, "i": j},
                    "max_attempts": 3,
                },
            )

    # Wait until most jobs completed (webhook outbox filling).
    t0 = time.monotonic()
    while time.monotonic() - t0 < 45:
        b = backlog()
        logger.info("pre-rotate backlog=%s", b)
        if int(b.get("completed") or 0) >= TENANTS * JOBS * 0.5:
            break
        time.sleep(0.5)

    # Rotate all endpoint secrets while deliveries still drain.
    for tid, eid in endpoint_ids:
        http_json(
            "POST",
            f"/v1/webhooks/{eid}/rotate-secret",
            {"tenant_id": tid, "secret": SECRET_V2},
        )
    logger.info("rotated %d endpoint secrets to v2", len(endpoint_ids))

    # Wait for full drain.
    deadline = time.monotonic() + MAX_WALL
    while time.monotonic() < deadline:
        b = backlog()
        logger.info("post-rotate backlog=%s", b)
        if (
            int(b.get("pending") or 0) == 0
            and int(b.get("leased") or 0) == 0
            and int(b.get("wh_pending") or 0) == 0
            and int(b.get("wh_delivering") or 0) == 0
            and int(b.get("completed") or 0) >= TENANTS * JOBS * 0.9
        ):
            break
        time.sleep(0.8)

    b = backlog()
    stats = sink_stats()
    logger.info("final backlog=%s sink=%s", b, stats)

    if int(b.get("wh_pending") or 0) + int(b.get("wh_delivering") or 0) > 0:
        logger.error("webhook backlog not drained: %s", b)
        return 1
    if int(stats.get("signed_posts") or 0) < 1:
        logger.error("sink saw no signed posts: %s", stats)
        return 1
    # When sink verifies both secrets, all signed posts should verify.
    if stats.get("sig_ok") is not None and stats.get("sig_bad") is not None:
        if int(stats["sig_bad"]) > 0:
            logger.error("signature verification failures: %s", stats)
            return 1
        if int(stats["sig_ok"]) < int(stats.get("signed_posts") or 0) * 0.9:
            logger.error("too few sig_ok: %s", stats)
            return 1

    logger.info(
        "WEBHOOK SIGN SOAK PASSED signed=%s ok=%s completed_jobs=%s",
        stats.get("signed_posts"),
        stats.get("sig_ok"),
        b.get("completed"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
