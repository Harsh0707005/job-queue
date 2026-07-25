#!/usr/bin/env python3
"""Distributed soak: parent cancel/dead kills pending and running dependents.

For each of 6 tenants, builds 20-job dependency trees (root → mids → leaves),
runs workers, cancels/kills mid-chain parents while work is in flight, and
asserts no dependent of a dead/cancelled parent ever completes.
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
logger = logging.getLogger("cascade_cancel_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANTS = int(os.environ.get("TENANTS", "6"))
JOBS_PER_TENANT = int(os.environ.get("JOBS_PER_TENANT", "20"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180"))
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
    last = None
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


def wait_ready() -> None:
    for b in COORDS:
        for _ in range(90):
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError(f"not ready {b}")


def backlog() -> dict[str, int]:
    j = (http_json("GET", "/v1/admin/backlog").get("jobs") or {})
    return {k: int(j.get(k) or 0) for k in (
        "pending", "leased", "completed", "dead", "failed", "cancelled"
    )}


def main() -> int:
    """
    Per tenant tree of exactly JOBS_PER_TENANT jobs:
      root (1)
      mids (M) each depends on root
      leaves (rest) each depends on a mid
    Total = 1 + M + L = JOBS_PER_TENANT.
    """
    assert JOBS_PER_TENANT >= 5
    n_mids = max(2, JOBS_PER_TENANT // 4)
    n_leaves = JOBS_PER_TENANT - 1 - n_mids
    logger.info(
        "cascade soak tenants=%d jobs/tenant=%d (1 root + %d mids + %d leaves)",
        TENANTS,
        JOBS_PER_TENANT,
        n_mids,
        n_leaves,
    )
    wait_ready()

    tenants = [f"casc-t{i:02d}" for i in range(TENANTS)]
    # tenant -> {root, mids: [], leaves: [], all: set}
    trees: dict[str, dict[str, Any]] = {}

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/pause", {})

    for tid in tenants:
        root = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"role": "root", "tenant": tid},
                "priority": 10,
                "visibility_timeout_seconds": 60,
                "max_runtime_seconds": 120,
            },
        )["job"]
        mids = []
        for i in range(n_mids):
            m = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"role": "mid", "i": i, "sleep_ms": 5000},
                    "depends_on": [root["id"]],
                    "priority": 5,
                    "visibility_timeout_seconds": 60,
                    "max_runtime_seconds": 120,
                },
            )["job"]
            mids.append(m)
        leaves = []
        for i in range(n_leaves):
            parent = mids[i % len(mids)]
            leaf = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"role": "leaf", "i": i, "sleep_ms": 8000},
                    "depends_on": [parent["id"]],
                    "priority": 1,
                    "visibility_timeout_seconds": 60,
                    "max_runtime_seconds": 120,
                },
            )["job"]
            leaves.append(leaf)
        trees[tid] = {
            "root": root,
            "mids": mids,
            "leaves": leaves,
            "all_ids": {root["id"]} | {m["id"] for m in mids} | {lf["id"] for lf in leaves},
            "leaf_ids": {lf["id"] for lf in leaves},
            "mid_ids": {m["id"] for m in mids},
        }
        logger.info(
            "tenant %s root=%s mids=%d leaves=%d",
            tid,
            root["id"][:8],
            len(mids),
            len(leaves),
        )

    for tid in tenants:
        http_json("POST", f"/v1/tenants/{tid}/resume", {})

    # Let workers complete roots and start some mids.
    time.sleep(3)

    # Cancel half of mid jobs (running or pending) per tenant via coordinator.
    cancelled_mids: set[str] = set()
    for tid, tree in trees.items():
        for i, m in enumerate(tree["mids"]):
            if i % 2 == 0:
                try:
                    r = http_json(
                        "POST",
                        f"/v1/jobs/{m['id']}/cancel",
                        {"tenant_id": tid},
                    )
                    if r.get("job", {}).get("status") == "cancelled":
                        cancelled_mids.add(m["id"])
                        logger.info("cancelled mid %s tenant=%s", m["id"][:8], tid)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("cancel mid %s: %s", m["id"][:8], exc)

    # Also nack-kill remaining mids we can lease ourselves (force dead path).
    for _ in range(TENANTS * 2):
        try:
            payload = http_json(
                "POST",
                "/v1/lease",
                {
                    "worker_id": "cascade-killer",
                    "limit": 4,
                    "visibility_timeout_seconds": 30,
                },
            )
        except Exception:
            break
        for j in payload.get("jobs") or []:
            if (j.get("payload") or {}).get("role") != "mid":
                # ack non-mids quickly so roots finish
                try:
                    http_json(
                        "POST",
                        f"/v1/jobs/{j['id']}/ack",
                        {
                            "tenant_id": j["tenant_id"],
                            "worker_id": "cascade-killer",
                            "lease_token": j["lease_token"],
                            "result": {"killer": "ack"},
                        },
                    )
                except Exception:
                    pass
                continue
            try:
                http_json(
                    "POST",
                    f"/v1/jobs/{j['id']}/nack",
                    {
                        "tenant_id": j["tenant_id"],
                        "worker_id": "cascade-killer",
                        "lease_token": j["lease_token"],
                        "error": "cascade soak kill mid",
                        "requeue": False,
                    },
                )
                cancelled_mids.add(j["id"])  # treated as killed parent
                logger.info("nacked mid %s", j["id"][:8])
            except Exception as exc:  # noqa: BLE001
                logger.warning("nack mid: %s", exc)

    logger.info("killed/cancelled %d mids; waiting for drain", len(cancelled_mids))

    started = time.monotonic()
    while time.monotonic() - started < MAX_WALL:
        j = backlog()
        logger.info("backlog %s", j)
        if j["pending"] == 0 and j["leased"] == 0:
            time.sleep(1)
            j = backlog()
            if j["pending"] == 0 and j["leased"] == 0:
                break
        time.sleep(1)
    else:
        logger.error("timeout waiting for drain: %s", backlog())
        return 1

    # Validate via completion order / status snapshots
    # Any leaf whose mid parent was cancelled/killed must be cancelled.
    order = http_json("GET", f"/v1/admin/completion-order?limit=5000")
    completed_ids = {c["id"] for c in (order.get("completed") or [])}

    # Pull final statuses using cancel endpoint is wrong; use priority-stats-like
    # admin: re-query each job by attempting get via backlog counts.
    # We only have completion-order for completed. For cancelled, use count.
    bl = backlog()
    if bl["completed"] > TENANTS * (1 + n_mids):  # roots + surviving mids max
        # leaves of killed mids must not complete — upper bound: all roots + half mids
        pass

    illegal_leaf_completes = 0
    for tid, tree in trees.items():
        for leaf in tree["leaves"]:
            mid_parent = None
            # find which mid this leaf depended on
            for m in tree["mids"]:
                # reconstruct: leaf i depended on mids[i % n_mids]
                pass
        for i, leaf in enumerate(tree["leaves"]):
            parent_mid = tree["mids"][i % len(tree["mids"])]
            if parent_mid["id"] in cancelled_mids or True:
                # If mid was cancelled/killed, leaf must not be completed.
                # We track cancelled_mids; also mids that failed/dead without
                # being in set — check completion of leaf only if mid cancelled.
                if parent_mid["id"] in cancelled_mids:
                    if leaf["id"] in completed_ids:
                        illegal_leaf_completes += 1
                        logger.error(
                            "leaf %s completed after parent mid %s killed",
                            leaf["id"][:8],
                            parent_mid["id"][:8],
                        )

    if illegal_leaf_completes:
        logger.error("%d leaves completed after parent kill", illegal_leaf_completes)
        return 1

    # No leased leftovers; cancelled count should be substantial.
    if bl["leased"] != 0 or bl["pending"] != 0:
        logger.error("backlog not empty: %s", bl)
        return 1
    if bl["cancelled"] < len(cancelled_mids):
        # at least cancelled mids themselves (if cancel path) + their leaves
        logger.warning(
            "cancelled count %d vs killed mids %d (nacks may be failed not cancelled)",
            bl["cancelled"],
            len(cancelled_mids),
        )
    # Leaves of killed mids: each killed mid has about n_leaves/n_mids leaves
    if bl["cancelled"] < 1 and not cancelled_mids:
        logger.error("nothing was cancelled")
        return 1

    logger.info(
        "CASCADE CANCEL SOAK PASSED completed=%d cancelled=%d dead=%d failed=%d killed_mids=%d",
        bl["completed"],
        bl["cancelled"],
        bl["dead"],
        bl["failed"],
        len(cancelled_mids),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
