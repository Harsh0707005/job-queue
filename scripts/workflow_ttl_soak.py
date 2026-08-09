#!/usr/bin/env python3
"""Fleet soak: workflow step jobs inherit TTL and cannot sit forever.

  * Tenants with a default TTL start sagas whose first step needs a skill
    no worker has. After TTL the step job expires and the run fails.
  * Per-workflow cancel TTL is honored independently.
  * A later-step skill miss still compensates completed work.
  * Happy-path workflows on a live tenant still complete.
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
logger = logging.getLogger("workflow_ttl_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
TTL_SECONDS = int(os.environ.get("TTL_SECONDS", "5") or "5")
DEAD_RUNS = int(os.environ.get("DEAD_RUNS", "6") or "6")
CANCEL_RUNS = int(os.environ.get("CANCEL_RUNS", "4") or "4")
COMP_RUNS = int(os.environ.get("COMP_RUNS", "3") or "3")
HAPPY_RUNS = int(os.environ.get("HAPPY_RUNS", "4") or "4")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
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
                return json.loads(raw.decode()) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            if 500 <= int(exc.code) < 600:
                last = RuntimeError(f"{base}{path} -> {exc.code}: {payload}")
                continue
            raise RuntimeError(f"{base}{path} -> {exc.code}: {payload}") from exc
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            if http_json("GET", "/health").get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def start_wf(tenant: str, name: str, steps: list[dict], **extra: Any) -> str:
    body = {"tenant_id": tenant, "name": name, "max_attempts": 1, "steps": steps}
    body.update(extra)
    out = http_json("POST", "/v1/workflows", body)
    rid = str((out.get("run") or {}).get("id") or "")
    if not rid:
        raise RuntimeError(f"start workflow failed {out}")
    return rid


def get_run(tid: str, rid: str) -> dict[str, Any]:
    return http_json("GET", f"/v1/workflows/{rid}?tenant_id={tid}").get("run") or {}


def list_jobs(tid: str) -> list[dict[str, Any]]:
    return list(
        http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500").get("jobs") or []
    )


def orphan_step() -> dict[str, Any]:
    return {
        "name": "orphan",
        "payload": {"op": "need-missing"},
        "required_skills": ["no-such-skill"],
        "compensate": {"payload": {"op": "undo-orphan"}},
    }


def main() -> int:
    logger.info(
        "workflow ttl soak dead=%d cancel=%d comp=%d happy=%d ttl=%ds",
        DEAD_RUNS,
        CANCEL_RUNS,
        COMP_RUNS,
        HAPPY_RUNS,
        TTL_SECONDS,
    )
    wait_coords()

    http_json(
        "POST",
        "/v1/tenants/wf-ttl-dead/job-ttl",
        {"ttl_seconds": TTL_SECONDS, "expiry_action": "dead"},
    )

    dead_ids = [
        start_wf("wf-ttl-dead", f"dead-{i}", [orphan_step()])
        for i in range(DEAD_RUNS)
    ]
    cancel_ids = [
        start_wf(
            "wf-ttl-cancel",
            f"can-{i}",
            [orphan_step()],
            ttl_seconds=TTL_SECONDS,
            expiry_action="cancel",
        )
        for i in range(CANCEL_RUNS)
    ]
    comp_ids = [
        start_wf(
            "wf-ttl-comp",
            f"comp-{i}",
            [
                {
                    "name": "ok",
                    "payload": {"op": "ok", "i": i},
                    "compensate": {"payload": {"op": "undo_ok", "i": i}},
                },
                {
                    "name": "stuck",
                    "payload": {"op": "stuck", "i": i},
                    "required_skills": ["no-such-skill"],
                    "ttl_seconds": TTL_SECONDS,
                    "expiry_action": "dead",
                    "compensate": {"payload": {"op": "undo_stuck"}},
                },
            ],
        )
        for i in range(COMP_RUNS)
    ]
    happy_ids = [
        start_wf(
            "wf-ttl-ok",
            f"ok-{i}",
            [
                {
                    "name": "a",
                    "payload": {"op": "a", "i": i},
                    "compensate": {"payload": {"op": "ua"}},
                },
                {
                    "name": "b",
                    "payload": {"op": "b", "i": i},
                },
            ],
        )
        for i in range(HAPPY_RUNS)
    ]
    logger.info("started workflows")

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        http_json("POST", "/v1/jobs/process-expired", {"limit": 200})
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 50})
        dead_jobs = list_jobs("wf-ttl-dead")
        can_jobs = list_jobs("wf-ttl-cancel")
        dead_term = sum(1 for j in dead_jobs if j.get("status") == "dead")
        can_term = sum(1 for j in can_jobs if j.get("status") == "cancelled")
        dead_failed = sum(
            1
            for rid in dead_ids
            if get_run("wf-ttl-dead", rid).get("status") == "failed"
        )
        can_failed = sum(
            1
            for rid in cancel_ids
            if get_run("wf-ttl-cancel", rid).get("status") == "failed"
        )
        comp_failed = sum(
            1
            for rid in comp_ids
            if get_run("wf-ttl-comp", rid).get("status") == "failed"
        )
        happy_done = sum(
            1
            for rid in happy_ids
            if get_run("wf-ttl-ok", rid).get("status") == "completed"
        )
        leftover = [
            j
            for rows in (dead_jobs, can_jobs)
            for j in rows
            if j.get("status") in ("pending", "leased", "awaiting_skills")
        ]
        now = time.time()
        if now - last >= 2:
            logger.info(
                "dead_jobs=%d/%d cancel_jobs=%d/%d runs dead=%d cancel=%d "
                "comp=%d happy=%d leftover=%d",
                dead_term,
                DEAD_RUNS,
                can_term,
                CANCEL_RUNS,
                dead_failed,
                can_failed,
                comp_failed,
                happy_done,
                len(leftover),
            )
            last = now
        if (
            dead_term >= DEAD_RUNS
            and can_term >= CANCEL_RUNS
            and dead_failed == DEAD_RUNS
            and can_failed == CANCEL_RUNS
            and comp_failed == COMP_RUNS
            and happy_done == HAPPY_RUNS
            and not leftover
        ):
            logger.info("workflow ttl soak OK")
            return 0
        time.sleep(0.5)
    raise RuntimeError("workflow ttl soak timed out")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("workflow ttl soak failed")
        sys.exit(1)
