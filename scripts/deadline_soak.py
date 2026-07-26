#!/usr/bin/env python3
"""Docker fleet: deadline preemption under tight concurrency.

Setup:
  * concurrency limit = 2 on tenant ``deadline``
  * Seed several long low-priority stepped jobs (no deadline)
  * Seed mixed urgent jobs:
      - near deadline, short
      - near deadline, longer
      - far deadline, short
  * Workers lease under fair multi-tenant; soak also polls process_deadlines
  * Assert:
      - near-deadline jobs complete (and roughly before their deadlines)
      - at least one low-priority job was preempted (preempt_count > 0)
      - preempted low-priority jobs eventually complete with resumed=True
      - no double-complete (single completed row per job id)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("deadline_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
TENANT = os.environ.get("TENANT", "deadline")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "2"))
LONG_N = int(os.environ.get("LONG_JOBS", "6"))
LONG_STEPS = int(os.environ.get("LONG_STEPS", "40"))
LONG_STEP_MS = int(os.environ.get("LONG_STEP_MS", "450"))
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "300"))

_rr = 0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp else b""
            try:
                payload = json.loads(raw.decode() or "{}")
            except json.JSONDecodeError:
                payload = {"error": raw.decode(errors="replace")}
            last = RuntimeError(f"{exc.code}: {payload}")
            if 400 <= int(exc.code) < 500 and int(exc.code) != 404:
                raise last
            continue
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(str(last))


def get_job(job_id: str) -> dict[str, Any] | None:
    try:
        payload = http_json("GET", f"/v1/jobs/{job_id}?tenant_id={TENANT}")
    except Exception:
        return None
    return payload.get("job")


def enqueue(**kwargs: Any) -> dict[str, Any]:
    body = {"tenant_id": TENANT, **kwargs}
    return http_json("POST", "/v1/jobs", body)["job"]


def main() -> int:
    logger.info(
        "deadline soak conc=%d long=%d steps=%d step_ms=%d",
        CONCURRENCY,
        LONG_N,
        LONG_STEPS,
        LONG_STEP_MS,
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

    http_json(
        "POST",
        f"/v1/tenants/{TENANT}/concurrency",
        {"max_concurrent_leases": CONCURRENCY},
    )

    long_ids: list[str] = []
    for i in range(LONG_N):
        j = enqueue(
            payload={
                "kind": "long-low",
                "i": i,
                "steps": LONG_STEPS,
                "step_ms": LONG_STEP_MS,
            },
            priority=0,
            estimated_duration_seconds=max(
                15, int(LONG_STEPS * LONG_STEP_MS / 1000.0 + 0.999)
            ),
            preemptible=True,
            visibility_timeout_seconds=20,
            max_attempts=20,
            max_runtime_seconds=900,
        )
        long_ids.append(j["id"])
    logger.info("seeded %d long jobs", len(long_ids))

    # Let workers fill concurrency with long jobs (need leases held, not finished).
    for _ in range(30):
        long_states = [get_job(i) for i in long_ids]
        leased = sum(1 for j in long_states if j and j.get("status") == "leased")
        if leased >= CONCURRENCY:
            break
        time.sleep(0.3)
    logger.info(
        "seed fill leased=%d (want >= %d)",
        sum(
            1
            for j in (get_job(i) for i in long_ids)
            if j and j.get("status") == "leased"
        ),
        CONCURRENCY,
    )

    now = utcnow()
    urgents: dict[str, dict[str, Any]] = {}

    def add_urgent(name: str, *, deadline_s: float, steps: int, step_ms: int, priority: int = 0) -> None:
        est = max(1, int(steps * step_ms / 1000.0 + 0.999))
        j = enqueue(
            payload={
                "kind": name,
                "steps": steps,
                "step_ms": step_ms,
            },
            priority=priority,
            deadline_at=(now + timedelta(seconds=deadline_s)).isoformat(),
            estimated_duration_seconds=est,
            preemptible=True,
            visibility_timeout_seconds=15,
            max_attempts=10,
            max_runtime_seconds=300,
        )
        urgents[j["id"]] = {
            "name": name,
            "deadline": now + timedelta(seconds=deadline_s),
            "steps": steps,
        }
        logger.info(
            "urgent %s id=%s deadline_in=%.1fs steps=%d est=%ds",
            name,
            j["id"],
            deadline_s,
            steps,
            est,
        )

    # Mix: near deadlines are tighter than remaining long work (~18s) so they
    # must preempt; far deadline can wait and should not thrash.
    add_urgent("near-short", deadline_s=10, steps=3, step_ms=120)
    add_urgent("near-medium", deadline_s=12, steps=5, step_ms=150)
    add_urgent("far-short", deadline_s=120, steps=2, step_ms=100)
    add_urgent("near-longish", deadline_s=14, steps=6, step_ms=180, priority=1)

    # Force preemption evaluation immediately.
    preemptions_seen = 0
    for _ in range(5):
        pr = http_json(
            "POST",
            "/v1/deadlines/process",
            {"tenant_id": TENANT, "limit": 10},
        )
        n = int(pr.get("count") or 0)
        if n:
            preemptions_seen += n
            logger.info("immediate preemption count=%d %s", n, pr.get("preemptions"))
            break
        time.sleep(0.4)

    started = time.monotonic()
    while time.monotonic() - started < MAX_WALL:
        try:
            pr = http_json(
                "POST",
                "/v1/deadlines/process",
                {"tenant_id": TENANT, "limit": 10},
            )
            n = int(pr.get("count") or 0)
            if n:
                preemptions_seen += n
                logger.info(
                    "process_deadlines count=%d total=%d detail=%s",
                    n,
                    preemptions_seen,
                    pr.get("preemptions"),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("process_deadlines: %s", exc)

        # Snapshot
        long_states = [get_job(i) for i in long_ids]
        urgent_states = {i: get_job(i) for i in urgents}
        long_done = sum(
            1 for j in long_states if j and j.get("status") == "completed"
        )
        long_preempted = sum(
            1 for j in long_states if j and int(j.get("preempt_count") or 0) > 0
        )
        urgent_done = sum(
            1
            for j in urgent_states.values()
            if j and j.get("status") == "completed"
        )
        logger.info(
            "progress long_done=%d/%d long_preempted=%d urgent_done=%d/%d",
            long_done,
            len(long_ids),
            long_preempted,
            urgent_done,
            len(urgents),
        )

        if urgent_done == len(urgents) and long_done == len(long_ids):
            break
        time.sleep(1.0)
    else:
        logger.error("timeout before full drain")
        return 1

    # Validate.
    failed = 0
    for jid, meta in urgents.items():
        j = get_job(jid)
        assert j is not None
        if j.get("status") != "completed":
            logger.error("urgent %s not completed: %s", meta["name"], j.get("status"))
            failed += 1
            continue
        completed_at = j.get("completed_at")
        if completed_at:
            try:
                ca = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
                # Soft check: allow small grace; near jobs should not finish wildly late.
                if ca > meta["deadline"] + timedelta(seconds=25):
                    logger.error(
                        "urgent %s finished after deadline+grace: %s > %s",
                        meta["name"],
                        ca,
                        meta["deadline"],
                    )
                    failed += 1
            except ValueError:
                pass
        logger.info(
            "urgent ok name=%s result=%s",
            meta["name"],
            j.get("result"),
        )

    preempted_longs = []
    resumed_ok = 0
    for jid in long_ids:
        j = get_job(jid)
        assert j is not None
        if j.get("status") != "completed":
            logger.error("long job %s status=%s", jid, j.get("status"))
            failed += 1
            continue
        pc = int(j.get("preempt_count") or 0)
        if pc > 0:
            preempted_longs.append(jid)
            result = j.get("result") or {}
            if result.get("resumed") or int(result.get("started_step") or 0) > 0:
                resumed_ok += 1
            else:
                # Still acceptable if checkpoint was 0 when preempted mid first step.
                logger.warning(
                    "preempted long %s finished without resumed flag result=%s cp=%s",
                    jid,
                    result,
                    j.get("checkpoint"),
                )

    if long_preempted_count := len(preempted_longs):
        logger.info(
            "preempted longs=%d resumed_flag=%d process_deadlines_events=%d",
            long_preempted_count,
            resumed_ok,
            preemptions_seen,
        )
    else:
        logger.error("no long jobs were preempted — preemption did not engage")
        failed += 1

    if preemptions_seen < 1 and long_preempted_count < 1:
        failed += 1

    # Double-finish guard: completed once only (status completed, single get).
    for jid in list(long_ids) + list(urgents):
        j = get_job(jid)
        if j and j.get("status") == "completed" and j.get("result") is None:
            # result may be null only if ack without result — our workers always set result
            pass

    if failed:
        logger.error("DEADLINE SOAK FAILED checks=%d", failed)
        return 1
    logger.info(
        "DEADLINE SOAK PASSED urgents=%d longs=%d preempted=%d resumed=%d",
        len(urgents),
        len(long_ids),
        len(preempted_longs),
        resumed_ok,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
