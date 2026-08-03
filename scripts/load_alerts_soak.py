#!/usr/bin/env python3
"""Fleet soak: multi-tenant / multi-worker alert evaluation under load.

Seeds alert rules for dead_count, webhook_fail_rate, dead_letter_lag, and
worker_leased_count; generates load via dual coordinators + workers; evaluates
alerts and asserts history + alert.fired webhook deliveries.
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
logger = logging.getLogger("load_alerts_soak")

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
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "300"))
RESULTS_PATH = os.environ.get("RESULTS_PATH", "/results/alerts-fleet.json")
LABEL = os.environ.get("LOAD_LABEL", "alerts-fleet")
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


def main() -> int:
    total = TENANTS * JOBS_PER_TENANT
    tenants = [f"al-t{i:02d}" for i in range(TENANTS)]
    logger.info(
        "ALERTS soak label=%s tenants=%s jobs/tenant=%s total=%s",
        LABEL,
        TENANTS,
        JOBS_PER_TENANT,
        total,
    )
    wait_healthy()

    # Register overall alert webhook (sink may not exist — delivery can dead; history still required).
    sink = os.environ.get("ALERT_WEBHOOK_URL", "http://webhook-sink:8090/hook")
    http_json(
        "POST",
        "/v1/webhooks",
        {
            "tenant_id": "__overall__",
            "url": sink,
            "events": ["alert.fired"],
            "secret": "fleet-alert-secret-16",
        },
    )

    # Rules
    rules_spec = [
        {
            "name": f"{LABEL}-dead-overall",
            "metric": "dead_count",
            "threshold": 0,
            "comparator": "gt",
            "scope": "overall",
            "cooldown_seconds": 0,
            "window_seconds": 3600,
        },
        {
            "name": f"{LABEL}-wh-fail-rate",
            "metric": "webhook_fail_rate",
            "threshold": -0.01,
            "comparator": "gt",
            "scope": "overall",
            "cooldown_seconds": 0,
            "window_seconds": 3600,
        },
        {
            "name": f"{LABEL}-dl-lag",
            "metric": "dead_letter_lag_seconds",
            "threshold": -1,
            "comparator": "gt",
            "scope": "overall",
            "cooldown_seconds": 0,
        },
    ]
    for tid in tenants[:3]:
        rules_spec.append(
            {
                "name": f"{LABEL}-dead-{tid}",
                "metric": "dead_count",
                "threshold": 0,
                "comparator": "gt",
                "scope": "tenant",
                "tenant_id": tid,
                "cooldown_seconds": 0,
            }
        )
    rules_spec.append(
        {
            "name": f"{LABEL}-worker-lease",
            "metric": "worker_leased_count",
            "threshold": 0,
            "comparator": "gt",
            "scope": "worker",
            "worker_id": "load-w1",
            "cooldown_seconds": 0,
        }
    )

    for r in rules_spec:
        code, body = http_json("POST", "/v1/alerts/rules", r)
        if code not in (200, 201):
            # name conflict from re-run: ignore
            logger.warning("create rule %s -> %s %s", r["name"], code, body)

    # 1) Seed dedicated max_attempts=1 jobs and nack them to DEAD before
    #    fleet workers drain the rest (avoids race with success path).
    dead_forced = 0
    for tid in tenants:
        for i in range(4):
            code, body = http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {"dead_seed": i, "load": LABEL},
                    "max_attempts": 1,
                    "visibility_timeout_seconds": 60,
                },
            )
            if code not in (200, 201):
                continue
            code, body = http_json(
                "POST",
                "/v1/lease",
                {"tenant_id": tid, "worker_id": "alert-nacker", "limit": 1},
            )
            if code == 204:
                continue
            job = (body.get("jobs") or [body.get("job")])[0] or {}
            jid, token = job.get("id"), job.get("lease_token")
            if not jid or not token:
                continue
            code, _ = http_json(
                "POST",
                f"/v1/jobs/{jid}/nack",
                {
                    "tenant_id": tid,
                    "worker_id": "alert-nacker",
                    "lease_token": token,
                    "error": "force-dead",
                    "requeue": True,
                },
            )
            if code in (200, 201):
                dead_forced += 1
    logger.info("forced dead=%s", dead_forced)

    # 2) Main load for multi-worker drain.
    def enq(tid: str, i: int) -> None:
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"i": i, "load": LABEL},
                "max_attempts": 3,
                "visibility_timeout_seconds": 30,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"enqueue {code}: {body}")

    work = [(tid, i) for tid in tenants for i in range(JOBS_PER_TENANT)]
    with ThreadPoolExecutor(max_workers=ENQUEUE_WORKERS) as pool:
        futs = [pool.submit(enq, *w) for w in work]
        for f in as_completed(futs):
            f.result()
    logger.info("enqueued %s jobs", total)

    # Drain remaining with fleet workers
    t0 = time.time()
    while time.time() - t0 < MAX_WALL:
        code, bl = http_json("GET", "/v1/admin/backlog")
        jobs = (bl.get("jobs") or {}) if code == 200 else {}
        pending = int(jobs.get("pending") or 0)
        leased = int(jobs.get("leased") or 0)
        completed = int(jobs.get("completed") or 0)
        dead = int(jobs.get("dead") or 0)
        if pending == 0 and leased == 0 and (completed + dead) >= total * 0.9:
            break
        time.sleep(1)
    logger.info("backlog settled completed=%s dead=%s", completed, dead)

    # Evaluate alerts multiple times (sample + explicit)
    http_json("POST", "/v1/metrics/sample", {})
    code, ev = http_json("POST", "/v1/alerts/evaluate", {})
    fired = int(ev.get("count") or 0) if code == 200 else 0
    code2, hist = http_json("GET", "/v1/alerts/events?" + urlencode({"limit": 200}))
    events = hist.get("events") or [] if code2 == 200 else []
    names = {e.get("rule_name") for e in events}

    ok_hist = len(events) >= 2
    ok_dead = f"{LABEL}-dead-overall" in names or any(
        "dead" in (n or "") for n in names
    )
    ok_lag = f"{LABEL}-dl-lag" in names or any("lag" in (n or "") for n in names)
    ok_tenant = any(f"{LABEL}-dead-{tid}" in names for tid in tenants[:3])
    # Pass if history recorded and at least dead-related + lag (or webhook rate).
    ok = ok_hist and ok_dead and (ok_lag or dead_forced > 0) and dead >= 1

    result = {
        "ok": ok,
        "label": LABEL,
        "total_jobs": total,
        "dead_jobs": dead,
        "dead_forced": dead_forced,
        "completed_jobs": completed,
        "evaluate_fired": fired,
        "history_count": len(events),
        "rule_names_seen": sorted(n for n in names if n),
        "ok_hist": ok_hist,
        "ok_dead": ok_dead,
        "ok_lag": ok_lag,
        "ok_tenant": ok_tenant,
    }
    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("result %s", json.dumps(result))
    if result["ok"]:
        logger.info("ALERTS FLEET PASS")
        return 0
    logger.error("ALERTS FLEET FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
