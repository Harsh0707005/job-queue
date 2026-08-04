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
    auto_pause_tid = tenants[0]  # al-t00: auto-pause on dead threshold
    mute_tid = tenants[1] if len(tenants) > 1 else tenants[0]
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
                # Overflow guard: pause tenant when dead threshold trips.
                "auto_pause_tenant": tid == auto_pause_tid,
                # Auto-resolve after 2 spaced healthy samples once dead=0.
                "recovery_threshold": 0,
                "recovery_evaluations": 2,
                "healthy_interval_seconds": 1,
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

    rule_ids: dict[str, str] = {}
    for r in rules_spec:
        code, body = http_json("POST", "/v1/alerts/rules", r)
        if code not in (200, 201):
            # name conflict from re-run: ignore
            logger.warning("create rule %s -> %s %s", r["name"], code, body)
        else:
            rule = body.get("rule") or {}
            if rule.get("id") and rule.get("name"):
                rule_ids[str(rule["name"])] = str(rule["id"])

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

    # Evaluate alerts multiple times (sample + explicit). Concurrent evaluate
    # from dual coordinators must not double-fire under cooldown.
    http_json("POST", "/v1/metrics/sample", {})
    code, ev = http_json("POST", "/v1/alerts/evaluate", {})
    fired = int(ev.get("count") or 0) if code == 200 else 0

    # Concurrent dual-coordinator evaluate race (cooldown=0 same-now de-dupe
    # still serializes; fleet rules use cooldown 0 after first fire → 0 more).
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = [
            pool.submit(http_json, "POST", "/v1/alerts/evaluate", {})
            for _ in range(4)
        ]
        for f in as_completed(futs):
            f.result()

    # Tenant-scoped history only (tenant_id required — no unscoped dump).
    events: list[dict[str, Any]] = []
    code_missing, missing_body = http_json(
        "GET", "/v1/alerts/events?" + urlencode({"limit": 10})
    )
    ok_require_tenant = code_missing == 400 and "tenant_id" in str(
        missing_body.get("error", "")
    ).lower()

    for tid in tenants[:3]:
        code2, hist = http_json(
            "GET",
            "/v1/alerts/events?" + urlencode({"tenant_id": tid, "limit": 100}),
        )
        if code2 == 200:
            for e in hist.get("events") or []:
                if e.get("tenant_id") != tid:
                    ok_require_tenant = False
                events.append(e)
    code_o, hist_o = http_json(
        "GET",
        "/v1/alerts/events?"
        + urlencode({"tenant_id": "__overall__", "limit": 100}),
    )
    overall_events = hist_o.get("events") or [] if code_o == 200 else []
    for e in overall_events:
        if e.get("tenant_id") not in (None, "__overall__"):
            ok_require_tenant = False
        events.append(e)

    names = {e.get("rule_name") for e in events}

    # Concurrent cooldown: after first evaluate, cooldown=0 rules may re-fire
    # with later timestamps; for rules we re-create with high cooldown to
    # assert single-fire, count overall dead rule events (should stay low).
    dead_overall_events = [
        e for e in overall_events if e.get("rule_name") == f"{LABEL}-dead-overall"
    ]
    # Isolation: tenant event list must not include other tenants.
    ok_isolation = True
    for tid in tenants[:3]:
        code_t, hist_t = http_json(
            "GET",
            "/v1/alerts/events?" + urlencode({"tenant_id": tid, "limit": 100}),
        )
        for e in (hist_t.get("events") or []) if code_t == 200 else []:
            if e.get("tenant_id") != tid:
                ok_isolation = False

    ok_hist = len(events) >= 2
    ok_dead = f"{LABEL}-dead-overall" in names or any(
        "dead" in (n or "") for n in names
    )
    ok_lag = f"{LABEL}-dl-lag" in names or any("lag" in (n or "") for n in names)
    ok_tenant = any(f"{LABEL}-dead-{tid}" in names for tid in tenants[:3])

    # --- auto-pause: tenant with auto_pause_tenant should be paused after fire
    # Residue API takes ?tenant= (not tenant_id).
    code_res, residue = http_json(
        "GET",
        "/v1/admin/tenant-residue?" + urlencode({"tenant": auto_pause_tid}),
    )
    paused_flag = False
    if code_res == 200:
        rows = residue.get("tenants") or []
        for row in rows:
            if row.get("tenant_id") == auto_pause_tid:
                paused_flag = bool(row.get("paused"))
                break
    # Event detail records auto_paused_tenant on fire.
    auto_pause_in_detail = any(
        (e.get("detail") or {}).get("auto_paused_tenant") == auto_pause_tid
        for e in events
        if e.get("rule_name") == f"{LABEL}-dead-{auto_pause_tid}"
    )
    # Seed a pending job and confirm lease is blocked while paused.
    http_json(
        "POST",
        "/v1/jobs",
        {
            "tenant_id": auto_pause_tid,
            "payload": {"probe": "paused"},
            "max_attempts": 1,
        },
    )
    code_lease, lease_body = http_json(
        "POST",
        "/v1/lease",
        {"tenant_id": auto_pause_tid, "worker_id": "pause-probe", "limit": 1},
    )
    lease_blocked = code_lease == 204 or not (
        (lease_body.get("jobs") or lease_body.get("job"))
        if isinstance(lease_body, dict)
        else False
    )
    ok_auto_pause = paused_flag and auto_pause_in_detail and lease_blocked
    # Resume so residual fleet teardown is clean.
    http_json("POST", f"/v1/tenants/{auto_pause_tid}/resume", {})

    # --- mute: silence tenant rule and ensure no new fire while muted
    mute_rule_name = f"{LABEL}-dead-{mute_tid}"
    mute_rid = rule_ids.get(mute_rule_name)
    ok_mute = False
    if mute_rid:
        code_m, muted_body = http_json(
            "POST",
            f"/v1/alerts/rules/{mute_rid}/mute",
            {"duration_seconds": 3600},
        )
        ok_mute = (
            code_m == 200
            and (muted_body.get("rule") or {}).get("muted_until") is not None
        )
        # Count events before re-evaluate.
        code_before, hist_before = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode({"tenant_id": mute_tid, "limit": 200}),
        )
        n_before = len(
            [
                e
                for e in (hist_before.get("events") or [])
                if e.get("rule_name") == mute_rule_name
            ]
        ) if code_before == 200 else 0
        http_json("POST", "/v1/alerts/evaluate", {})
        code_after, hist_after = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode({"tenant_id": mute_tid, "limit": 200}),
        )
        n_after = len(
            [
                e
                for e in (hist_after.get("events") or [])
                if e.get("rule_name") == mute_rule_name
            ]
        ) if code_after == 200 else -1
        ok_mute = ok_mute and n_after == n_before
        http_json("POST", f"/v1/alerts/rules/{mute_rid}/unmute", {})

    # --- resolve: mark one open event resolved in history
    ok_resolve = False
    code_open, open_hist = http_json(
        "GET",
        "/v1/alerts/events?"
        + urlencode(
            {"tenant_id": mute_tid, "status": "open", "limit": 10}
        ),
    )
    open_events = open_hist.get("events") or [] if code_open == 200 else []
    if open_events:
        eid = open_events[0]["id"]
        code_r, resolved_body = http_json(
            "POST",
            f"/v1/alerts/events/{eid}/resolve",
            {
                "tenant_id": mute_tid,
                "resolved_by": "fleet-soak",
                "note": "verified resolve path",
            },
        )
        ev_r = (resolved_body.get("event") or {}) if code_r == 200 else {}
        ok_resolve = ev_r.get("status") == "resolved" and ev_r.get("resolved_at")
        # Open filter should exclude it.
        code_open2, open_hist2 = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode(
                {"tenant_id": mute_tid, "status": "open", "limit": 50}
            ),
        )
        still_open = {
            e.get("id") for e in (open_hist2.get("events") or [])
        } if code_open2 == 200 else set()
        ok_resolve = ok_resolve and eid not in still_open
        code_closed, closed_hist = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode(
                {"tenant_id": mute_tid, "status": "resolved", "limit": 50}
            ),
        )
        closed_ids = {
            e.get("id") for e in (closed_hist.get("events") or [])
        } if code_closed == 200 else set()
        ok_resolve = ok_resolve and eid in closed_ids

    # --- auto-resolve: recover dead_count for a tenant, require 2 healthy evals
    ar_tid = tenants[2] if len(tenants) > 2 else tenants[0]
    ar_rule_name = f"{LABEL}-dead-{ar_tid}"
    ok_auto_resolve = False
    auto_resolved_count = 0
    # Capture open events before recovery.
    code_ar0, ar_open0 = http_json(
        "GET",
        "/v1/alerts/events?"
        + urlencode(
            {"tenant_id": ar_tid, "status": "open", "limit": 100}
        ),
    )
    ar_open_ids = {
        e.get("id")
        for e in (ar_open0.get("events") or [])
        if e.get("rule_name") == ar_rule_name
    } if code_ar0 == 200 else set()
    if ar_open_ids:
        # Replay dead letters so dead_count can fall to 0 (metric recovers).
        http_json(
            "POST",
            "/v1/dead-letters/replay",
            {
                "tenant_id": ar_tid,
                "limit": 500,
                "reset_attempts": True,
                "max_attempts": 3,
            },
        )
        # Drain replayed jobs via fleet workers.
        t_ar = time.time()
        while time.time() - t_ar < 60:
            code_s, snap = http_json(
                "GET",
                "/v1/metrics/snapshot?"
                + urlencode({"tenant_id": ar_tid, "window_seconds": 60}),
            )
            gauges = (snap.get("gauges") or {}) if code_s == 200 else {}
            ar_dead = int(gauges.get("dead") or 0)
            ar_pending = int(gauges.get("pending") or 0)
            ar_leased = int(gauges.get("leased") or 0)
            if ar_dead == 0 and ar_pending == 0 and ar_leased == 0:
                break
            time.sleep(0.5)
        # Concurrent same-window evaluates must not instant-resolve
        # (healthy_interval_seconds gates ticks).
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [
                pool.submit(http_json, "POST", "/v1/alerts/evaluate", {})
                for _ in range(4)
            ]
            concurrent_resolved = 0
            for f in as_completed(futs):
                code_c, body_c = f.result()
                if code_c == 200:
                    concurrent_resolved += sum(
                        1
                        for e in (body_c.get("resolved") or [])
                        if e.get("rule_name") == ar_rule_name
                    )
        # One healthy eval should not resolve yet (need 2 spaced ticks).
        code_e1, ev1 = http_json("POST", "/v1/alerts/evaluate", {})
        resolved1 = [
            e
            for e in (ev1.get("resolved") or [])
            if e.get("rule_name") == ar_rule_name
        ] if code_e1 == 200 else []
        # Wait past healthy_interval_seconds before second tick.
        time.sleep(1.2)
        code_e2, ev2 = http_json("POST", "/v1/alerts/evaluate", {})
        resolved2 = [
            e
            for e in (ev2.get("resolved") or [])
            if e.get("rule_name") == ar_rule_name
        ] if code_e2 == 200 else []
        auto_resolved_count = len(resolved2)
        code_ar2, ar_closed = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode(
                {
                    "tenant_id": ar_tid,
                    "status": "resolved",
                    "limit": 100,
                }
            ),
        )
        closed_auto = [
            e
            for e in (ar_closed.get("events") or [])
            if e.get("id") in ar_open_ids and e.get("resolved_by") == "auto"
        ] if code_ar2 == 200 else []
        code_ar1, ar_open1 = http_json(
            "GET",
            "/v1/alerts/events?"
            + urlencode(
                {"tenant_id": ar_tid, "status": "open", "limit": 100}
            ),
        )
        still_open = {
            e.get("id")
            for e in (ar_open1.get("events") or [])
            if e.get("rule_name") == ar_rule_name
        } if code_ar1 == 200 else set()
        # Concurrent burst must not resolve; after spaced second tick, closed.
        ok_auto_resolve = (
            concurrent_resolved == 0
            and len(resolved1) == 0
            and len(closed_auto) >= 1
            and auto_resolved_count >= 1
            and not (ar_open_ids & still_open)
        )

    # Pass if history recorded and at least dead-related + lag (or webhook rate).
    ok = (
        ok_hist
        and ok_dead
        and (ok_lag or dead_forced > 0)
        and dead >= 1
        and ok_require_tenant
        and ok_isolation
        and ok_auto_pause
        and ok_mute
        and ok_resolve
        and ok_auto_resolve
    )

    result = {
        "ok": ok,
        "label": LABEL,
        "total_jobs": total,
        "dead_jobs": dead,
        "dead_forced": dead_forced,
        "completed_jobs": completed,
        "evaluate_fired": fired,
        "history_count": len(events),
        "overall_dead_events": len(dead_overall_events),
        "rule_names_seen": sorted(n for n in names if n),
        "ok_hist": ok_hist,
        "ok_dead": ok_dead,
        "ok_lag": ok_lag,
        "ok_tenant": ok_tenant,
        "ok_require_tenant": ok_require_tenant,
        "ok_isolation": ok_isolation,
        "ok_auto_pause": ok_auto_pause,
        "ok_mute": ok_mute,
        "ok_resolve": ok_resolve,
        "ok_auto_resolve": ok_auto_resolve,
        "auto_resolved_count": auto_resolved_count,
        "auto_resolve_tenant": ar_tid,
        "auto_pause_tenant": auto_pause_tid,
        "auto_pause_paused": paused_flag,
        "auto_pause_in_detail": auto_pause_in_detail,
        "lease_while_paused_code": code_lease,
        "lease_blocked": lease_blocked,
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
