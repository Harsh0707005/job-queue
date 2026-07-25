#!/usr/bin/env python3
"""Distributed offboard soak: dual coordinators, batch workers, mid-flight offboard.

Fleet (via docker-compose.offboard.yml):
  2 coordinators, Postgres, 6 fair batch workers, 2 schedule runners,
  2 webhook agents, webhook sink, chaos (kill workers + coordinator-1).

Scenario:
  * 8 tenants with recurring schedules + webhooks
  * chaos kills/restarts workers and coordinator-1 throughout
  * mid-run, offboard 2 tenants that still have leased jobs and/or claimed
    (delivering) webhooks
  * those 2 must drain fully empty (no jobs/schedules/webhooks/settings)
  * the other 6 keep running, then schedules are paused and the fleet drains
    clean (no pending/leased jobs, no pending/delivering webhooks)
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
logger = logging.getLogger("offboard_soak")

COORDINATOR_URLS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
SINK_URL = os.environ.get("SINK_URL", "http://webhook-sink:8090").rstrip("/")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://webhook-sink:8090/hook")

TENANTS = int(os.environ.get("TENANTS", "8"))
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "8"))
JOB_SLEEP_MS = int(os.environ.get("JOB_SLEEP_MS", "2500"))
OFFBOARD_AFTER = float(os.environ.get("OFFBOARD_AFTER_SECONDS", "50"))
KEEP_RUNNING_AFTER = float(os.environ.get("KEEP_RUNNING_AFTER_SECONDS", "40"))
OFFBOARD_DRAIN_TIMEOUT = float(os.environ.get("OFFBOARD_DRAIN_TIMEOUT", "180"))
FINAL_DRAIN_TIMEOUT = float(os.environ.get("FINAL_DRAIN_TIMEOUT", "240"))
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "900"))
POLL = float(os.environ.get("POLL_SECONDS", "2"))
SEED_SLOW_JOBS = int(os.environ.get("SEED_SLOW_JOBS", "4"))
OFFBOARD_COUNT = int(os.environ.get("OFFBOARD_COUNT", "2"))

_rr = 0


def http_json(
    method: str,
    path: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    absolute: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    global _rr
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if absolute:
        targets = [absolute]
    else:
        assert path is not None
        start = _rr % len(COORDINATOR_URLS)
        _rr += 1
        bases = COORDINATOR_URLS[start:] + COORDINATOR_URLS[:start]
        targets = [b + path for b in bases]
    last: Exception | None = None
    for url in targets:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:  # noqa: BLE001
            last = exc
            continue
    raise RuntimeError(f"{method} {absolute or path} failed: {last}")


def wait_ready(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = 0
        for b in COORDINATOR_URLS:
            try:
                if http_json("GET", absolute=b + "/health").get("ok"):
                    ok += 1
            except Exception:
                pass
        try:
            http_json("GET", absolute=SINK_URL + "/health")
            sink_ok = True
        except Exception:
            sink_ok = False
        if ok >= 1 and sink_ok:
            logger.info("ready: %d/%d coordinators + sink", ok, len(COORDINATOR_URLS))
            return
        time.sleep(1)
    raise RuntimeError("stack not ready")


def tenant_ids() -> list[str]:
    return [f"ob-t{i:02d}" for i in range(TENANTS)]


def setup(tenants: list[str]) -> dict[str, str]:
    """Register webhooks + schedules; return tenant -> schedule_id."""
    mapping: dict[str, str] = {}
    for tid in tenants:
        http_json(
            "POST",
            "/v1/webhooks",
            {
                "tenant_id": tid,
                "url": WEBHOOK_URL,
                "events": ["job.completed"],
                "secret": f"secret-{tid}",
                "max_attempts": 8,
                "backoff_base_seconds": 1,
                "backoff_max_seconds": 30,
            },
        )
        sched = http_json(
            "POST",
            "/v1/schedules",
            {
                "tenant_id": tid,
                "name": f"ob-sched-{tid}",
                "interval_seconds": INTERVAL_SECONDS,
                "payload": {
                    "tenant": tid,
                    "kind": "offboard-soak",
                    "sleep_ms": JOB_SLEEP_MS,
                },
                "visibility_timeout_seconds": 45,
                "max_attempts": 3,
                "priority": 5,
            },
        )["schedule"]
        mapping[tid] = sched["id"]
        # Seed slow jobs so leases exist even before first schedule tick.
        for i in range(SEED_SLOW_JOBS):
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {
                        "tenant": tid,
                        "kind": "seed",
                        "i": i,
                        "sleep_ms": JOB_SLEEP_MS * 2,
                    },
                    "priority": 10,
                    "visibility_timeout_seconds": 60,
                    "max_runtime_seconds": 120,
                },
            )
        logger.info("registered %s schedule=%s seed=%d", tid, sched["id"][:8], SEED_SLOW_JOBS)
    return mapping


def residue(tenant: str | None = None) -> list[dict[str, Any]]:
    path = "/v1/admin/tenant-residue"
    if tenant:
        path += f"?tenant={tenant}"
    return list(http_json("GET", path).get("tenants") or [])


def residue_map(tenants: list[str] | None = None) -> dict[str, dict[str, Any]]:
    rows = residue()
    by = {r["tenant_id"]: r for r in rows}
    if tenants is None:
        return by
    out: dict[str, dict[str, Any]] = {}
    for t in tenants:
        if t in by:
            out[t] = by[t]
        else:
            # Explicit empty probe.
            probe = residue(t)
            out[t] = probe[0] if probe else {
                "tenant_id": t,
                "empty": True,
                "jobs": {"total": 0, "leased": 0, "pending": 0},
                "schedules": 0,
                "webhook_endpoints": 0,
                "webhook_deliveries": {"total": 0, "delivering": 0, "pending": 0},
            }
    return out


def backlog() -> dict[str, Any]:
    return http_json("GET", "/v1/admin/backlog")


def _docker(args: list[str]) -> int:
    """Best-effort docker CLI (soak container needs docker.sock)."""
    import subprocess

    try:
        r = subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode != 0 and r.stderr:
            logger.debug("docker %s -> %s", args, r.stderr.strip()[:200])
        return r.returncode
    except Exception as exc:  # noqa: BLE001
        logger.warning("docker %s failed: %s", args, exc)
        return 1


def _container_ids(name_substr: str) -> list[str]:
    import subprocess

    try:
        r = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={name_substr}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return [x for x in r.stdout.split() if x]
    except Exception:
        return []


def pin_webhook_claims() -> None:
    """Stop webhook agents briefly so pending builds, restart, allow claims.

    Goal: when we offboard, some tenants still have status=delivering rows
    (agent claimed but may not have finalized yet) plus leased jobs.
    """
    if not _container_ids("webhook-1") and not _container_ids("webhook-2"):
        logger.warning("no webhook containers visible; skip claim pin")
        return

    logger.info("pinning webhook claims: stopping webhook agents")
    for name in ("webhook-1", "webhook-2"):
        for cid in _container_ids(name):
            _docker(["kill", cid])

    # Let workers finish a few jobs so pending webhook rows accumulate.
    for _ in range(8):
        try:
            bl = backlog()
            wh = bl.get("webhooks") or {}
            jobs = bl.get("jobs") or {}
            logger.info(
                "webhook agents down jobs=%s wh=%s",
                {k: jobs.get(k) for k in ("pending", "leased", "completed")},
                wh,
            )
            if int(wh.get("pending") or 0) >= 4 and int(jobs.get("leased") or 0) >= 2:
                break
        except Exception as exc:  # noqa: BLE001
            logger.warning("pin poll: %s", exc)
        time.sleep(1)

    logger.info("restarting webhook agents to claim deliveries")
    for name in ("webhook-1", "webhook-2"):
        try:
            import subprocess

            r = subprocess.run(
                ["docker", "ps", "-aq", "-f", f"name={name}"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            ids = [x for x in r.stdout.split() if x]
            for cid in ids[:1]:
                _docker(["start", cid])
        except Exception as exc:  # noqa: BLE001
            logger.warning("restart %s: %s", name, exc)

    # Wait for claimed (delivering) rows — sink may be intentionally slow.
    for _ in range(40):
        try:
            bl = backlog()
            wh = bl.get("webhooks") or {}
            jobs = bl.get("jobs") or {}
            delivering = int(wh.get("delivering") or 0)
            pending = int(wh.get("pending") or 0)
            leased = int(jobs.get("leased") or 0)
            logger.info(
                "claim window delivering=%d pending=%d leased=%d",
                delivering,
                pending,
                leased,
            )
            if delivering > 0 and leased > 0:
                return
            if delivering > 0:
                return
        except Exception as exc:  # noqa: BLE001
            logger.warning("claim window poll: %s", exc)
        time.sleep(0.4)


def pick_offboard_targets(survivors: list[str], n: int) -> list[str]:
    """Prefer tenants with leased jobs and/or delivering webhooks."""
    rm = residue_map(survivors)
    scored: list[tuple[int, str]] = []
    for tid in survivors:
        r = rm[tid]
        jobs = r.get("jobs") or {}
        wh = r.get("webhook_deliveries") or {}
        leased = int(jobs.get("leased") or 0)
        pending = int(jobs.get("pending") or 0)
        delivering = int(wh.get("delivering") or 0)
        wh_pending = int(wh.get("pending") or 0)
        score = leased * 100 + delivering * 50 + pending * 5 + wh_pending * 2
        scored.append((score, tid))
    scored.sort(reverse=True)
    logger.info(
        "offboard candidates: %s",
        [(t, s) for s, t in scored[: min(6, len(scored))]],
    )
    chosen = [t for _, t in scored[:n]]
    # Prefer at least one target with delivering webhooks if available.
    with_del = [
        t
        for _, t in scored
        if int((rm[t].get("webhook_deliveries") or {}).get("delivering") or 0) > 0
    ]
    if with_del:
        ordered: list[str] = []
        for t in with_del + chosen:
            if t not in ordered:
                ordered.append(t)
        chosen = ordered[:n]
    # If nobody has in-flight yet, still pick first n (seed should prevent this).
    return chosen


def wait_empty(tenants: list[str], timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            rm = residue_map(tenants)
            states = {
                t: {
                    "empty": rm[t].get("empty"),
                    "jobs": (rm[t].get("jobs") or {}).get("total"),
                    "leased": (rm[t].get("jobs") or {}).get("leased"),
                    "schedules": rm[t].get("schedules"),
                    "eps": rm[t].get("webhook_endpoints"),
                    "delivering": (rm[t].get("webhook_deliveries") or {}).get(
                        "delivering"
                    ),
                    "offboarding": rm[t].get("offboarding"),
                }
                for t in tenants
            }
            logger.info("offboard drain: %s", states)
            if all(rm[t].get("empty") for t in tenants):
                return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("residue poll error (chaos?): %s", exc)
        time.sleep(POLL)
    return False


def pause_all_schedules(tenant_to_sid: dict[str, str], survivors: list[str]) -> None:
    for tid in survivors:
        sid = tenant_to_sid.get(tid)
        if not sid:
            continue
        try:
            http_json(
                "POST",
                f"/v1/schedules/{sid}/pause",
                {"tenant_id": tid},
            )
            logger.info("paused schedule %s tenant=%s", sid[:8], tid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("pause schedule %s: %s", tid, exc)


def wait_clean_backlog(timeout: float) -> dict[str, Any] | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            bl = backlog()
            jobs = bl.get("jobs") or {}
            wh = bl.get("webhooks") or {}
            logger.info(
                "final drain jobs=%s webhooks=%s",
                {k: jobs.get(k) for k in ("pending", "leased", "completed", "cancelled")},
                {k: wh.get(k) for k in ("pending", "delivering", "succeeded", "dead")},
            )
            if (
                int(jobs.get("pending") or 0) == 0
                and int(jobs.get("leased") or 0) == 0
                and int(wh.get("pending") or 0) == 0
                and int(wh.get("delivering") or 0) == 0
            ):
                # Stable for one more poll.
                time.sleep(2)
                bl2 = backlog()
                j2 = bl2.get("jobs") or {}
                w2 = bl2.get("webhooks") or {}
                if (
                    int(j2.get("pending") or 0) == 0
                    and int(j2.get("leased") or 0) == 0
                    and int(w2.get("pending") or 0) == 0
                    and int(w2.get("delivering") or 0) == 0
                ):
                    return bl2
        except Exception as exc:  # noqa: BLE001
            logger.warning("backlog poll error: %s", exc)
        time.sleep(POLL)
    return None


def main() -> int:
    wall_start = time.monotonic()
    tenants = tenant_ids()
    logger.info(
        "offboard soak tenants=%d interval=%ds sleep_ms=%d offboard_after=%.0fs "
        "keep_running=%.0fs coords=%s",
        TENANTS,
        INTERVAL_SECONDS,
        JOB_SLEEP_MS,
        OFFBOARD_AFTER,
        KEEP_RUNNING_AFTER,
        COORDINATOR_URLS,
    )
    wait_ready()
    tenant_to_sid = setup(tenants)

    # Warm up under chaos until offboard window.
    logger.info("warm-up for %.0fs under chaos...", OFFBOARD_AFTER)
    while time.monotonic() - wall_start < OFFBOARD_AFTER:
        if time.monotonic() - wall_start > MAX_WALL:
            logger.error("wall clock exceeded during warm-up")
            return 1
        try:
            bl = backlog()
            logger.info(
                "warm jobs=%s wh=%s",
                bl.get("jobs"),
                bl.get("webhooks"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("warm poll (chaos?): %s", exc)
        time.sleep(POLL)

    # Ensure targets still have in-flight work: burst more slow jobs on all tenants.
    logger.info("bursting slow jobs before offboard to pin in-flight work")
    for tid in tenants:
        for i in range(3):
            try:
                http_json(
                    "POST",
                    "/v1/jobs",
                    {
                        "tenant_id": tid,
                        "payload": {
                            "tenant": tid,
                            "kind": "pre-offboard-burst",
                            "i": i,
                            "sleep_ms": JOB_SLEEP_MS * 3,
                        },
                        "priority": 20,
                        "visibility_timeout_seconds": 90,
                        "max_runtime_seconds": 180,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("burst enqueue %s: %s", tid, exc)

    # Pin claimed (delivering) webhooks: stop agents so completions enqueue
    # pending deliveries, restart them so they claim, then offboard mid-claim.
    pin_webhook_claims()
    time.sleep(1.5)

    targets = pick_offboard_targets(tenants, OFFBOARD_COUNT)
    survivors = [t for t in tenants if t not in targets]
    logger.info("OFFBOARDING %s  (survivors %s)", targets, survivors)

    # Capture residue snapshot at offboard for forensics.
    try:
        pre = residue_map(targets)
        logger.info("pre-offboard residue: %s", json.dumps(pre, default=str))
    except Exception as exc:  # noqa: BLE001
        logger.warning("pre residue: %s", exc)

    offboard_results: dict[str, dict[str, Any]] = {}
    total_leased_rem = 0
    total_delivering_rem = 0
    for tid in targets:
        try:
            r = http_json("POST", f"/v1/tenants/{tid}/offboard", {})
            offboard_results[tid] = r
            lr = int(r.get("leased_remaining") or 0)
            dr = int(r.get("webhooks_delivering_remaining") or 0)
            total_leased_rem += lr
            total_delivering_rem += dr
            logger.info(
                "offboard %s empty=%s leased_remaining=%s delivering_remaining=%s "
                "pending_cancelled=%s schedules_deleted=%s webhooks_deleted=%s",
                tid,
                r.get("empty"),
                lr,
                dr,
                (r.get("result") or {}).get("pending_cancelled"),
                (r.get("result") or {}).get("schedules_deleted"),
                (r.get("result") or {}).get("webhooks_deleted"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("offboard %s failed: %s", tid, exc)
            return 1

    if total_leased_rem == 0:
        logger.error(
            "offboard targets had no leased jobs remaining "
            "(cannot prove in-flight job drain)"
        )
        return 1
    if total_delivering_rem == 0:
        logger.error(
            "offboard targets had no delivering webhooks remaining "
            "(cannot prove claimed-webhook drain; is sink SLOW_MS set?)"
        )
        return 1
    logger.info(
        "in-flight proof: leased_remaining=%d delivering_remaining=%d",
        total_leased_rem,
        total_delivering_rem,
    )

    # Survivors must keep receiving work: enqueue a few more + schedules still fire.
    for tid in survivors:
        try:
            http_json(
                "POST",
                "/v1/jobs",
                {
                    "tenant_id": tid,
                    "payload": {
                        "tenant": tid,
                        "kind": "survivor-post-offboard",
                        "sleep_ms": JOB_SLEEP_MS,
                    },
                    "priority": 15,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("survivor enqueue %s: %s", tid, exc)

    # Offboarded tenants must reject new work.
    for tid in targets:
        try:
            http_json(
                "POST",
                "/v1/jobs",
                {"tenant_id": tid, "payload": {"should": "fail"}},
            )
            logger.error("enqueue after offboard unexpectedly succeeded for %s", tid)
            return 1
        except Exception:
            logger.info("enqueue correctly rejected for offboarded %s", tid)

    logger.info("waiting for offboarded tenants to become fully empty...")
    if not wait_empty(targets, OFFBOARD_DRAIN_TIMEOUT):
        try:
            logger.error(
                "offboarded tenants not empty: %s",
                json.dumps(residue_map(targets), default=str),
            )
        except Exception:
            pass
        return 1
    logger.info("offboarded tenants empty ✓")

    # Survivors keep running under chaos.
    logger.info("survivors keep running for %.0fs...", KEEP_RUNNING_AFTER)
    keep_start = time.monotonic()
    survivor_completed_start: dict[str, int] = {}
    try:
        fair = http_json("GET", "/v1/admin/fairness-check")
        survivor_completed_start = {
            t: int((fair.get("completed_by_tenant") or {}).get(t) or 0)
            for t in survivors
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("fairness at keep-start: %s", exc)

    while time.monotonic() - keep_start < KEEP_RUNNING_AFTER:
        if time.monotonic() - wall_start > MAX_WALL:
            logger.error("wall clock exceeded during keep-running")
            return 1
        try:
            # Offboarded must stay empty.
            rm = residue_map(targets)
            if not all(rm[t].get("empty") for t in targets):
                logger.error(
                    "offboarded tenant reappeared residue: %s",
                    json.dumps(rm, default=str),
                )
                return 1
            bl = backlog()
            logger.info("keep-running backlog jobs=%s wh=%s", bl.get("jobs"), bl.get("webhooks"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("keep poll: %s", exc)
        time.sleep(POLL)

    # Survivors should have made more progress (or at least still healthy).
    try:
        fair = http_json("GET", "/v1/admin/fairness-check")
        by = fair.get("completed_by_tenant") or {}
        for t in survivors:
            now_c = int(by.get(t) or 0)
            before = int(survivor_completed_start.get(t) or 0)
            logger.info("survivor %s completed %d -> %d", t, before, now_c)
    except Exception as exc:  # noqa: BLE001
        logger.warning("fairness at keep-end: %s", exc)

    # Stop new scheduled work for survivors and drain.
    logger.info("pausing survivor schedules for final drain")
    pause_all_schedules(tenant_to_sid, survivors)

    final = wait_clean_backlog(FINAL_DRAIN_TIMEOUT)
    if final is None:
        logger.error("final drain timed out: %s", backlog())
        try:
            logger.error("residue all: %s", json.dumps(residue(), default=str))
        except Exception:
            pass
        return 1

    # Final assertions.
    rm_all = residue_map(tenants)
    for tid in targets:
        if not rm_all[tid].get("empty"):
            logger.error("target %s not empty at end: %s", tid, rm_all[tid])
            return 1
        # Must not appear with settings/offboarding either.
        if rm_all[tid].get("has_settings") or rm_all[tid].get("offboarding"):
            logger.error("target %s still has settings: %s", tid, rm_all[tid])
            return 1

    for tid in survivors:
        r = rm_all[tid]
        jobs = r.get("jobs") or {}
        wh = r.get("webhook_deliveries") or {}
        if int(jobs.get("pending") or 0) != 0 or int(jobs.get("leased") or 0) != 0:
            logger.error("survivor %s still active jobs: %s", tid, jobs)
            return 1
        if int(wh.get("pending") or 0) != 0 or int(wh.get("delivering") or 0) != 0:
            logger.error("survivor %s webhook backlog: %s", tid, wh)
            return 1
        # Survivors should still exist historically (completed jobs / schedules paused).
        if int(jobs.get("total") or 0) == 0 and int(r.get("schedules") or 0) == 0:
            logger.warning(
                "survivor %s has no residue (unusual but may mean complete wipe)", tid
            )

    jobs = final.get("jobs") or {}
    wh = final.get("webhooks") or {}
    if int(jobs.get("pending") or 0) or int(jobs.get("leased") or 0):
        logger.error("global job backlog not clean: %s", jobs)
        return 1
    if int(wh.get("pending") or 0) or int(wh.get("delivering") or 0):
        logger.error("global webhook backlog not clean: %s", wh)
        return 1

    # Webhook integrity: no duplicate succeeded job+event.
    try:
        whs = http_json("GET", "/v1/admin/webhook-stats")
        dups = whs.get("duplicate_succeeded_job_events") or []
        if dups:
            logger.error("duplicate webhook successes: %s", dups)
            return 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("webhook-stats: %s", exc)

    logger.info(
        "OFFBOARD SOAK PASSED targets=%s empty survivors=%s final_jobs=%s final_wh=%s",
        targets,
        survivors,
        {k: jobs.get(k) for k in ("pending", "leased", "completed", "cancelled", "dead")},
        {k: wh.get(k) for k in ("pending", "delivering", "succeeded", "dead")},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
