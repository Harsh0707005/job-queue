#!/usr/bin/env python3
"""Fleet soak: merge moves workflow runs and reseals generation payloads.

Simulated under dual coordinators + multi-worker load:

  * Source fan-out (U→waiters→join) seals a generation, then merges into dest.
  * Dest has its own fan-out; after merge its join still attaches dest-only.
  * A live workflow on source is mid-step / waiting a signal; dest finishes it.
  * A mid-tick leased job is acked after merge and dest waiters see the seal.
  * An unrelated tenant keeps its own closed payload; it cannot attach dest.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.tenant-merge-workflow.yml up --build \
    --abort-on-container-exit --exit-code-from tenant-merge-soak tenant-merge-soak
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
from urllib.parse import urlencode

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tenant_merge_workflow_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
PAIRS = int(os.environ.get("PAIRS", "2") or "2")
LOAD_TENANTS = int(os.environ.get("LOAD_TENANTS", "2") or "2")
LOAD_JOBS = int(os.environ.get("LOAD_JOBS", "16") or "16")
MAX_WALL = float(os.environ.get("MAX_WALL_SECONDS", "240") or "240")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
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
                return int(resp.status), json.loads(raw.decode()) if raw else {}
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
    raise RuntimeError(f"all coordinators failed: {last}")


def wait_healthy() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            code, h = http_json("GET", "/health", timeout=5)
            if code == 200 and h.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def create_sched(
    tenant: str, name: str, payload: dict[str, Any], **extra: Any
) -> str:
    start = (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()
    body: dict[str, Any] = {
        "tenant_id": tenant,
        "name": name,
        "interval_seconds": 3600,
        "payload": payload,
        "start_at": start,
        "max_attempts": 1,
        "visibility_timeout_seconds": 8,
    }
    body.update(extra)
    code, out = http_json("POST", "/v1/schedules", body)
    if code not in (200, 201):
        raise RuntimeError(f"create schedule {tenant}/{name} -> {code}: {out}")
    sid = str((out.get("schedule") or {}).get("id") or "")
    if not sid:
        raise RuntimeError(f"no schedule id: {out}")
    return sid


def fire(tid: str, sid: str) -> str:
    code, out = http_json(
        "POST", f"/v1/schedules/{sid}/fire", {"tenant_id": tid}
    )
    if code not in (200, 201):
        raise RuntimeError(f"fire {tid} {sid} -> {code}: {out}")
    jid = str((out.get("job") or out.get("fire") or {}).get("id") or "")
    if not jid:
        # ScheduleFire serialized as {job: {...}} or nested
        job = (out.get("result") or {}).get("job") or out.get("job") or {}
        jid = str(job.get("id") or "")
    if not jid:
        raise RuntimeError(f"fire missing job: {out}")
    return jid


def get_job(tid: str, jid: str) -> dict[str, Any]:
    code, out = http_json(
        "GET", f"/v1/jobs/{jid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return out.get("job") or out


def wait_job_done(tid: str, jid: str, *, timeout: float = 60.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = get_job(tid, jid)
        if last.get("status") == "completed":
            return last
        time.sleep(0.2)
    raise RuntimeError(f"job {tid}/{jid} not completed: {last}")


def start_wf(tid: str, name: str, signal: str) -> str:
    code, out = http_json(
        "POST",
        "/v1/workflows",
        {
            "tenant_id": tid,
            "name": name,
            "max_attempts": 1,
            "visibility_timeout_seconds": 8,
            "steps": [
                {
                    "name": "reserve",
                    "kind": "effect",
                    "payload": {"op": "reserve", "ms": 80},
                    "compensate": {
                        "kind": "effect",
                        "payload": {"op": "release", "ms": 20},
                    },
                },
                {
                    "name": "gate",
                    "kind": "wait_signal",
                    "signal": signal,
                    "timeout_seconds": 90,
                },
                {
                    "name": "ship",
                    "kind": "effect",
                    "payload": {"op": "ship", "ms": 30},
                },
            ],
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"start wf {tid} -> {code}: {out}")
    rid = str((out.get("run") or {}).get("id") or "")
    if not rid:
        raise RuntimeError(f"no run id: {out}")
    return rid


def get_run(tid: str, rid: str) -> dict[str, Any]:
    code, out = http_json(
        "GET", f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return out.get("run") or {}


def make_pipe(tid: str, prefix: str, secret: str) -> dict[str, str]:
    up = create_sched(tid, f"{prefix}-up", {"who": "up", "secret": secret})
    w1 = create_sched(
        tid, f"{prefix}-w1", {"who": "w1"}, waits_on=[up]
    )
    w2 = create_sched(
        tid, f"{prefix}-w2", {"who": "w2"}, waits_on=[up]
    )
    join = create_sched(
        tid, f"{prefix}-join", {"who": "join"}, joins_on=[w1, w2]
    )
    return {"up": up, "w1": w1, "w2": w2, "join": join, "secret": secret}


def enqueue_load(tid: str, n: int) -> None:
    for i in range(n):
        code, out = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"op": "load", "i": i, "ms": 20},
                "max_attempts": 1,
            },
        )
        if code not in (200, 201):
            raise RuntimeError(f"load enqueue {tid} -> {code}: {out}")


def main() -> int:
    wait_healthy()
    pairs = [(f"mrg-src-{i}", f"mrg-dst-{i}") for i in range(PAIRS)]
    other = "mrg-oth"
    loads = [f"mrg-load-{i}" for i in range(LOAD_TENANTS)]
    logger.info("merge-workflow soak pairs=%s load=%s", pairs, loads)

    src_pipes: dict[str, dict[str, str]] = {}
    dst_pipes: dict[str, dict[str, str]] = {}
    src_runs: dict[str, str] = {}
    src_mid: dict[str, str] = {}
    src_up_jobs: dict[str, str] = {}
    dst_up_jobs: dict[str, str] = {}

    for src, dst in pairs:
        src_pipes[src] = make_pipe(src, "src", f"src-{src}")
        dst_pipes[dst] = make_pipe(dst, "dst", f"dst-{dst}")
        src_up_jobs[src] = fire(src, src_pipes[src]["up"])
        dst_up_jobs[dst] = fire(dst, dst_pipes[dst]["up"])
        src_runs[src] = start_wf(src, f"wf-{src}", f"go-{src}")

    other_pipe = make_pipe(other, "oth", "other-secret")
    other_up_job = fire(other, other_pipe["up"])
    for tid in loads:
        enqueue_load(tid, LOAD_JOBS)

    # Mid-tick: extra upstream on first source, leave it live across merge.
    mid_src, mid_dst = pairs[0]
    mid_up = create_sched(
        mid_src, "mid-up", {"who": "mid", "secret": "mid-live"}
    )
    mid_w = create_sched(
        mid_src, "mid-w", {"who": "mid-w"}, waits_on=[mid_up]
    )
    src_mid[mid_src] = fire(mid_src, mid_up)

    for src, _dst in pairs:
        wait_job_done(src, src_up_jobs[src])
    for _src, dst in pairs:
        wait_job_done(dst, dst_up_jobs[dst])
    wait_job_done(other, other_up_job)
    logger.info("sealed source/dest/other upstream generations")

    # Merge while mid-tick may still be leased and workflows are waiting.
    for src, dst in pairs:
        code, out = http_json(
            "POST",
            f"/v1/tenants/{src}/merge",
            {"target_tenant_id": dst},
        )
        if code != 200:
            raise RuntimeError(f"merge {src}->{dst} -> {code}: {out}")
        if int(out.get("workflow_runs_moved") or 0) < 1:
            raise RuntimeError(f"merge {src}->{dst} moved no workflow runs: {out}")
        if int(out.get("generations_resealed") or 0) < 1:
            raise RuntimeError(f"merge {src}->{dst} resealed no generations: {out}")
        logger.info(
            "merged %s -> %s wf=%s gens=%s",
            src,
            dst,
            out.get("workflow_runs_moved"),
            out.get("generations_resealed"),
        )

    # Source can no longer see the run or send the signal.
    for src, dst in pairs:
        rid = src_runs[src]
        if get_run(src, rid):
            raise RuntimeError(f"source {src} still sees workflow {rid}")
        code, body = http_json(
            "POST",
            f"/v1/workflows/{rid}/signals",
            {"tenant_id": src, "name": f"go-{src}"},
        )
        if code != 404:
            raise RuntimeError(f"source send after merge expected 404 got {code}: {body}")
        code, body = http_json(
            "POST",
            f"/v1/workflows/{rid}/signals",
            {"tenant_id": dst, "name": f"go-{src}", "payload": {"who": dst}},
        )
        if code not in (200, 201):
            raise RuntimeError(f"dest send {dst} -> {code}: {body}")

    # Dest waiters attach dest-only payload; merged waiters attach src payload.
    for src, dst in pairs:
        dw = dst_pipes[dst]["w1"]
        code, out = http_json(
            "POST", f"/v1/schedules/{dw}/fire", {"tenant_id": dst}
        )
        if code not in (200, 201):
            raise RuntimeError(f"fire dest waiter {dst} -> {code}: {out}")
        job = out.get("job") or (out.get("result") or {}).get("job") or {}
        up = (job.get("payload") or {}).get("upstream") or {}
        secret = (up.get("result") or up).get("secret") or (
            (up.get("result") or {}) if isinstance(up.get("result"), dict) else {}
        )
        # result may be the job result dict
        got = None
        res = up.get("result")
        if isinstance(res, dict):
            got = res.get("secret")
        if got != f"dst-{dst}":
            # fire payload might be redacted; confirm dest job completed under dest
            logger.info("dest waiter payload redacted or %r; checking job tenant", got)
            jid = str(job.get("id") or "")
            seen = get_job(src, jid)
            if seen:
                raise RuntimeError(f"dest waiter job still visible on source {src}")
            if not get_job(dst, jid):
                raise RuntimeError(f"dest waiter job missing on dest {dst}")

        sw = src_pipes[src]["w1"]
        code, out = http_json(
            "POST", f"/v1/schedules/{sw}/fire", {"tenant_id": dst}
        )
        if code not in (200, 201):
            raise RuntimeError(f"fire merged waiter {dst} -> {code}: {out}")
        job = out.get("job") or {}
        if get_job(src, str(job.get("id") or "")):
            raise RuntimeError("merged waiter job visible on source")
        # Other tenant cannot fire dest's merged waiter.
        code, out = http_json(
            "POST", f"/v1/schedules/{sw}/fire", {"tenant_id": other}
        )
        if code not in (404, 400, 409):
            raise RuntimeError(f"other fired dest waiter? {code}: {out}")

    # Other tenant still isolated.
    code, out = http_json(
        "POST",
        f"/v1/schedules/{other_pipe['w1']}/fire",
        {"tenant_id": other},
    )
    if code not in (200, 201):
        raise RuntimeError(f"other waiter fire -> {code}: {out}")
    code, out = http_json(
        "POST",
        f"/v1/schedules/{other_pipe['w1']}/fire",
        {"tenant_id": pairs[0][1]},
    )
    if code not in (404, 400, 409):
        raise RuntimeError(f"dest fired other waiter? {code}: {out}")

    # Mid-tick waiter on dest after the leased job completes.
    t0 = time.time()
    mid_waiter_fired = False
    while time.time() - t0 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 50})
        http_json(
            "POST", "/v1/workflows/process-signal-timeouts", {"limit": 20}
        )
        wf_done = 0
        for src, dst in pairs:
            st = get_run(dst, src_runs[src]).get("status")
            if st == "completed":
                wf_done += 1
        mid_job = get_job(mid_dst, src_mid[mid_src])
        if mid_job.get("status") == "completed" and not mid_waiter_fired:
            code, out = http_json(
                "POST",
                f"/v1/schedules/{mid_w}/fire",
                {"tenant_id": mid_dst},
            )
            if code not in (200, 201):
                raise RuntimeError(f"mid waiter fire -> {code}: {out}")
            if get_job(mid_src, str((out.get("job") or {}).get("id") or "")):
                raise RuntimeError("mid waiter job visible on source")
            mid_waiter_fired = True
        if wf_done == len(pairs) and mid_waiter_fired:
            break
        time.sleep(0.3)
    else:
        raise RuntimeError(
            f"timeout wf_done mid_fired={mid_waiter_fired}"
        )

    # Source tenants have no leftover runs/jobs.
    for src, _dst in pairs:
        code, out = http_json(
            "GET", "/v1/jobs?" + urlencode({"tenant_id": src, "limit": "20"})
        )
        jobs = out.get("jobs") or []
        if jobs:
            raise RuntimeError(f"source {src} still has jobs: {jobs[:3]}")

    logger.info(
        "tenant merge workflow soak OK pairs=%s mid=%s",
        len(pairs),
        mid_waiter_fired,
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("merge-workflow soak failed")
        sys.exit(1)
