#!/usr/bin/env python3
"""Fleet soak: named wait_signal steps under a multi-worker multi-tenant load.

Stage 1 — early latch still counts; late send unblocks a parked wait.
Stage 2 — missed deadline fails the run and reverse-compensates.
Stage 3 — one tenant cannot send or list another tenant's signals.

  docker compose -f docker-compose.yml -f docker-compose.workflow.yml \
    -f docker-compose.workflow-signal.yml up --build \
    --abort-on-container-exit --exit-code-from workflow-soak workflow-soak
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
logger = logging.getLogger("workflow_signal_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS")
        or "http://coordinator-1:8080,http://coordinator-2:8080"
    ).split(",")
    if u.strip()
]
SINK = os.environ.get("EFFECT_SINK_URL", "http://effect-sink:8091").rstrip("/")
TENANTS = int(os.environ.get("TENANTS", "4") or "4")
EARLY_PER = int(os.environ.get("EARLY_PER_TENANT", "3") or "3")
LATE_PER = int(os.environ.get("LATE_PER_TENANT", "3") or "3")
DEADLINE_PER = int(os.environ.get("DEADLINE_PER_TENANT", "2") or "2")
ISO_PAIRS = int(os.environ.get("ISO_PAIRS", "4") or "4")
TIMEOUT_SECONDS = float(os.environ.get("SIGNAL_TIMEOUT_SECONDS", "4") or "4")
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


def wait_healthy(timeout: float = 180.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, h = http_json("GET", "/health", timeout=5)
            if code == 200 and h.get("ok"):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise RuntimeError("coordinators not ready")


def start_wf(tenant: str, name: str, steps: list[dict[str, Any]]) -> str:
    code, body = http_json(
        "POST",
        "/v1/workflows",
        {
            "tenant_id": tenant,
            "name": name,
            "max_attempts": 1,
            "visibility_timeout_seconds": 4,
            "max_compensate_retries": 8,
            "steps": steps,
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"start {tenant}/{name} -> {code}: {body}")
    rid = str((body.get("run") or {}).get("id") or "")
    if not rid:
        raise RuntimeError(f"no run id: {body}")
    return rid


def get_run(tid: str, rid: str) -> dict[str, Any]:
    code, body = http_json(
        "GET", f"/v1/workflows/{rid}?" + urlencode({"tenant_id": tid})
    )
    if code != 200:
        return {}
    return body.get("run") or {}


def send_sig(tid: str, rid: str, name: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    body: dict[str, Any] = {"tenant_id": tid, "name": name}
    if payload is not None:
        body["payload"] = payload
    return http_json("POST", f"/v1/workflows/{rid}/signals", body)


def wait_steps(*, signal: str, timeout: float | None) -> list[dict[str, Any]]:
    wait: dict[str, Any] = {
        "name": "gate",
        "kind": "wait_signal",
        "signal": signal,
    }
    if timeout is not None:
        wait["timeout_seconds"] = timeout
    return [
        {
            "name": "reserve",
            "kind": "effect",
            "payload": {"op": "reserve", "ms": 40},
            "compensate": {"kind": "effect", "payload": {"op": "release", "ms": 20}},
        },
        wait,
        {
            "name": "ship",
            "kind": "effect",
            "payload": {"op": "ship", "ms": 30},
        },
    ]


def early_steps(signal: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "gate",
            "kind": "wait_signal",
            "signal": signal,
            "timeout_seconds": 60,
        },
        {
            "name": "work",
            "kind": "effect",
            "payload": {"op": "work", "ms": 30},
        },
    ]


def sink_stats() -> dict[str, Any]:
    req = urllib.request.Request(SINK + "/stats", method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def main() -> int:
    tenants = [f"sig-t{i:02d}" for i in range(TENANTS)]
    logger.info(
        "signal soak tenants=%s early/t=%s late/t=%s dead/t=%s iso=%s timeout=%s",
        TENANTS,
        EARLY_PER,
        LATE_PER,
        DEADLINE_PER,
        ISO_PAIRS,
        TIMEOUT_SECONDS,
    )
    wait_healthy()

    early: list[tuple[str, str, str]] = []
    late: list[tuple[str, str, str]] = []
    dead: list[tuple[str, str]] = []
    iso_a: list[tuple[str, str, str]] = []
    iso_b: list[tuple[str, str, str]] = []

    def start_all() -> None:
        work: list[tuple[str, str, str, list[dict[str, Any]]]] = []
        for tid in tenants:
            for i in range(EARLY_PER):
                sig = f"early-{tid}-{i}"
                work.append((tid, f"early-{i}", sig, early_steps(sig)))
            for i in range(LATE_PER):
                sig = f"late-{tid}-{i}"
                work.append((tid, f"late-{i}", sig, wait_steps(signal=sig, timeout=60)))
            for i in range(DEADLINE_PER):
                work.append(
                    (
                        tid,
                        f"dead-{i}",
                        f"miss-{tid}-{i}",
                        wait_steps(signal=f"miss-{tid}-{i}", timeout=TIMEOUT_SECONDS),
                    )
                )
        with ThreadPoolExecutor(max_workers=16) as pool:
            futs = {
                pool.submit(start_wf, tid, name, steps): (tid, name, sig)
                for tid, name, sig, steps in work
            }
            for fut in as_completed(futs):
                tid, name, sig = futs[fut]
                rid = fut.result()
                if name.startswith("early"):
                    early.append((tid, rid, sig))
                elif name.startswith("late"):
                    late.append((tid, rid, sig))
                else:
                    dead.append((tid, rid))

        for i in range(ISO_PAIRS):
            ta = f"iso-a{i:02d}"
            tb = f"iso-b{i:02d}"
            sig = f"iso-{i}"
            ra = start_wf(ta, f"iso-a-{i}", early_steps(sig))
            rb = start_wf(tb, f"iso-b-{i}", early_steps(sig))
            iso_a.append((ta, ra, sig))
            iso_b.append((tb, rb, sig))

    start_all()
    logger.info(
        "started early=%s late=%s dead=%s iso=%s",
        len(early),
        len(late),
        len(dead),
        len(iso_a),
    )

    # Stage 1 early + stage 3: send immediately (before the wait is "due"
    # on later steps; first-step waits are already parked).
    for tid, rid, sig in early:
        code, body = send_sig(tid, rid, sig, {"when": "early"})
        if code not in (200, 201):
            raise RuntimeError(f"early send {tid} {rid} -> {code}: {body}")

    # Stage 3 isolation: B cannot unblock A with the same signal name.
    for (ta, ra, sig), (tb, rb, _) in zip(iso_a, iso_b):
        code, body = send_sig(tb, ra, sig, {"who": "intruder"})
        if code != 404:
            raise RuntimeError(f"cross-tenant send expected 404 got {code}: {body}")
        listed = http_json(
            "GET",
            f"/v1/workflows/{ra}/signals?" + urlencode({"tenant_id": tb}),
        )
        if listed[0] != 404:
            raise RuntimeError(f"cross-tenant list expected 404 got {listed}")
        code_b, body_b = send_sig(tb, rb, sig, {"who": "owner-b"})
        if code_b not in (200, 201):
            raise RuntimeError(f"iso B send {tb} -> {code_b}: {body_b}")

    t0 = time.time()
    late_sent: set[str] = set()
    iso_a_sent: set[str] = set()
    last_log = 0.0
    while time.time() - t0 < MAX_WALL:
        http_json("POST", "/v1/workflows/process-signal-timeouts", {"limit": 100})
        http_json("POST", "/v1/workflows/process-compensation", {"limit": 100})

        # Late sends: wait until the gate is parked (reserve finished), then latch.
        for tid, rid, sig in late:
            if rid in late_sent:
                continue
            code, body = http_json(
                "GET",
                f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
            )
            if code != 200:
                continue
            steps = body.get("steps") or []
            gate = next((s for s in steps if s.get("name") == "gate"), None)
            if gate and gate.get("status") == "awaiting_signal":
                sc, sb = send_sig(tid, rid, sig, {"when": "late"})
                if sc not in (200, 201):
                    raise RuntimeError(f"late send {tid} {rid} -> {sc}: {sb}")
                late_sent.add(rid)

        # After proving isolation, A may proceed.
        for ta, ra, sig in iso_a:
            if ra in iso_a_sent:
                continue
            st = get_run(ta, ra).get("status")
            if st == "running":
                sc, sb = send_sig(ta, ra, sig, {"who": "owner-a"})
                if sc not in (200, 201):
                    raise RuntimeError(f"iso A send {ta} -> {sc}: {sb}")
                iso_a_sent.add(ra)

        early_done = sum(
            1 for tid, rid, _ in early if get_run(tid, rid).get("status") == "completed"
        )
        late_done = sum(
            1 for tid, rid, _ in late if get_run(tid, rid).get("status") == "completed"
        )
        dead_failed = 0
        dead_pending = 0
        for tid, rid in dead:
            st = get_run(tid, rid).get("status")
            if st == "failed":
                dead_failed += 1
            elif st not in ("completed",):
                dead_pending += 1
        iso_a_done = sum(
            1 for tid, rid, _ in iso_a if get_run(tid, rid).get("status") == "completed"
        )
        iso_b_done = sum(
            1 for tid, rid, _ in iso_b if get_run(tid, rid).get("status") == "completed"
        )

        now = time.time()
        if now - last_log > 5:
            logger.info(
                "progress early=%s/%s late=%s/%s dead=%s/%s isoA=%s/%s isoB=%s/%s",
                early_done,
                len(early),
                late_done,
                len(late),
                dead_failed,
                len(dead),
                iso_a_done,
                len(iso_a),
                iso_b_done,
                len(iso_b),
            )
            last_log = now

        if (
            early_done == len(early)
            and late_done == len(late)
            and dead_failed == len(dead)
            and iso_a_done == len(iso_a)
            and iso_b_done == len(iso_b)
        ):
            break
        time.sleep(0.4)
    else:
        logger.error("timed out waiting for signal soak")
        return 1

    # Deadline runs must not have completed ship; compensate reserve.
    for tid, rid in dead:
        code, body = http_json(
            "GET",
            f"/v1/workflows/{rid}/steps?" + urlencode({"tenant_id": tid}),
        )
        if code != 200:
            raise RuntimeError(f"steps {tid} {rid} -> {code}")
        steps = body.get("steps") or []
        gate = next(s for s in steps if s.get("name") == "gate")
        if gate.get("status") != "failed":
            raise RuntimeError(f"deadline gate not failed: {gate}")
        comps = [
            s
            for s in steps
            if s.get("direction") == "compensate" and s.get("name") == "reserve"
        ]
        if not comps or comps[0].get("status") != "compensated":
            raise RuntimeError(f"deadline did not undo reserve: {steps}")

    # Isolation: A never stored B's payload.
    for ta, ra, _ in iso_a:
        code, body = http_json(
            "GET",
            f"/v1/workflows/{ra}/signals?" + urlencode({"tenant_id": ta}),
        )
        if code != 200:
            raise RuntimeError(f"list A signals {code}: {body}")
        payloads = [(s.get("payload") or {}) for s in (body.get("signals") or [])]
        if any(p.get("who") == "intruder" for p in payloads):
            raise RuntimeError(f"tenant A stored foreign signal: {payloads}")

    logger.info(
        "workflow signal soak OK early=%s late=%s dead=%s iso=%s",
        len(early),
        len(late),
        len(dead),
        len(iso_a),
    )
    _ = sink_stats
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("signal soak failed")
        sys.exit(1)
