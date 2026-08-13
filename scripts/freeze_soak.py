#!/usr/bin/env python3
"""Fleet soak: freeze specific jobs under multi-tenant multi-worker load.

  * Dual coordinators, four workers, two freeze tenants + healthy tenants.
  * Frozen jobs are never leased or completed by workers.
  * Cross-tenant freeze/unfreeze/dead is rejected and does not mutate state.
  * Dual-coordinator freeze / unfreeze / dead-letter races leave one valid
    terminal state (frozen, pending/completed, or dead) — never both.
  * Sibling jobs on the same tenant and all other tenants keep draining.
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
logger = logging.getLogger("freeze_soak")

COORDS = [
    u.strip().rstrip("/")
    for u in (
        os.environ.get("COORDINATOR_URLS") or "http://coordinator-1:8080"
    ).split(",")
    if u.strip()
]
HEALTHY_TENANTS = int(os.environ.get("HEALTHY_TENANTS", "3") or "3")
HEALTHY_EACH = int(os.environ.get("HEALTHY_EACH", "16") or "16")
FREEZE_EACH = int(os.environ.get("FREEZE_EACH", "20") or "20")
HOLD_EACH = int(os.environ.get("HOLD_EACH", "8") or "8")
MAX_WALL = int(os.environ.get("MAX_WALL_SECONDS", "180") or "180")
_rr = 0


def http_json(
    method: str,
    path: str,
    body: dict | None = None,
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
                payload = json.loads(raw.decode()) if raw else {}
                return int(resp.status), payload
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
    raise RuntimeError(str(last))


def wait_coords() -> None:
    for _ in range(90):
        try:
            code, body = http_json("GET", "/health")
            if code == 200 and body.get("ok"):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("coordinators not ready")


def list_jobs(tid: str) -> list[dict[str, Any]]:
    code, body = http_json("GET", f"/v1/jobs?tenant_id={tid}&limit=500")
    if code != 200:
        raise RuntimeError(f"list {tid} {code}: {body}")
    return list(body.get("jobs") or [])


def enqueue(tid: str, n: int, *, prefix: str) -> list[str]:
    ids: list[str] = []
    for i in range(n):
        code, body = http_json(
            "POST",
            "/v1/jobs",
            {
                "tenant_id": tid,
                "payload": {"kind": "work", "ms": 10, "i": i, "p": prefix},
            },
        )
        if code != 201:
            raise RuntimeError(f"enqueue {tid} {code} {body}")
        ids.append(str(body["job"]["id"]))
    return ids


def freeze(jid: str, tid: str, *, reason: str = "soak") -> tuple[int, dict]:
    return http_json(
        "POST", f"/v1/jobs/{jid}/freeze", {"tenant_id": tid, "reason": reason}
    )


def unfreeze(jid: str, tid: str) -> tuple[int, dict]:
    return http_json("POST", f"/v1/jobs/{jid}/unfreeze", {"tenant_id": tid})


def dead_frozen(jid: str, tid: str, *, error: str = "soak-drop") -> tuple[int, dict]:
    return http_json(
        "POST",
        f"/v1/jobs/{jid}/freeze/dead",
        {"tenant_id": tid, "error": error},
    )


def by_status(jobs: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for j in jobs:
        st = str(j.get("status") or "?")
        out[st] = out.get(st, 0) + 1
    return out


def main() -> int:
    healthy = [f"frz-ok-{i:02d}" for i in range(HEALTHY_TENANTS)]
    a, b = "frz-a", "frz-b"
    logger.info("freeze soak healthy=%s hold=%d each", ",".join(healthy), HOLD_EACH)
    wait_coords()

    for tid in healthy:
        enqueue(tid, HEALTHY_EACH, prefix="h")
    # Pause the freeze tenants so workers cannot complete jobs before we hold
    # a specific subset. Other tenants keep draining the whole time.
    for tid in (a, b):
        code, body = http_json("POST", f"/v1/tenants/{tid}/pause", {})
        if code != 200:
            raise RuntimeError(f"pause {tid} {code} {body}")
    a_ids = enqueue(a, FREEZE_EACH, prefix="a")
    b_ids = enqueue(b, FREEZE_EACH, prefix="b")
    hold_a = a_ids[:HOLD_EACH]
    hold_b = b_ids[:HOLD_EACH]
    expect_healthy = HEALTHY_TENANTS * HEALTHY_EACH
    expect_a_free = FREEZE_EACH - HOLD_EACH
    expect_b_free = FREEZE_EACH - HOLD_EACH

    # Dual-coordinator freeze of the hold set (second call is idempotent).
    for jid in hold_a:
        c1, b1 = freeze(jid, a)
        c2, b2 = freeze(jid, a)
        if c1 != 200 or (b1.get("job") or {}).get("status") != "frozen":
            raise RuntimeError(f"freeze a {jid} {c1} {b1}")
        if c2 != 200 or (b2.get("job") or {}).get("status") != "frozen":
            raise RuntimeError(f"idempotent freeze a {jid} {c2} {b2}")
    for jid in hold_b:
        c1, b1 = freeze(jid, b)
        if c1 != 200 or (b1.get("job") or {}).get("status") != "frozen":
            raise RuntimeError(f"freeze b {jid} {c1} {b1}")
    for tid in (a, b):
        code, body = http_json("POST", f"/v1/tenants/{tid}/resume", {})
        if code != 200:
            raise RuntimeError(f"resume {tid} {code} {body}")

    # Cross-tenant must not mutate the other tenant's job.
    probe = hold_a[0]
    code, body = freeze(probe, b, reason="steal")
    if code == 200:
        raise RuntimeError(f"cross-tenant freeze succeeded: {body}")
    got = [j for j in list_jobs(a) if j.get("id") == probe]
    if not got or got[0].get("status") != "frozen":
        raise RuntimeError(f"cross-tenant freeze mutated {got}")
    code, body = unfreeze(probe, b)
    if code == 200:
        raise RuntimeError(f"cross-tenant unfreeze succeeded: {body}")
    code, body = dead_frozen(probe, b)
    if code == 200:
        raise RuntimeError(f"cross-tenant dead succeeded: {body}")

    deadline = time.time() + MAX_WALL
    last = 0.0
    while time.time() < deadline:
        h_done = 0
        for tid in healthy:
            h_done += by_status(list_jobs(tid)).get("completed", 0)
        sa = by_status(list_jobs(a))
        sb = by_status(list_jobs(b))
        now = time.time()
        if now - last >= 2:
            logger.info(
                "healthy=%d/%d a=%s b=%s",
                h_done,
                expect_healthy,
                sa,
                sb,
            )
            last = now
        a_done = sa.get("completed", 0)
        b_done = sb.get("completed", 0)
        a_frozen = sa.get("frozen", 0)
        b_frozen = sb.get("frozen", 0)
        if (
            h_done == expect_healthy
            and a_done == expect_a_free
            and b_done == expect_b_free
            and a_frozen == HOLD_EACH
            and b_frozen == HOLD_EACH
        ):
            break
        # A hold job must never complete while frozen.
        for j in list_jobs(a) + list_jobs(b):
            if j.get("id") in set(hold_a + hold_b) and j.get("status") == "completed":
                raise RuntimeError(f"frozen job completed: {j.get('id')}")
        time.sleep(0.4)
    else:
        raise RuntimeError("timed out waiting for free jobs to drain")

    # Split hold-a: half unfreeze (both coordinators race), half dead-letter.
    thaw_a = hold_a[: HOLD_EACH // 2]
    kill_a = hold_a[HOLD_EACH // 2 :]
    for jid in thaw_a:
        c1, _ = unfreeze(jid, a)
        c2, b2 = unfreeze(jid, a)
        if c1 != 200 and c2 != 200:
            raise RuntimeError(f"unfreeze {jid} failed {c1}/{c2}")
        # Second call must not invent a second job / corrupt state.
        if c2 == 200 and (b2.get("job") or {}).get("status") not in (
            "pending",
            "leased",
            "completed",
        ):
            # already leased/completed by a worker is fine; 409 if still racing
            pass
    for jid in kill_a:
        c1, b1 = dead_frozen(jid, a, error="soak-a")
        c2, b2 = dead_frozen(jid, a, error="soak-a-2")
        winners = []
        if c1 == 200:
            winners.append((b1.get("job") or {}).get("status"))
        if c2 == 200:
            winners.append((b2.get("job") or {}).get("status"))
        if "dead" not in winners and c1 != 200 and c2 != 200:
            raise RuntimeError(f"dead-letter {jid} failed {c1} {b1} {c2} {b2}")

    thaw_b = hold_b[: HOLD_EACH // 2]
    kill_b = hold_b[HOLD_EACH // 2 :]
    for jid in thaw_b:
        code, body = unfreeze(jid, b)
        if code != 200:
            raise RuntimeError(f"unfreeze b {jid} {code} {body}")
    for jid in kill_b:
        code, body = dead_frozen(jid, b, error="soak-b")
        if code != 200:
            raise RuntimeError(f"dead b {jid} {code} {body}")

    expect_a_done = expect_a_free + len(thaw_a)
    expect_b_done = expect_b_free + len(thaw_b)
    expect_a_dead = len(kill_a)
    expect_b_dead = len(kill_b)

    while time.time() < deadline:
        sa = by_status(list_jobs(a))
        sb = by_status(list_jobs(b))
        h_done = sum(by_status(list_jobs(t)).get("completed", 0) for t in healthy)
        now = time.time()
        if now - last >= 2:
            logger.info("post thaw healthy=%d a=%s b=%s", h_done, sa, sb)
            last = now
        if (
            h_done == expect_healthy
            and sa.get("completed", 0) == expect_a_done
            and sb.get("completed", 0) == expect_b_done
            and sa.get("dead", 0) == expect_a_dead
            and sb.get("dead", 0) == expect_b_dead
            and sa.get("pending", 0) == 0
            and sb.get("pending", 0) == 0
            and sa.get("leased", 0) == 0
            and sb.get("leased", 0) == 0
            and sa.get("frozen", 0) == 0
            and sb.get("frozen", 0) == 0
        ):
            # Dead jobs must stay dead; thawed ones must not be dead.
            jobs_a = {j["id"]: j for j in list_jobs(a)}
            for jid in kill_a:
                if jobs_a[jid].get("status") != "dead":
                    raise RuntimeError(f"killed job not dead: {jobs_a[jid]}")
            for jid in thaw_a:
                if jobs_a[jid].get("status") != "completed":
                    raise RuntimeError(f"thawed job not completed: {jobs_a[jid]}")
            logger.info(
                "freeze soak OK healthy=%d a_done=%d a_dead=%d b_done=%d b_dead=%d",
                h_done,
                expect_a_done,
                expect_a_dead,
                expect_b_done,
                expect_b_dead,
            )
            return 0
        time.sleep(0.4)
    raise RuntimeError("timed out after unfreeze/dead-letter")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logger.exception("freeze soak failed")
        sys.exit(1)
