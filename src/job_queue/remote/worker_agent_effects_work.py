"""RemoteWorkerAgent._do_side_effects."""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from job_queue.models import Job
from job_queue.queue import LeaseError

logger = logging.getLogger(__name__)


class RemoteWorkerEffectsWorkMixin:
    def _do_side_effects(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Two-phase once-only effects; reclaim reuses started idempotency keys."""
        raw = payload.get("effects") or ["effect-0"]
        if isinstance(raw, str):
            effects = [raw]
        else:
            effects = [str(x) for x in raw]
        try:
            effect_ms = int(payload.get("effect_ms") or 80)
        except (TypeError, ValueError):
            effect_ms = 80
        sink = (
            os.environ.get("EFFECT_SINK_URL")
            or os.environ.get("SIDE_EFFECT_SINK_URL")
            or "http://effect-sink:8091/effect"
        ).rstrip("/")
        if not sink.endswith("/effect"):
            # allow base URL
            if sink.endswith("/"):
                sink = sink + "effect"
            else:
                sink = sink + "/effect"

        ran: list[str] = []
        skipped: list[str] = []
        resumed: list[str] = []
        for key in effects:
            if lease_lost.is_set():
                raise LeaseError(f"job {job.id} lease lost during effect {key}")
            try:
                begun = self.client.begin_side_effect(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    effect_key=key,
                )
            except LeaseError:
                lease_lost.set()
                raise
            except Exception:
                logger.exception("begin_side_effect failed job=%s key=%s", job.id, key)
                raise

            status = str(begun.get("status") or "")
            if status == "completed":
                skipped.append(key)
                logger.info(
                    "job %s skip effect %s (already completed) worker=%s",
                    job.id,
                    key,
                    self.worker_id,
                )
                continue
            if status == "started" and begun.get("started_by") not in (
                None,
                self.worker_id,
            ):
                resumed.append(key)
                logger.info(
                    "job %s resume effect %s (prior start by %s) idem=%s worker=%s",
                    job.id,
                    key,
                    begun.get("started_by"),
                    begun.get("idempotency_key"),
                    self.worker_id,
                )

            # Simulate non-trivial work; external system uses stable key.
            deadline = time.time() + (max(0, effect_ms) / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(f"job {job.id} lease lost during effect {key}")
                time.sleep(min(0.05, max(0.005, deadline - time.time())))

            idem = str(begun.get("idempotency_key") or f"{job.id}:{key}")
            body = json.dumps(
                {
                    "job_id": str(job.id),
                    "tenant_id": job.tenant_id,
                    "effect_key": key,
                    "worker_id": self.worker_id,
                    "idempotency_key": idem,
                }
            ).encode()
            req = urllib.request.Request(
                sink,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": idem,
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    _ = resp.read()
            except Exception:
                logger.exception("effect sink POST failed job=%s key=%s", job.id, key)
                raise

            try:
                self.client.complete_side_effect(
                    job.id,
                    tenant_id=job.tenant_id,
                    worker_id=self.worker_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    effect_key=key,
                    result={
                        "sink": "ok",
                        "worker_id": self.worker_id,
                        "idempotency_key": idem,
                    },
                )
            except LeaseError:
                lease_lost.set()
                # External call may have succeeded; started row remains so the
                # next worker retries with the same idempotency_key.
                raise
            ran.append(key)
            logger.info(
                "job %s completed effect %s worker=%s",
                job.id,
                key,
                self.worker_id,
            )

        return {
            "ok": True,
            "worker_id": self.worker_id,
            "tenant_id": job.tenant_id,
            "host": socket.gethostname(),
            "effects_ran": ran,
            "effects_skipped": skipped,
            "effects_resumed": resumed,
            "effects_total": len(effects),
        }
