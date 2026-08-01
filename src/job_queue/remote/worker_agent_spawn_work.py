"""RemoteWorkerAgent._do_spawn_work."""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import BulkCancelResult, DrainResult, Job, SpawnChildrenResult
from job_queue.queue import ApprovalRequested, ChildrenAwaited, LeaseError


from job_queue.remote.client import CoordinatorClient, _ActiveRemoteJob

logger = logging.getLogger(__name__)


class RemoteWorkerSpawnWorkMixin:
    def _do_spawn_work(
        self,
        job: Job,
        lease_lost: threading.Event,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Fan-out subtasks, park parent, resume after join.

        Payload::
            {
              "kind": "spawn",
              "spawn_children": [{"payload": {"ms": 100}}, ...],
              "after_ms": 50,
              "timeout_seconds": 60  # optional join deadline
            }

        On first lease: spawn + park (raises ChildrenAwaited).
        After children complete: parent is pending with
        ``await_children_outcome=joined`` / checkpoint phase children_joined;
        this path finishes ``after_ms`` work and acks.
        """
        already_joined = (
            job.await_children_outcome == "joined"
            or (
                isinstance(job.checkpoint, dict)
                and job.checkpoint.get("phase") == "children_joined"
            )
        )
        if already_joined:
            after_ms = 0
            try:
                after_ms = int(payload.get("after_ms") or payload.get("ms") or 20)
            except (TypeError, ValueError):
                after_ms = 20
            if after_ms > 0:
                deadline = time.time() + (after_ms / 1000.0)
                while time.time() < deadline:
                    if lease_lost.is_set():
                        raise LeaseError(
                            f"job {job.id} lease lost after children joined"
                        )
                    time.sleep(min(0.05, max(0.005, deadline - time.time())))
            child_ids = list(job.waiting_on) or []
            if isinstance(job.checkpoint, dict):
                raw_ids = job.checkpoint.get("waiting_on") or []
                if raw_ids and not child_ids:
                    child_ids = [UUID(str(x)) for x in raw_ids]
            return {
                "ok": True,
                "joined": True,
                "child_ids": [str(x) for x in child_ids],
                "worker_id": self.worker_id,
                "tenant_id": job.tenant_id,
                "host": socket.gethostname(),
            }

        # Build child specs from payload.
        raw_children = (
            payload.get("spawn_children")
            or payload.get("spawn")
            or payload.get("children")
            or []
        )
        if not isinstance(raw_children, list) or not raw_children:
            raise ValueError("spawn job requires non-empty spawn_children list")
        children: list[dict[str, Any]] = []
        for i, item in enumerate(raw_children):
            if isinstance(item, dict) and "payload" in item:
                children.append(item)
            elif isinstance(item, dict):
                # Treat whole dict as child payload.
                children.append({"payload": item})
            else:
                children.append({"payload": {"value": item, "i": i}})

        timeout = payload.get("timeout_seconds")
        try:
            timeout_f = float(timeout) if timeout is not None else None
        except (TypeError, ValueError):
            timeout_f = None

        pre_ms = 0
        try:
            pre_ms = int(payload.get("pre_ms") or 0)
        except (TypeError, ValueError):
            pre_ms = 0
        if pre_ms > 0:
            deadline = time.time() + (pre_ms / 1000.0)
            while time.time() < deadline:
                if lease_lost.is_set():
                    raise LeaseError(f"job {job.id} lease lost before spawn")
                time.sleep(min(0.05, max(0.005, deadline - time.time())))

        if lease_lost.is_set():
            raise LeaseError(f"job {job.id} lease lost before spawn")
        try:
            result = self.client.spawn_children(
                job.id,
                tenant_id=job.tenant_id,
                worker_id=self.worker_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
                children=children,
                timeout_seconds=timeout_f,
                progress={"phase": "awaiting_children", "pre_done": True},
                wait=True,
            )
        except LeaseError:
            lease_lost.set()
            raise
        raise ChildrenAwaited(result.parent, result.children)
