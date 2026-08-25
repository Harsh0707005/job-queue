"""Background cleaner that purges aged completed/dead jobs."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from job_queue.models import RetentionPurgeResult
from job_queue.queue import JobQueue

logger = logging.getLogger(__name__)


class RetentionCleaner:
    """Poll ``purge_expired_jobs`` until retention windows are empty.

    Example::

        cleaner = RetentionCleaner(queue, poll_interval_seconds=30.0)
        cleaner.run_once()
        # or
        cleaner.run()  # until stop()
    """

    def __init__(
        self,
        queue: JobQueue,
        *,
        poll_interval_seconds: float = 30.0,
        batch_limit: int = 500,
        tenant_id: str | None = None,
        on_purge: Callable[[RetentionPurgeResult], None] | None = None,
    ) -> None:
        self.queue = queue
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self.tenant_id = tenant_id
        self.on_purge = on_purge
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> RetentionPurgeResult:
        """One purge pass (up to *batch_limit* jobs)."""
        try:
            if hasattr(self.queue, "process_payload_blobs"):
                self.queue.process_payload_blobs(
                    limit=min(self.batch_limit, 200)
                )
        except Exception:  # noqa: BLE001
            logger.exception("payload blob gc failed")
        try:
            self.queue.process_payload_key_rotations(
                limit=20,
                batch_size=min(self.batch_limit, 500),
                tenant_id=self.tenant_id,
            )
        except Exception:  # noqa: BLE001
            logger.exception("payload key rotation advance failed")
        try:
            self.queue.process_tenant_restores(
                limit=20, batch_size=min(self.batch_limit, 200)
            )
        except Exception:  # noqa: BLE001
            logger.exception("tenant restore advance failed")
        try:
            self.queue.process_awaiting_skills(
                limit=min(self.batch_limit, 200)
            )
        except Exception:  # noqa: BLE001
            logger.exception("awaiting-skills park/revive failed")
        try:
            self.queue.process_expired_jobs(
                limit=min(self.batch_limit, 200),
                tenant_id=self.tenant_id,
            )
        except Exception:  # noqa: BLE001
            logger.exception("job ttl expiry failed")
        result = self.queue.purge_expired_jobs(
            limit=self.batch_limit,
            tenant_id=self.tenant_id,
        )
        if self.on_purge is not None:
            self.on_purge(result)
        if result.total_deleted or getattr(result, "metrics_deleted", 0):
            logger.info(
                "purged completed=%d dead=%d side_effects=%d "
                "metric_buckets=%d metric_snapshots=%d",
                result.completed_deleted,
                result.dead_deleted,
                result.side_effects_deleted,
                getattr(result, "metric_buckets_deleted", 0),
                getattr(result, "metric_snapshots_deleted", 0),
            )
        return result

    def run(self) -> int:
        """Loop until ``stop()``. Returns total jobs deleted."""
        total = 0
        while not self._stop.is_set():
            # Drain while there is work so large backlogs catch up.
            while not self._stop.is_set():
                result = self.run_once()
                total += result.total_deleted
                metrics_n = int(getattr(result, "metrics_deleted", 0) or 0)
                if result.total_deleted == 0 and metrics_n == 0:
                    break
            self._stop.wait(self.poll_interval_seconds)
        return total
