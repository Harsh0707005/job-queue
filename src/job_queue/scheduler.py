"""Background helper that materializes due recurring schedules into jobs."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from job_queue.models import ScheduleFire
from job_queue.queue import JobQueue

logger = logging.getLogger(__name__)


class ScheduleRunner:
    """Poll ``process_due_schedules`` on an interval.

    Example::

        runner = ScheduleRunner(queue, poll_interval_seconds=1.0)
        runner.run_once()          # single pass
        # or
        thread = threading.Thread(target=runner.run, daemon=True)
        thread.start()
        ...
        runner.stop()
    """

    def __init__(
        self,
        queue: JobQueue,
        *,
        tenant_id: str | None = None,
        poll_interval_seconds: float = 1.0,
        batch_limit: int = 100,
        on_fire: Callable[[ScheduleFire], None] | None = None,
    ) -> None:
        self.queue = queue
        self.tenant_id = tenant_id
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_limit = batch_limit
        self.on_fire = on_fire
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> list[ScheduleFire]:
        """Process all currently due schedules (up to batch_limit)."""
        fires = self.queue.process_due_schedules(
            tenant_id=self.tenant_id,
            limit=self.batch_limit,
        )
        if self.on_fire is not None:
            for fire in fires:
                self.on_fire(fire)
        if fires:
            logger.info("fired %s schedule(s)", len(fires))
        return fires

    def run(self) -> int:
        """Loop until ``stop()``. Returns total fires processed."""
        total = 0
        while not self._stop.is_set():
            fires = self.run_once()
            total += len(fires)
            self._stop.wait(self.poll_interval_seconds)
        return total
