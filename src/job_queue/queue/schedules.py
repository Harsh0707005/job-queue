"""Compatibility re-export: SchedulesMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.schedules_crud import SchedulesCrudMixin
from job_queue.queue.schedules_process import SchedulesProcessMixin
from job_queue.queue.schedules_blackout import SchedulesBlackoutMixin


class SchedulesMixin(
    SchedulesCrudMixin, SchedulesProcessMixin, SchedulesBlackoutMixin
):
    """Aggregated SchedulesMixin (split for maintainability)."""

    pass
