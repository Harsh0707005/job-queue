"""Compatibility re-export: SchedulesMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.schedules_crud import SchedulesCrudMixin
from job_queue.queue.schedules_process import SchedulesProcessMixin
from job_queue.queue.schedules_blackout import SchedulesBlackoutMixin
from job_queue.queue.schedules_fire import SchedulesFireMixin
from job_queue.queue.schedules_move import SchedulesMoveMixin
from job_queue.queue.schedules_generation import SchedulesGenerationMixin
from job_queue.queue.schedules_generation_ops import SchedulesGenerationOpsMixin


class SchedulesMixin(
    SchedulesCrudMixin,
    SchedulesProcessMixin,
    SchedulesBlackoutMixin,
    SchedulesFireMixin,
    SchedulesMoveMixin,
    SchedulesGenerationMixin,
    SchedulesGenerationOpsMixin,
):
    """Aggregated SchedulesMixin (split for maintainability)."""

    pass
