"""HTTP coordinator client (composed from domain mixins)."""
from __future__ import annotations

from job_queue.remote.client_core import ClientCoreMixin
from job_queue.remote.client_lease import ClientLeaseMixin
from job_queue.remote.client_workflow import ClientWorkflowMixin
from job_queue.remote.client_jobs import ClientJobsMixin
from job_queue.remote.client_admin import ClientAdminMixin
from job_queue.remote.client_core import _ActiveRemoteJob

class CoordinatorClient(ClientCoreMixin, ClientLeaseMixin, ClientWorkflowMixin, ClientJobsMixin, ClientAdminMixin):
    """HTTP client for one or more fleet coordinators (failover / RR)."""

    pass

__all__ = ["CoordinatorClient", "_ActiveRemoteJob"]
