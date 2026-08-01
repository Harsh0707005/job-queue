"""Compatibility re-export: WebhooksClaimMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.webhooks_claim_batch import WebhooksClaimBatchMixin
from job_queue.queue.webhooks_finalize import WebhooksFinalizeMixin


class WebhooksClaimMixin(WebhooksClaimBatchMixin, WebhooksFinalizeMixin):
    """Aggregated WebhooksClaimMixin (split for maintainability)."""

    pass
