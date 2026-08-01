"""Compatibility re-export: WebhooksDeliveryMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.webhooks_query import WebhooksQueryMixin
from job_queue.queue.webhooks_claim import WebhooksClaimMixin
from job_queue.queue.webhooks_enqueue import WebhooksEnqueueMixin


class WebhooksDeliveryMixin(
    WebhooksQueryMixin, WebhooksClaimMixin, WebhooksEnqueueMixin
):
    """Aggregated WebhooksDeliveryMixin (split for maintainability)."""

    pass
