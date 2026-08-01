"""Compatibility re-export: WebhooksOpsMixin composed from sub-mixins."""

from __future__ import annotations

from job_queue.queue.webhooks_registry import WebhooksRegistryMixin
from job_queue.queue.webhooks_delivery import WebhooksDeliveryMixin


class WebhooksOpsMixin(WebhooksRegistryMixin, WebhooksDeliveryMixin):
    """Aggregated WebhooksOpsMixin (split for maintainability)."""

    pass
