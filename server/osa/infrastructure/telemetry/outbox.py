"""OTel adapter implementing :class:`OutboxInstrumentation`.

Owns the ``osa_deliveries_total`` / ``osa_dispatch_duration_seconds`` metrics.
The ``consumer_group`` label is bounded by the fixed set of registered handlers
and ``status`` by :class:`DeliveryStatus`.
"""

from opentelemetry.metrics import Meter

from osa.domain.shared.event import DeliveryStatus
from osa.domain.shared.port.instrumentation import OutboxInstrumentation


class OtelOutboxInstrumentation(OutboxInstrumentation):
    """Emits outbox-delivery metrics through an injected OTel :class:`Meter`."""

    def __init__(self, meter: Meter) -> None:
        self._deliveries = meter.create_counter(
            "osa_deliveries_total",
            description="Count of terminal outbox deliveries, by consumer group and status.",
        )
        self._dispatch = meter.create_histogram(
            "osa_dispatch_duration_seconds",
            unit="s",
            description="Wall-clock dispatch duration of a delivery, by consumer group.",
        )

    def delivery_completed(
        self,
        *,
        consumer_group: str,
        status: DeliveryStatus,
        retry_count: int,
        duration_s: float,
    ) -> None:
        self._deliveries.add(1, {"consumer_group": consumer_group, "status": status.value})
        self._dispatch.record(duration_s, {"consumer_group": consumer_group})
