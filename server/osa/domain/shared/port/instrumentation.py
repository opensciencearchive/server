"""OutboxInstrumentation port — a domain-probe for outbox-delivery telemetry.

One method per business fact worth measuring (a delivery reached a terminal
disposition). Consumed by the infrastructure worker and implemented by an OTel
adapter; trivially no-op-able in tests. Synchronous (emission must never block
dispatch) and keyword-only. Labels are typed enums so cardinality stays bounded.
"""

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.event import DeliveryStatus
from osa.domain.shared.port import Port


class OutboxInstrumentation(Port, Protocol):
    """Domain-probe for outbox-delivery metrics (see module docstring)."""

    @abstractmethod
    def delivery_completed(
        self,
        *,
        consumer_group: str,
        status: DeliveryStatus,
        retry_count: int,
        duration_s: float,
    ) -> None:
        """Record a delivery reaching a terminal disposition after dispatch."""
        ...
