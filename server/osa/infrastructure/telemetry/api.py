"""OTel adapter for API-edge telemetry.

Infrastructure-only (no domain port): the API layer is itself infrastructure, so
there is no domain fact to abstract. Owns ``osa_unhandled_errors_total``,
incremented by the app's bare-``Exception`` handler.
"""

from opentelemetry.metrics import Meter


class ApiInstrumentation:
    """Emits API-edge metrics through an injected OTel :class:`Meter`."""

    def __init__(self, meter: Meter) -> None:
        self._unhandled_errors = meter.create_counter(
            "osa_unhandled_errors_total",
            description="Count of unhandled exceptions surfaced to the global error handler.",
        )

    def unhandled_error(self) -> None:
        """Record one unhandled exception reaching the global error handler."""
        self._unhandled_errors.add(1)
