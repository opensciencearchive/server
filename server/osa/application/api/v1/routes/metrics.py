"""Prometheus scrape endpoint (#158).

``GET /metrics`` renders OSA's owned Prometheus registry in the standard
exposition format. Mounted **unversioned at the root** (Prometheus convention),
unauthenticated, and excluded from request tracing.

When Prometheus is disabled (``OSA_OBSERVABILITY__PROMETHEUS_ENABLED=false``) or
telemetry was never configured, the bootstrap owns no registry and this returns
``404`` — operators should push OTLP instead.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from osa.infrastructure.telemetry.setup import bootstrap

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics() -> Response:
    """Render the owned Prometheus registry, or 404 when disabled."""
    registry = bootstrap.prometheus_registry
    if registry is None:
        return JSONResponse(
            status_code=404,
            content={
                "detail": (
                    "Prometheus metrics are disabled "
                    "(OSA_OBSERVABILITY__PROMETHEUS_ENABLED=false, or telemetry "
                    "is not configured). Push OTLP to a collector instead."
                )
            },
        )
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
