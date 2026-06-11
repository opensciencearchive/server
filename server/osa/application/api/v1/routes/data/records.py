"""Single-record-by-ID route — ``GET /data/records/{id}[@{version}]`` (US4).

Resolves a published record by its bare internal ID; the server finds the
schema via primary-key lookup. The response carries both ``id`` and ``srn``.
A bare ``GET /data/records`` (no id) is not defined — that slot is reserved for
the deferred cross-schema bulk read and 404s naturally.
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.application.api.v1.routes.data.models import RecordResponse
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.shared.model.ids import RecordRef

router = APIRouter(route_class=DishkaRoute)


@router.get(
    "/records/{record_id}", operation_id="data_get_record_by_id", response_model=RecordResponse
)
async def get_record_by_id(
    record_id: str, service: FromDishka[DataCatalogService]
) -> RecordResponse:
    ref = RecordRef.parse(record_id)
    summary = await service.get_record_by_id(ref.id, ref.version)
    return RecordResponse.from_summary(summary)
