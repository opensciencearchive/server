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
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.ids import RecordId

router = APIRouter(route_class=DishkaRoute)


def _parse_id_and_version(raw: str) -> tuple[RecordId, int | None]:
    """Split ``{id}`` or ``{id}@{version}`` into a typed id + optional version."""
    if "@" in raw:
        id_part, version_part = raw.split("@", 1)
        try:
            return RecordId(id_part), int(version_part)
        except ValueError as exc:
            raise ValidationError(
                f"Invalid record version in {raw!r}; expected an integer.",
                field="id",
            ) from exc
    return RecordId(raw), None


@router.get(
    "/records/{record_id}", operation_id="data_get_record_by_id", response_model=RecordResponse
)
async def get_record_by_id(
    record_id: str, service: FromDishka[DataCatalogService]
) -> RecordResponse:
    rid, version = _parse_id_and_version(record_id)
    summary = await service.get_record_by_id(rid, version)
    return RecordResponse.from_summary(summary)
