"""Records API routes."""

from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.model.srn import RecordSRN

router = APIRouter(
    prefix="/records",
    tags=["records"],
    route_class=DishkaRoute,
)


class RecordResponse(BaseModel):
    """Single record response."""

    record: dict[str, Any]


@router.get("/{srn:path}")
async def get_record(
    srn: str,
    repo: FromDishka[RecordRepository],
) -> RecordResponse:
    """Get a record by SRN."""
    try:
        record_srn = RecordSRN.parse(srn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SRN: {e}")

    record = await repo.get(record_srn)
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    return RecordResponse(
        record={
            "srn": str(record.srn),
            "deposition_srn": str(record.deposition_srn),
            "metadata": record.metadata,
            "published_at": record.published_at.isoformat(),
        }
    )
