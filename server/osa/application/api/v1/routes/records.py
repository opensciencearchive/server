"""Records API routes."""

from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from osa.domain.record.query.get_record import GetRecord, GetRecordHandler, RecordDetail
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
    handler: FromDishka[GetRecordHandler],
) -> RecordResponse:
    """Get a record by SRN."""
    try:
        record_srn = RecordSRN.parse(srn)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid SRN: {e}")

    result: RecordDetail = await handler.run(GetRecord(srn=record_srn))

    return RecordResponse(
        record={
            "srn": str(result.srn),
            "deposition_srn": str(result.deposition_srn),
            "metadata": result.metadata,
            "published_at": result.published_at.isoformat(),
        }
    )
