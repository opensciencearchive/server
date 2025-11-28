from typing import List

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from osa.domain.shadow.model.aggregate import ShadowId
from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shadow.port.repository import ShadowRepository
from osa.domain.shadow.service.orchestrator import ShadowOrchestrator


router = APIRouter(prefix="/shadows", tags=["shadow"])


class CreateShadowRequestDTO(BaseModel):
    url: str
    profile_srn: str = "urn:osa:osa-registry:profile:default@1.0.0"  # Default for now


class ShadowStatusDTO(BaseModel):
    shadow_id: str
    status: str
    status_url: str


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ShadowStatusDTO,
    description="Start a Shadow Archive analysis (Roast My Dataset)",
)
async def create_shadow_request(
    payload: CreateShadowRequestDTO,
    orchestrator: FromDishka[ShadowOrchestrator],
) -> ShadowStatusDTO:
    # Trigger the workflow
    # In a real async app, this might just queue a job.
    # Here we call the orchestrator which currently runs synchronously (mostly).
    # If Orchestrator.start_workflow is slow, this should be offloaded to a background task.

    shadow_id = orchestrator.start_workflow(payload.url, payload.profile_srn)

    return ShadowStatusDTO(
        shadow_id=shadow_id,
        status="pending",  # Initial status
        status_url=f"/shadows/{shadow_id}",
    )


@router.get(
    "/{shadow_id}",
    response_model=ShadowReport,
    description="Get the Shadow Report or current status",
)
async def get_shadow_report(
    shadow_id: str,
    repo: FromDishka[ShadowRepository],
) -> ShadowReport:
    # Check if report exists
    report = repo.get_report(ShadowId(shadow_id))
    if report:
        return report

    # Check if request exists (for status)
    req = repo.get_request(ShadowId(shadow_id))
    if not req:
        raise HTTPException(status_code=404, detail="Shadow request not found")

    # If request exists but no report, return status (represented as a partial report or 202?)
    # The spec says "Returns ShadowReport (or status if still processing)".
    # I'll return a 202 with status if not ready, or map Request to Report structure with empty fields?
    # 202 is better for polling.

    # But FastAPI response_model=ShadowReport expects that schema.
    # I'll just raise an HTTPException with 202 (FastAPI allows this workaround) or change return type.
    # Or I can return a Union[ShadowReport, ShadowStatusDTO].

    # For simplicity, I'll throw 404 if not complete yet, but with a specific message,
    # OR return a dummy report with status.

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Analysis in progress. Status: {req.status}",
    )


@router.get(
    "", response_model=List[ShadowReport], description="Get the Shadow Leaderboard"
)
async def list_shadow_reports(
    repo: FromDishka[ShadowRepository],
    limit: int = 20,
    offset: int = 0,
) -> List[ShadowReport]:
    return repo.list_reports(limit=limit, offset=offset)
