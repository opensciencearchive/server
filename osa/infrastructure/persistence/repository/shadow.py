from typing import Optional

from sqlalchemy import select, insert, update, desc
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shadow.model.aggregate import ShadowId, ShadowRequest
from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shadow.port.repository import ShadowRepository
from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.mappers.shadow import (
    row_to_shadow_request,
    shadow_request_to_dict,
    row_to_shadow_report,
    shadow_report_to_dict,
)
from osa.infrastructure.persistence.tables import (
    shadow_requests_table,
    shadow_reports_table,
)


class PostgresShadowRepository(ShadowRepository):
    """PostgreSQL implementation of ShadowRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save_request(self, req: ShadowRequest) -> None:
        req_dict = shadow_request_to_dict(req)

        # Check existence (simple upsert logic)
        existing = await self.get_request(req.id)

        if existing:
            stmt = (
                update(shadow_requests_table)
                .where(shadow_requests_table.c.id == req.id)
                .values(**req_dict)
            )
        else:
            stmt = insert(shadow_requests_table).values(**req_dict)

        await self.session.execute(stmt)
        # Note: flush is often handled by UoW or caller, but here we flush to ensure ID integrity if needed
        await self.session.flush()

    async def get_request(self, id: ShadowId) -> Optional[ShadowRequest]:
        stmt = select(shadow_requests_table).where(shadow_requests_table.c.id == id)
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_shadow_request(dict(row)) if row else None

    async def get_request_by_deposition_id(
        self, deposition_id: DepositionSRN
    ) -> Optional[ShadowRequest]:
        stmt = select(shadow_requests_table).where(
            shadow_requests_table.c.deposition_id == str(deposition_id)
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_shadow_request(dict(row)) if row else None

    async def save_report(self, report: ShadowReport) -> None:
        report_dict = shadow_report_to_dict(report)
        # Assuming reports are immutable once created, simplified insert.
        # Or handle upsert if re-runs are allowed.

        existing = await self.get_report(report.shadow_id)
        if existing:
            stmt = (
                update(shadow_reports_table)
                .where(shadow_reports_table.c.shadow_id == report.shadow_id)
                .values(**report_dict)
            )
        else:
            stmt = insert(shadow_reports_table).values(**report_dict)

        await self.session.execute(stmt)
        await self.session.flush()

    async def get_report(self, id: ShadowId) -> Optional[ShadowReport]:
        stmt = select(shadow_reports_table).where(
            shadow_reports_table.c.shadow_id == id
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_shadow_report(dict(row)) if row else None

    async def list_reports(
        self, limit: int = 20, offset: int = 0
    ) -> list[ShadowReport]:
        stmt = (
            select(shadow_reports_table)
            .order_by(desc(shadow_reports_table.c.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        return [row_to_shadow_report(dict(row)) for row in rows]
