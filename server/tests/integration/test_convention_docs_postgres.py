"""Integration tests for ``conventions.docs`` persistence against real Postgres (#151, US2).

Docs persist as a ``NOT NULL`` JSONB column; the slug-keyed re-deploy (upsert)
replaces them wholesale; ``GET /api/v1/conventions/{slug}`` (public) returns
the docs block for author read-back.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.docs import ConventionDocs, Example
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.srn import ConventionSlug, SchemaId
from osa.infrastructure.persistence.repository.convention import (
    PostgresConventionRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-integration-tests-minimum-32-chars")

if "postgresql" not in os.environ.get("OSA_DATABASE__URL", ""):
    pytest.skip("OSA_DATABASE__URL not set to PostgreSQL", allow_module_level=True)


def _docs(purpose: str = "Test-campaign results for structural alloys.") -> ConventionDocs:
    return ConventionDocs(
        purpose=purpose,
        example_questions=[
            "Which alloys stay ductile below -40C?",
            "What is the yield strength range for 7xxx-series samples?",
            "Which samples have corrosion panels attached?",
        ],
        examples=[
            Example(
                question="Which alloys stay ductile below -40C?",
                query='POST /api/v1/data/alloy-tests/records {"filter": {}}',
                interpretation="Rows are individual test coupons.",
            )
        ],
        when_not_to_use="Not a materials-property database.",
        see_also=["https://other-node.example.org"],
    )


def _convention(docs: ConventionDocs) -> Convention:
    return Convention(
        id=ConventionSlug.parse("alloy-tests-conv"),
        title="Alloy Ductility Tests",
        description="Mechanical test results",
        schema_id=SchemaId.parse("alloy-tests@2.1.0"),
        file_requirements=FileRequirements(
            accepted_types=[".csv"], min_count=0, max_count=5, max_file_size=100_000_000
        ),
        hooks=[],
        ingester=None,
        docs=docs,
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
class TestConventionDocsRoundTrip:
    async def test_deploy_persists_docs(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        await repo.save(_convention(_docs()))
        await pg_session.commit()

        row = (
            (
                await pg_session.execute(
                    select(conventions_table.c.docs).where(
                        conventions_table.c.id == "alloy-tests-conv"
                    )
                )
            )
            .mappings()
            .first()
        )
        assert row is not None
        assert row["docs"]["purpose"] == "Test-campaign results for structural alloys."

        loaded = await repo.get(ConventionSlug.parse("alloy-tests-conv"))
        assert loaded is not None
        assert loaded.docs == _docs()

    async def test_redeploy_replaces_docs_wholesale(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        await repo.save(_convention(_docs()))
        await pg_session.commit()

        new_docs = ConventionDocs(
            purpose="A completely new purpose.",
            example_questions=["a?", "b?", "c?"],
            examples=[Example(question="a?", query="GET /x", interpretation="means x")],
        )
        await repo.save(_convention(new_docs))
        await pg_session.commit()

        loaded = await repo.get(ConventionSlug.parse("alloy-tests-conv"))
        assert loaded is not None
        assert loaded.docs == new_docs
        # Replaced wholesale — optional fields from the first deploy are gone.
        assert loaded.docs.when_not_to_use is None
        assert loaded.docs.see_also is None

    async def test_get_convention_returns_docs_block(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        await repo.save(_convention(_docs()))
        await pg_session.commit()

        from osa.application.api.rest.app import create_app

        client = AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://test")
        async with client:
            resp = await client.get("/api/v1/conventions/alloy-tests-conv")
        assert resp.status_code == 200
        body = resp.json()
        assert body["docs"]["purpose"] == "Test-campaign results for structural alloys."
        assert len(body["docs"]["examples"]) == 1
        assert body["docs"]["examples"][0]["question"] == "Which alloys stay ductile below -40C?"
