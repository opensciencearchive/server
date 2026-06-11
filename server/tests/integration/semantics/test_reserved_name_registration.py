"""Integration test for reserved-name enforcement at schema registration (T066).

Exercises the real schema-registration service path against Postgres: a schema
whose id is a reserved ``/data/`` path slot (``records`` / ``datasets``) is
rejected with ``ReservedNameError`` (``code="reserved_name"``), which the API
layer maps to HTTP 400. The aggregate invariant is the single source of truth;
this confirms it fires through the persistence-wired service, not just the unit.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import ReservedNameError
from osa.domain.shared.model.srn import Domain, SchemaIdentifier
from osa.infrastructure.persistence.repository.ontology import PostgresOntologyRepository
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)


def _service(session: AsyncSession) -> SchemaService:
    return SchemaService(
        schema_repo=PostgresSemanticsSchemaRepository(session),
        ontology_repo=PostgresOntologyRepository(session),
        node_domain=Domain("localhost"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reserved", ["records", "datasets"])
async def test_registering_reserved_schema_id_rejected(
    pg_engine: AsyncEngine, pg_session: AsyncSession, reserved: str
):
    service = _service(pg_session)
    with pytest.raises(ReservedNameError) as exc:
        await service.create_schema(
            id=SchemaIdentifier(reserved),
            title="should be rejected",
            version="1.0.0",
            fields=[],
        )
    assert exc.value.code == "reserved_name"
    assert exc.value.name == reserved
