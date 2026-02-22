"""Integration tests for DepositionRepository against real PostgreSQL."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.auth.model.identity import System
from osa.domain.auth.model.value import SYSTEM_USER_ID, UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.infrastructure.persistence.repository.deposition import (
    PostgresDepositionRepository,
)


def _make_deposition(
    *,
    srn: str | None = None,
    owner_id: UserId = SYSTEM_USER_ID,
    status: DepositionStatus = DepositionStatus.DRAFT,
    metadata: dict | None = None,
) -> Deposition:
    dep_id = srn or f"urn:osa:localhost:dep:{uuid4()}"
    now = datetime.now(UTC)
    return Deposition(
        srn=DepositionSRN.parse(dep_id),
        convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:test-conv@1.0.0"),
        status=status,
        metadata=metadata or {"title": "Test Deposition"},
        files=[],
        owner_id=owner_id,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
class TestDepositionRepoRoundTrip:
    async def test_save_and_get(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        dep = _make_deposition()
        await repo.save(dep)
        await pg_session.commit()

        got = await repo.get(dep.srn)
        assert got is not None
        assert str(got.srn) == str(dep.srn)
        assert str(got.convention_srn) == str(dep.convention_srn)
        assert got.status == DepositionStatus.DRAFT
        assert got.metadata == {"title": "Test Deposition"}
        assert got.owner_id == SYSTEM_USER_ID

    async def test_get_nonexistent_returns_none(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        got = await repo.get(DepositionSRN.parse(f"urn:osa:localhost:dep:{uuid4()}"))
        assert got is None

    async def test_save_update_metadata(self, pg_session: AsyncSession):
        """Save, update metadata, save again — should update in place."""
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        dep = _make_deposition()
        await repo.save(dep)
        await pg_session.commit()

        dep.update_metadata({"title": "Updated Title", "species": "human"})
        await repo.save(dep)
        await pg_session.commit()

        got = await repo.get(dep.srn)
        assert got is not None
        assert got.metadata == {"title": "Updated Title", "species": "human"}

    async def test_save_with_files(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        dep = _make_deposition()
        dep.add_file(
            DepositionFile(
                name="data.csv",
                size=1024,
                checksum="sha256:aaa",
                content_type="text/csv",
            )
        )
        await repo.save(dep)
        await pg_session.commit()

        got = await repo.get(dep.srn)
        assert got is not None
        assert len(got.files) == 1
        assert got.files[0].name == "data.csv"
        assert got.files[0].size == 1024

    async def test_list_ordered_by_created_at_desc(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        dep_a = _make_deposition(metadata={"title": "First"})
        dep_b = _make_deposition(metadata={"title": "Second"})

        await repo.save(dep_a)
        await pg_session.flush()
        await repo.save(dep_b)
        await pg_session.commit()

        result = await repo.list()
        assert len(result) == 2
        assert result[0].metadata["title"] == "Second"
        assert result[1].metadata["title"] == "First"

    async def test_list_with_limit_and_offset(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        for i in range(5):
            dep = _make_deposition(metadata={"title": f"Dep {i}"})
            await repo.save(dep)
            await pg_session.flush()
        await pg_session.commit()

        page = await repo.list(limit=2, offset=1)
        assert len(page) == 2

    async def test_count(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        assert await repo.count() == 0

        for _ in range(3):
            await repo.save(_make_deposition())
            await pg_session.flush()
        await pg_session.commit()

        assert await repo.count() == 3

    async def test_count_by_owner(self, pg_session: AsyncSession):
        identity = System()
        repo = PostgresDepositionRepository(pg_session, identity)

        other_user = UserId.generate()
        # Ensure the other user exists in users table
        from sqlalchemy import text

        await pg_session.execute(
            text("INSERT INTO users (id, display_name, created_at) VALUES (:id, :name, :now)"),
            {"id": str(other_user), "name": "Other User", "now": datetime.now(UTC)},
        )
        await pg_session.flush()

        await repo.save(_make_deposition(owner_id=SYSTEM_USER_ID))
        await pg_session.flush()
        await repo.save(_make_deposition(owner_id=other_user))
        await pg_session.flush()
        await repo.save(_make_deposition(owner_id=other_user))
        await pg_session.commit()

        assert await repo.count_by_owner(SYSTEM_USER_ID) == 1
        assert await repo.count_by_owner(other_user) == 2
