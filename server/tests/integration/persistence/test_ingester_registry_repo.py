"""Integration tests for PostgresIngesterRegistry release idempotency.

Releases are immutable and idempotent by *definition equality against the live
release* — not by digest alone. A redeploy that changes anything the release
records (image, digest, config, limits, source_ref) mints a new version, so a
run's ``release_id`` always describes exactly what was deployed; only a
byte-identical redeploy is a no-op. (``built_by`` is excluded: who built it
does not change what it is.)
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.hook import OciConfig, OciLimits
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId
from osa.infrastructure.persistence.repository.ingester_registry import (
    PostgresIngesterRegistry,
)

_NAME = IngesterName("from_pdb")
_SOURCE_REF = "git+https://example.com/r@deadbeef"


def _runtime(*, digest: str = "sha256:abc", batch_size: int = 100) -> OciConfig:
    return OciConfig(
        image="ghcr.io/x:v1",
        digest=digest,
        config={"batch_size": batch_size},
        limits=OciLimits(),
    )


async def _seeded_registry(pg_session: AsyncSession) -> PostgresIngesterRegistry:
    registry = PostgresIngesterRegistry(pg_session)
    await registry.upsert_identity(_NAME, LocalId("proteinschema"))
    return registry


@pytest.mark.asyncio
class TestReleaseIdempotency:
    async def test_identical_redeploy_is_a_noop(self, pg_session: AsyncSession):
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "ci@example")

        again = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "ci@example")

        assert first.created is True
        assert again.created is False
        assert again.release.id == first.release.id
        ingester = await registry.get_ingester(_NAME)
        assert ingester is not None and ingester.live_release_id == first.release.id

    async def test_config_only_change_mints_new_release(self, pg_session: AsyncSession):
        """Same digest, different config: the release must describe what runs —
        digest-only dedupe would silently retain stale config (Greptile P1)."""
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)

        second = await registry.create_release(_NAME, _runtime(batch_size=500), _SOURCE_REF, None)

        assert second.created is True
        assert second.release.id != first.release.id
        assert second.release.version == first.release.version + 1
        assert second.release.runtime.digest == first.release.runtime.digest
        ingester = await registry.get_ingester(_NAME)
        assert ingester is not None and ingester.live_release_id == second.release.id

    async def test_digest_change_mints_new_release(self, pg_session: AsyncSession):
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(
            _NAME, _runtime(digest="sha256:abc"), _SOURCE_REF, None
        )

        second = await registry.create_release(
            _NAME, _runtime(digest="sha256:def"), _SOURCE_REF, None
        )

        assert second.created is True
        assert second.release.version == first.release.version + 1

    async def test_built_by_alone_does_not_mint(self, pg_session: AsyncSession):
        """Who built it doesn't change what it is."""
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "alice@example")

        again = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "bob@example")

        assert again.created is False
        assert again.release.id == first.release.id

    async def test_rollback_then_identical_redeploy_matches_live(self, pg_session: AsyncSession):
        """Idempotency compares against the LIVE release, honouring rollbacks:
        after set_live(v1), redeploying v1's definition is a no-op even though
        v2 exists."""
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)
        await registry.create_release(_NAME, _runtime(batch_size=500), _SOURCE_REF, None)
        await registry.set_live(_NAME, first.release.version)

        again = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)

        assert again.created is False
        assert again.release.id == first.release.id
