"""Integration tests for PostgresHookRegistry release idempotency (#217).

Mirrors ``test_ingester_registry_repo.py::TestReleaseIdempotency`` — the two
registries must share one rule until #218 collapses them into the Function
substrate. Releases are idempotent by *definition equality against the live
release*, not by digest: a config-only redeploy previously returned the stale
release and silently never took effect (hook runs execute from the live
release), which #217 fixes.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.hook import (
    ColumnDef,
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.infrastructure.persistence.repository.hook_registry import PostgresHookRegistry

_NAME = HookName("detect_pockets")
_SOURCE_REF = "git+https://example.com/pocketeer@deadbeef"


def _feature() -> TableFeatureSpec:
    return TableFeatureSpec(
        cardinality="many",
        columns=[ColumnDef(name="score", json_type="number", required=True)],
    )


def _runtime(*, digest: str = "sha256:abc", batch_size: int = 100) -> OciConfig:
    return OciConfig(
        image="ghcr.io/x/pocketeer:v1",
        digest=digest,
        config={"batch_size": batch_size},
        limits=OciLimits(),
    )


async def _seeded_registry(pg_session: AsyncSession) -> PostgresHookRegistry:
    registry = PostgresHookRegistry(pg_session)
    await registry.upsert_identity(_NAME, _feature())
    return registry


@pytest.mark.asyncio
class TestHookReleaseIdempotency:
    async def test_identical_redeploy_is_a_noop(self, pg_session: AsyncSession):
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "ci@example")

        again = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "ci@example")

        assert first.created is True
        assert again.created is False
        assert again.release.id == first.release.id
        hook = await registry.get_hook(_NAME)
        assert hook is not None and hook.live_release_id == first.release.id

    async def test_config_only_change_mints_new_release(self, pg_session: AsyncSession):
        """#217: same digest, different config MUST mint — hook runs execute from
        the live release, so digest-only dedupe made config redeploys silently
        never take effect."""
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)

        second = await registry.create_release(_NAME, _runtime(batch_size=500), _SOURCE_REF, None)

        assert second.created is True
        assert second.release.id != first.release.id
        assert second.release.version == first.release.version + 1
        assert second.release.runtime.digest == first.release.runtime.digest
        hook = await registry.get_hook(_NAME)
        assert hook is not None and hook.live_release_id == second.release.id

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
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "alice@example")

        again = await registry.create_release(_NAME, _runtime(), _SOURCE_REF, "bob@example")

        assert again.created is False
        assert again.release.id == first.release.id

    async def test_rollback_then_identical_redeploy_matches_live(self, pg_session: AsyncSession):
        """Idempotency compares against the LIVE release, honouring rollbacks."""
        registry = await _seeded_registry(pg_session)
        first = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)
        await registry.create_release(_NAME, _runtime(batch_size=500), _SOURCE_REF, None)
        await registry.set_live(_NAME, first.release.version)

        again = await registry.create_release(_NAME, _runtime(batch_size=100), _SOURCE_REF, None)

        assert again.created is False
        assert again.release.id == first.release.id
