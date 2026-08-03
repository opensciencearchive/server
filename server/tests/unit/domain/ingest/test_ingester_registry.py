"""Ingester identity + versioned releases — the record-provenance anchor (#180).

Records asserted by an ingester had no traceable code identity: the image and
digest lived inline on the convention as a mutable blob, overwritten on every
redeploy, so ``record → ingest_run → ???`` dead-ended. Hooks already had
``feature row → hook_run → hook_release → (image, digest, config, source_ref)``.

These tests pin the ingester half of that symmetry.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from osa.domain.ingest.model.ingester import Ingester
from osa.domain.ingest.model.ingester_release import (
    IngesterRelease,
    IngesterReleaseId,
    IngesterReleaseOutcome,
)
from osa.domain.shared.model.hook import OciConfig, OciLimits
from osa.domain.shared.model.names import MAX_NAME_LENGTH
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId


def _runtime(digest: str = "sha256:abc") -> OciConfig:
    return OciConfig(
        image="ghcr.io/example/ncbi-ingester:v1",
        digest=digest,
        config={},
        limits=OciLimits(timeout_seconds=3600, memory="2g", cpu="1.0"),
    )


def _release(version: int = 1, digest: str = "sha256:abc") -> IngesterRelease:
    return IngesterRelease(
        id=IngesterReleaseId(uuid4()),
        ingester_name=IngesterName("from_ncbi"),
        version=version,
        runtime=_runtime(digest),
        source_ref="git:deadbee",
        built_by="ci",
        built_at=datetime.now(UTC),
    )


def _ingester(live_release_id: IngesterReleaseId | None = None) -> Ingester:
    return Ingester(
        name=IngesterName("from_ncbi"),
        schema_id=LocalId("strain"),
        live_release_id=live_release_id,
        created_at=datetime.now(UTC),
    )


class TestIngesterName:
    def test_accepts_a_pg_safe_name(self):
        assert IngesterName("from_ncbi").root == "from_ncbi"

    @pytest.mark.parametrize(
        "invalid",
        ["From_NCBI", "9lives", "has-dash", "", "a" * (MAX_NAME_LENGTH + 1)],
    )
    def test_rejects_names_that_are_unsafe_as_identifiers(self, invalid: str):
        with pytest.raises(ValueError, match="ingester name"):
            IngesterName(invalid)

    def test_is_hashable_so_it_can_key_a_dict(self):
        assert {IngesterName("from_ncbi"): 1}[IngesterName("from_ncbi")] == 1


class TestIngesterIdentity:
    def test_declares_the_schema_it_produces_records_for(self):
        assert _ingester().schema_id == LocalId("strain")

    def test_starts_with_no_live_release(self):
        assert _ingester().live_release_id is None

    def test_with_live_release_returns_a_repointed_copy(self):
        ingester = _ingester()
        release_id = IngesterReleaseId(uuid4())

        repointed = ingester.with_live_release(release_id)

        assert repointed.live_release_id == release_id
        assert ingester.live_release_id is None, "the original must not be mutated"

    def test_is_frozen(self):
        with pytest.raises(ValueError):
            _ingester().name = IngesterName("other")


class TestIngesterRelease:
    def test_carries_the_full_code_identity_that_produced_a_record(self):
        release = _release()

        assert release.runtime.image == "ghcr.io/example/ncbi-ingester:v1"
        assert release.runtime.digest == "sha256:abc"
        assert release.source_ref == "git:deadbee"

    def test_is_frozen_so_a_published_release_cannot_be_rewritten(self):
        with pytest.raises(ValueError):
            _release().version = 2

    def test_release_id_is_a_uuid(self):
        assert isinstance(_release().id, UUID)


class TestIngesterReleaseOutcome:
    def test_created_distinguishes_a_new_version_from_an_idempotent_no_op(self):
        minted = IngesterReleaseOutcome(release=_release(), created=True)
        no_op = IngesterReleaseOutcome(release=_release(), created=False)

        assert minted.created is True
        assert no_op.created is False
