"""Unit tests for Convention aggregate."""

from datetime import UTC, datetime

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.srn import ConventionSRN, SchemaId


def _make_conv_srn(id: str = "test-conv", version: str = "1.0.0") -> ConventionSRN:
    return ConventionSRN.parse(f"urn:osa:localhost:conv:{id}@{version}")


def _make_schema_id(id: str = "test-schema", version: str = "1.0.0") -> SchemaId:
    return SchemaId.parse(f"{id}@{version}")


def _make_file_reqs() -> FileRequirements:
    return FileRequirements(
        accepted_types=[".csv", ".h5ad"],
        min_count=1,
        max_count=5,
        max_file_size=5_368_709_120,
    )


class TestConventionCreation:
    def test_create_with_required_fields(self):
        conv = Convention(
            srn=_make_conv_srn(),
            title="scRNA-seq Submission",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        assert conv.title == "scRNA-seq Submission"
        assert conv.schema_id == _make_schema_id()
        assert conv.file_requirements.max_count == 5

    def test_create_with_description(self):
        conv = Convention(
            srn=_make_conv_srn(),
            title="Test",
            description="A test convention",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        assert conv.description == "A test convention"

    def test_create_with_empty_hooks(self):
        conv = Convention(
            srn=_make_conv_srn(),
            title="Test",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            hooks=[],
            created_at=datetime.now(UTC),
        )
        assert conv.hooks == []


class TestConventionImmutability:
    def test_srn_is_versioned(self):
        conv = Convention(
            srn=_make_conv_srn("my-conv", "2.0.0"),
            title="Test",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        assert str(conv.srn) == "urn:osa:localhost:conv:my-conv@2.0.0"
