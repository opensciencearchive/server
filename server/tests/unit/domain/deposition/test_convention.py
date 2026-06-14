"""Unit tests for Convention aggregate."""

from datetime import UTC, datetime

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.srn import ConventionSlug, SchemaId


def _make_conv_slug(slug: str = "test-conv") -> ConventionSlug:
    return ConventionSlug(slug)


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
            id=_make_conv_slug(),
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
            id=_make_conv_slug(),
            title="Test",
            description="A test convention",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        assert conv.description == "A test convention"

    def test_create_with_empty_hooks(self):
        conv = Convention(
            id=_make_conv_slug(),
            title="Test",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            hooks=[],
            created_at=datetime.now(UTC),
        )
        assert conv.hooks == []


class TestConventionIdentity:
    def test_id_is_bare_slug(self):
        # #145: conventions are unversioned; identity is a bare slug, not a URN.
        conv = Convention(
            id=_make_conv_slug("my-conv"),
            title="Test",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        assert conv.id.root == "my-conv"
