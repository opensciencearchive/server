"""Unit tests for RecordDraft value object."""

from osa.domain.record.model.draft import RecordDraft
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSlug


def _make_conv_slug() -> ConventionSlug:
    return ConventionSlug("test")


class TestRecordDraft:
    def test_construction(self):
        draft = RecordDraft(
            source=DepositionSource(id="urn:osa:localhost:dep:abc"),
            metadata={"title": "Test"},
            convention_id=_make_conv_slug(),
        )
        assert draft.source.type == "deposition"
        assert draft.metadata == {"title": "Test"}
        assert draft.convention_id == _make_conv_slug()

    def test_expected_features_defaults_empty(self):
        draft = RecordDraft(
            source=DepositionSource(id="dep-1"),
            metadata={},
            convention_id=_make_conv_slug(),
        )
        assert draft.expected_features == []

    def test_expected_features_can_be_set(self):
        draft = RecordDraft(
            source=DepositionSource(id="dep-1"),
            metadata={},
            convention_id=_make_conv_slug(),
            expected_features=["pocket_detect", "qc_check"],
        )
        assert [f.root for f in draft.expected_features] == ["pocket_detect", "qc_check"]
