"""Unit tests for RecordDraft value object."""

from osa.domain.record.model.draft import RecordDraft
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSRN


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


class TestRecordDraft:
    def test_construction(self):
        draft = RecordDraft(
            source=DepositionSource(id="urn:osa:localhost:dep:abc"),
            metadata={"title": "Test"},
            convention_srn=_make_conv_srn(),
        )
        assert draft.source.type == "deposition"
        assert draft.metadata == {"title": "Test"}
        assert draft.convention_srn == _make_conv_srn()

    def test_expected_features_defaults_empty(self):
        draft = RecordDraft(
            source=DepositionSource(id="dep-1"),
            metadata={},
            convention_srn=_make_conv_srn(),
        )
        assert draft.expected_features == []

    def test_expected_features_can_be_set(self):
        draft = RecordDraft(
            source=DepositionSource(id="dep-1"),
            metadata={},
            convention_srn=_make_conv_srn(),
            expected_features=["pocket_detect", "qc_check"],
        )
        assert draft.expected_features == ["pocket_detect", "qc_check"]
