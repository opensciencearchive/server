"""US3 tests: typed field-reference parsing and tree validation."""

import pytest

from osa.domain.discovery.model.refs import (
    FeatureFieldRef,
    MetadataFieldRef,
    parse_field_ref,
)


class TestParseFieldRef:
    def test_parses_metadata_ref(self):
        ref = parse_field_ref("metadata.species")
        assert isinstance(ref, MetadataFieldRef)
        assert ref.field == "species"

    def test_parses_feature_ref(self):
        ref = parse_field_ref("features.cell_classifier.confidence")
        assert isinstance(ref, FeatureFieldRef)
        assert ref.hook == "cell_classifier"
        assert ref.column == "confidence"

    def test_rejects_unknown_prefix(self):
        with pytest.raises(ValueError, match="prefix"):
            parse_field_ref("other.foo")

    def test_rejects_malformed_metadata(self):
        with pytest.raises(ValueError):
            parse_field_ref("metadata.a.b")

    def test_rejects_malformed_feature(self):
        with pytest.raises(ValueError):
            parse_field_ref("features.hook")

    def test_rejects_invalid_identifier(self):
        with pytest.raises(ValueError):
            parse_field_ref("metadata.Has-Dash")

    def test_dotted_round_trip(self):
        assert parse_field_ref("metadata.species").dotted() == "metadata.species"
        assert parse_field_ref("features.hook.col").dotted() == "features.hook.col"
