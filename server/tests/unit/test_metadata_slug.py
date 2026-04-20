"""Tests for schema_slug() — pg-safe slug derivation from Schema title."""

import pytest

from osa.infrastructure.persistence.metadata_table import schema_slug


class TestSchemaSlug:
    def test_accepts_simple_title(self):
        assert schema_slug("bio_sample") == "bio_sample"

    def test_lowercases_camel_case(self):
        assert schema_slug("BioSample") == "biosample"

    def test_replaces_spaces_with_underscore(self):
        assert schema_slug("bio sample") == "bio_sample"

    def test_replaces_punctuation_with_underscore(self):
        assert schema_slug("bio-sample.v2") == "bio_sample_v2"

    def test_collapses_repeated_non_alnum(self):
        assert schema_slug("bio---sample") == "bio_sample"

    def test_strips_leading_and_trailing_underscores(self):
        assert schema_slug("__bio_sample__") == "bio_sample"

    def test_is_stable_across_invocations(self):
        assert schema_slug("BioSample v1") == schema_slug("BioSample v1")

    def test_rejects_empty_title(self):
        with pytest.raises(ValueError):
            schema_slug("")

    def test_rejects_title_with_only_punctuation(self):
        with pytest.raises(ValueError):
            schema_slug("!!!")

    def test_rejects_title_starting_with_digit(self):
        with pytest.raises(ValueError):
            schema_slug("1bio_sample")
