"""Tests for schema_slug() — pg-safe slug derivation from Schema title."""

import pytest

from osa.infrastructure.persistence.metadata_table import (
    PG_IDENT_MAX_LEN,
    check_pg_table_name,
    schema_slug,
)


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

    def test_accepts_max_length_schema_identifier(self):
        """SchemaIdentifier allows 64-char ids; slug must not reject them."""
        long_id = "a" + "b" * 63  # 64 chars, matches SchemaIdentifier upper bound
        assert schema_slug(long_id) == long_id

    def test_rejects_over_max_length(self):
        with pytest.raises(ValueError):
            schema_slug("a" + "b" * 64)  # 65 chars


class TestCheckPgTableName:
    def test_accepts_table_name_at_pg_limit(self):
        name = "a" * PG_IDENT_MAX_LEN
        check_pg_table_name(name)  # no raise

    def test_rejects_table_name_over_pg_limit(self):
        with pytest.raises(ValueError, match="exceeds PG's"):
            check_pg_table_name("a" * (PG_IDENT_MAX_LEN + 1))
