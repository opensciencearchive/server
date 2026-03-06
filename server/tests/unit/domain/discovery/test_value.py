"""Tests for discovery domain value objects — cursor helpers and VALID_OPERATORS."""

import pytest

from osa.domain.discovery.model.value import (
    VALID_OPERATORS,
    FilterOperator,
    decode_cursor,
    encode_cursor,
)
from osa.domain.semantics.model.value import FieldType


class TestCursorRoundTrip:
    def test_round_trip_string_values(self) -> None:
        cursor = encode_cursor("2026-01-01", "urn:osa:localhost:rec:abc@1")
        decoded = decode_cursor(cursor)
        assert decoded["s"] == "2026-01-01"
        assert decoded["id"] == "urn:osa:localhost:rec:abc@1"

    def test_round_trip_numeric_values(self) -> None:
        cursor = encode_cursor(7.66, 123)
        decoded = decode_cursor(cursor)
        assert decoded["s"] == 7.66
        assert decoded["id"] == 123

    def test_round_trip_none_sort_value(self) -> None:
        cursor = encode_cursor(None, "urn:osa:localhost:rec:abc@1")
        decoded = decode_cursor(cursor)
        assert decoded["s"] is None
        assert decoded["id"] == "urn:osa:localhost:rec:abc@1"


class TestDecodeCursorErrors:
    def test_malformed_base64(self) -> None:
        with pytest.raises(ValueError, match="Malformed cursor"):
            decode_cursor("not-valid-base64!!!")

    def test_invalid_json(self) -> None:
        import base64

        bad = base64.urlsafe_b64encode(b"not json").decode()
        with pytest.raises(ValueError, match="Malformed cursor"):
            decode_cursor(bad)

    def test_missing_s_key(self) -> None:
        import base64
        import json

        bad = base64.urlsafe_b64encode(json.dumps({"id": "x"}).encode()).decode()
        with pytest.raises(ValueError, match="'s' and 'id'"):
            decode_cursor(bad)

    def test_missing_id_key(self) -> None:
        import base64
        import json

        bad = base64.urlsafe_b64encode(json.dumps({"s": 1}).encode()).decode()
        with pytest.raises(ValueError, match="'s' and 'id'"):
            decode_cursor(bad)

    def test_non_dict_payload(self) -> None:
        import base64
        import json

        bad = base64.urlsafe_b64encode(json.dumps([1, 2]).encode()).decode()
        with pytest.raises(ValueError, match="'s' and 'id'"):
            decode_cursor(bad)


class TestValidOperators:
    def test_text_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.TEXT] == {FilterOperator.EQ, FilterOperator.CONTAINS}

    def test_url_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.URL] == {FilterOperator.EQ, FilterOperator.CONTAINS}

    def test_number_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.NUMBER] == {
            FilterOperator.EQ,
            FilterOperator.GTE,
            FilterOperator.LTE,
        }

    def test_date_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.DATE] == {
            FilterOperator.EQ,
            FilterOperator.GTE,
            FilterOperator.LTE,
        }

    def test_boolean_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.BOOLEAN] == {FilterOperator.EQ}

    def test_term_operators(self) -> None:
        assert VALID_OPERATORS[FieldType.TERM] == {FilterOperator.EQ}

    def test_all_field_types_have_operators(self) -> None:
        for ft in FieldType:
            assert ft in VALID_OPERATORS, f"Missing operators for {ft}"
