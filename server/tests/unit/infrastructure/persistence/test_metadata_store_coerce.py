"""Unit tests for ``_coerce_value`` — ensures bad JSONB values surface as
``ValidationError`` (→ 400) instead of propagating raw ``ValueError`` (→ 500).
"""

from datetime import date, datetime

import pytest

from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.metadata_store import _coerce_value


def _date_col(name: str = "collected_on") -> ColumnDef:
    return ColumnDef(name=name, json_type="string", format="date", required=False)


def _datetime_col(name: str = "measured_at") -> ColumnDef:
    return ColumnDef(name=name, json_type="string", format="date-time", required=False)


class TestCoerceValueDate:
    def test_parses_iso_date_string(self):
        assert _coerce_value(_date_col(), "2026-04-23") == date(2026, 4, 23)

    def test_passes_through_date(self):
        d = date(2026, 4, 23)
        assert _coerce_value(_date_col(), d) is d

    def test_none_passes_through(self):
        assert _coerce_value(_date_col(), None) is None

    def test_malformed_iso_date_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            _coerce_value(_date_col("collected_on"), "2026-99-99")
        assert exc_info.value.field == "collected_on"
        assert "ISO-8601 date" in str(exc_info.value)

    def test_non_string_non_date_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            _coerce_value(_date_col(), 12345)
        assert exc_info.value.field == "collected_on"

    def test_includes_record_srn_in_error_when_provided(self):
        with pytest.raises(ValidationError, match="record urn:osa:localhost:rec:abc@1"):
            _coerce_value(_date_col(), "not-a-date", record_srn="urn:osa:localhost:rec:abc@1")


class TestCoerceValueDatetime:
    def test_parses_iso_datetime_string(self):
        assert _coerce_value(_datetime_col(), "2026-04-23T10:30:00") == datetime(
            2026, 4, 23, 10, 30, 0
        )

    def test_passes_through_datetime(self):
        dt = datetime(2026, 4, 23, 10, 30, 0)
        assert _coerce_value(_datetime_col(), dt) is dt

    def test_malformed_iso_datetime_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            _coerce_value(_datetime_col("measured_at"), "not-a-datetime")
        assert exc_info.value.field == "measured_at"
        assert "ISO-8601 date-time" in str(exc_info.value)


class TestCoerceValueOther:
    def test_text_passthrough(self):
        col = ColumnDef(name="species", json_type="string", format=None, required=False)
        assert _coerce_value(col, "Homo sapiens") == "Homo sapiens"

    def test_number_passthrough(self):
        col = ColumnDef(name="resolution", json_type="number", format=None, required=False)
        assert _coerce_value(col, 1.5) == 1.5
