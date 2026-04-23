"""Tests for metadata column mapping — reuses the shared column_mapper."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.column_mapper import map_column


class TestScalarTypes:
    def test_text(self):
        col = map_column(ColumnDef(name="title", json_type="string", required=True))
        assert isinstance(col.type, sa.Text)
        assert col.nullable is False

    def test_number(self):
        col = map_column(ColumnDef(name="resolution", json_type="number", required=True))
        assert isinstance(col.type, sa.Float)

    def test_integer(self):
        col = map_column(ColumnDef(name="count", json_type="integer", required=True))
        assert isinstance(col.type, sa.BigInteger)

    def test_boolean(self):
        col = map_column(ColumnDef(name="ok", json_type="boolean", required=False))
        assert isinstance(col.type, sa.Boolean)
        assert col.nullable is True

    def test_date(self):
        col = map_column(ColumnDef(name="d", json_type="string", format="date", required=False))
        assert isinstance(col.type, sa.Date)

    def test_array_jsonb(self):
        col = map_column(ColumnDef(name="tags", json_type="array", required=False))
        assert isinstance(col.type, JSONB)
