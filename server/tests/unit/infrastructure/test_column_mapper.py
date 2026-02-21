"""Tests for ColumnDef → SQLAlchemy column type mapping."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from osa.domain.shared.model.hook import ColumnDef


def test_string_maps_to_text():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="title", json_type="string", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.Text)
    assert sa_col.nullable is False


def test_string_datetime_maps_to_datetime_tz():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="created", json_type="string", format="date-time", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.DateTime)
    assert sa_col.type.timezone is True


def test_string_date_maps_to_date():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="birth", json_type="string", format="date", required=False)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.Date)
    assert sa_col.nullable is True


def test_string_uuid_maps_to_uuid():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="id", json_type="string", format="uuid", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.Uuid)


def test_number_maps_to_float():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="score", json_type="number", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.Float)


def test_integer_maps_to_bigint():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="count", json_type="integer", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.BigInteger)


def test_boolean_maps_to_boolean():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="active", json_type="boolean", required=True)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, sa.Boolean)


def test_array_maps_to_jsonb():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="tags", json_type="array", required=False)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, postgresql.JSONB)


def test_object_maps_to_jsonb():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="extra", json_type="object", required=False)
    sa_col = map_column(col)
    assert isinstance(sa_col.type, postgresql.JSONB)


def test_nullable_when_not_required():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="optional", json_type="string", required=False)
    sa_col = map_column(col)
    assert sa_col.nullable is True


def test_not_nullable_when_required():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="mandatory", json_type="string", required=True)
    sa_col = map_column(col)
    assert sa_col.nullable is False


def test_column_name_preserved():
    from osa.infrastructure.persistence.column_mapper import map_column

    col = ColumnDef(name="my_column", json_type="integer", required=True)
    sa_col = map_column(col)
    assert sa_col.name == "my_column"
