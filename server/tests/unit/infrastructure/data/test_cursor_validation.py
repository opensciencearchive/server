"""A malformed pagination cursor must surface as a 400, not a 500.

``decode_cursor`` raises bare ``ValueError`` on corrupt base64 / missing keys,
and the feature sort coerces ``id`` with ``int(...)`` which also raises
``ValueError`` on a non-integer. Both run inside ``stream_rows``' pre-flight
pull; if they escape as raw ``ValueError`` (not an ``OSAError``) the global
handler returns 500. These tests pin them to ``ValidationError(field="cursor")``
so the route maps them to 400 — exercised DB-free via the sort helpers, which
decode the cursor before any DB access.
"""

import base64
import json
from datetime import date, datetime

import pytest
import sqlalchemy as sa

from osa.domain.data.model.query_plan import (
    PaginationCursor,
    PaginationParams,
    QueryPlan,
    SortDirection,
    SortSpec,
    TableKind,
    encode_cursor,
)
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.data.postgres_table_read_store import PostgresTableReadStore
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
)
from osa.infrastructure.persistence.tables import records_table

SCHEMA = SchemaId.parse("compound@1.0.0")


def _store() -> PostgresTableReadStore:
    # The sort helpers never touch self.session; decoding happens before any DB
    # access, so a placeholder session is sufficient for these unit tests.
    return PostgresTableReadStore(None)  # type: ignore[arg-type]


def _records_plan(cursor: str) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        pagination=PaginationParams(cursor=PaginationCursor(value=cursor)),
    )


def _feature_plan(cursor: str) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.FEATURE,
        feature_name="chem_features",
        pagination=PaginationParams(cursor=PaginationCursor(value=cursor)),
        sort=[SortSpec(column="id", direction=SortDirection.ASC)],
    )


def _feature_table():
    fschema = FeatureSchema(columns=[ColumnDef(name="score", json_type="number", required=True)])
    return build_feature_table("chem_features", fschema)


class TestRecordsCursorValidation:
    def test_corrupt_base64_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError) as exc:
            _store()._records_sort(_records_plan("!!not-base64!!"), records_table)
        assert exc.value.field == "cursor"

    def test_missing_keys_raises_validation_error(self) -> None:
        # A well-formed base64 JSON object that lacks the required s/id keys.
        bad = base64.urlsafe_b64encode(json.dumps({"x": 1}).encode()).decode()
        with pytest.raises(ValidationError) as exc:
            _store()._records_sort(_records_plan(bad), records_table)
        assert exc.value.field == "cursor"

    def test_date_metadata_cursor_value_coerced_to_date(self) -> None:
        # A FieldType.DATE metadata column is a sa.Date in the dynamic table;
        # the cursor carries its value as an ISO string — asyncpg rejects a
        # str bound against DATE, so the decoder must coerce by column type.
        value = PostgresTableReadStore._coerce_cursor_value(
            "2026-01-02", sa.Column("assay_date", sa.Date())
        )
        assert isinstance(value, date)

    def test_datetime_metadata_cursor_value_coerced_to_datetime(self) -> None:
        value = PostgresTableReadStore._coerce_cursor_value(
            "2026-01-02T03:04:05+00:00", sa.Column("measured_at", sa.DateTime(timezone=True))
        )
        assert isinstance(value, datetime)


class TestFeatureCursorValidation:
    def test_corrupt_base64_raises_validation_error(self) -> None:
        ft = _feature_table()
        with pytest.raises(ValidationError) as exc:
            _store()._features_sort(_feature_plan("!!not-base64!!"), ft)
        assert exc.value.field == "cursor"

    def test_non_integer_id_raises_validation_error(self) -> None:
        ft = _feature_table()
        # Well-formed cursor whose id component is not an int → int(...) ValueError.
        cursor = encode_cursor(5, "not-an-int")
        with pytest.raises(ValidationError) as exc:
            _store()._features_sort(_feature_plan(cursor), ft)
        assert exc.value.field == "cursor"

    def test_created_at_cursor_value_coerced_to_datetime(self) -> None:
        # created_at is an implicit feature column; the type-driven coercion
        # must turn its ISO string back into a datetime before binding.
        ft = _feature_table()
        value = PostgresTableReadStore._coerce_cursor_value(
            "2026-01-02T03:04:05+00:00", ft.c.created_at
        )
        assert isinstance(value, datetime)

    def test_integer_id_sort_value_coerced_to_int(self) -> None:
        # Sorting by id, the cursor sort value binds against BIGINT — a str
        # is coerced (and garbage raises ValueError → 400 via the sort path).
        ft = _feature_table()
        assert PostgresTableReadStore._coerce_cursor_value("7", ft.c.id) == 7
