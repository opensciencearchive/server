"""Tests for feature table DDL correctness — verifies tables support SQL JOINs and typed queries.

These tests validate that the dynamically generated SQLAlchemy table metadata
produces correct DDL: proper column types, nullable constraints, record_srn FK
column for JOINs, and a single ``features`` PG schema.
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.feature_store import FEATURES_SCHEMA


def _make_columns(*col_defs: tuple[str, str, bool]) -> list[ColumnDef]:
    """Create a list of ColumnDef from (name, json_type, required) tuples."""
    return [ColumnDef(name=n, json_type=t, required=r) for n, t, r in col_defs]


def _build_feature_table(table_name: str, columns: list[ColumnDef]) -> sa.Table:
    """Build a dynamic feature table exactly as PostgresFeatureStore does."""
    metadata = sa.MetaData(schema=FEATURES_SCHEMA)
    sa_columns: list[sa.Column] = [
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("record_srn", sa.Text, nullable=False, index=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    ]
    for col_def in columns:
        sa_columns.append(map_column(col_def))

    return sa.Table(table_name, metadata, *sa_columns)


class TestFeatureTableDDL:
    def test_record_srn_column_exists(self):
        """Feature tables must have record_srn for JOINing with records table."""
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("pocket_detect", columns)

        assert "record_srn" in table.c
        assert not table.c.record_srn.nullable

    def test_record_srn_is_indexed(self):
        """record_srn must be indexed for efficient JOINs."""
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("pocket_detect", columns)

        assert table.c.record_srn.index is True

    def test_number_columns_are_float(self):
        """Number columns map to Float(53) for double-precision queries."""
        columns = _make_columns(("score", "number", True), ("volume", "number", False))
        table = _build_feature_table("detect", columns)

        assert isinstance(table.c.score.type, sa.Float)
        assert isinstance(table.c.volume.type, sa.Float)

    def test_integer_columns_are_bigint(self):
        columns = _make_columns(("n_atoms", "integer", True))
        table = _build_feature_table("check", columns)

        assert isinstance(table.c.n_atoms.type, sa.BigInteger)

    def test_string_columns_are_text(self):
        columns = _make_columns(("pocket_id", "string", True))
        table = _build_feature_table("detect", columns)

        assert isinstance(table.c.pocket_id.type, sa.Text)

    def test_boolean_columns_are_boolean(self):
        columns = _make_columns(("is_valid", "boolean", True))
        table = _build_feature_table("check", columns)

        assert isinstance(table.c.is_valid.type, sa.Boolean)

    def test_array_columns_are_jsonb(self):
        columns = _make_columns(("residues", "array", True))
        table = _build_feature_table("detect", columns)

        assert isinstance(table.c.residues.type, JSONB)

    def test_object_columns_are_jsonb(self):
        columns = _make_columns(("metadata", "object", False))
        table = _build_feature_table("detect", columns)

        assert isinstance(table.c.metadata.type, JSONB)

    def test_nullable_respects_required_field(self):
        columns = _make_columns(
            ("score", "number", True),
            ("notes", "string", False),
        )
        table = _build_feature_table("detect", columns)

        assert not table.c.score.nullable  # required -> NOT NULL
        assert table.c.notes.nullable  # optional -> nullable

    def test_has_primary_key(self):
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("detect", columns)

        pk_cols = [c.name for c in table.primary_key.columns]
        assert pk_cols == ["id"]

    def test_has_created_at(self):
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("detect", columns)

        assert "created_at" in table.c
        assert isinstance(table.c.created_at.type, sa.DateTime)

    def test_table_uses_features_schema(self):
        """All feature tables live in the single 'features' PG schema."""
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("detect", columns)

        assert table.schema == FEATURES_SCHEMA


class TestFeatureTableSQLGeneration:
    """Verify that SQLAlchemy can generate valid SELECT/WHERE/JOIN expressions."""

    def test_where_on_typed_column(self):
        """Can build WHERE score > 0.5 on a number column."""
        columns = _make_columns(("score", "number", True))
        table = _build_feature_table("detect", columns)

        stmt = sa.select(table.c.record_srn).where(table.c.score > 0.5)
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        assert "score > 0.5" in compiled
        assert "record_srn" in compiled

    def test_join_on_record_srn(self):
        """Can JOIN feature table with records table on record_srn."""
        columns = _make_columns(("score", "number", True))
        feature_table = _build_feature_table("detect", columns)

        # Simulate a records table
        records_meta = sa.MetaData()
        records_table = sa.Table(
            "records",
            records_meta,
            sa.Column("srn", sa.Text, primary_key=True),
            sa.Column("title", sa.Text),
        )

        stmt = (
            sa.select(records_table.c.title, feature_table.c.score)
            .select_from(
                records_table.join(
                    feature_table,
                    records_table.c.srn == feature_table.c.record_srn,
                )
            )
            .where(feature_table.c.score > 0.8)
        )

        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "JOIN" in compiled
        assert f"records.srn = {FEATURES_SCHEMA}.detect.record_srn" in compiled
        assert "score > 0.8" in compiled

    def test_aggregate_on_typed_columns(self):
        """Can compute aggregates (AVG, COUNT) on typed columns."""
        columns = _make_columns(("score", "number", True), ("volume", "number", True))
        table = _build_feature_table("detect", columns)

        stmt = sa.select(
            table.c.record_srn,
            sa.func.avg(table.c.score).label("avg_score"),
            sa.func.count().label("pocket_count"),
        ).group_by(table.c.record_srn)

        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "avg" in compiled.lower()
        assert "GROUP BY" in compiled
