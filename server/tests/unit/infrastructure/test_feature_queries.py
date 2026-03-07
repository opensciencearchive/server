"""Tests for feature table SQL generation — verifies tables support SQL JOINs and typed queries.

DDL correctness tests live in test_feature_table.py. These tests verify that
SQLAlchemy can generate valid SELECT/WHERE/JOIN expressions from the built tables.
"""

import sqlalchemy as sa

from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.feature_table import (
    FEATURES_SCHEMA,
    FeatureSchema,
    build_feature_table,
)


def _schema(*col_defs: tuple[str, str, bool]) -> FeatureSchema:
    """Create a FeatureSchema from (name, json_type, required) tuples."""
    return FeatureSchema(
        columns=[ColumnDef(name=n, json_type=t, required=r) for n, t, r in col_defs]
    )


class TestFeatureTableSQLGeneration:
    """Verify that SQLAlchemy can generate valid SELECT/WHERE/JOIN expressions."""

    def test_where_on_typed_column(self):
        """Can build WHERE score > 0.5 on a number column."""
        table = build_feature_table("detect", _schema(("score", "number", True)))

        stmt = sa.select(table.c.record_srn).where(table.c.score > 0.5)
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        assert "score > 0.5" in compiled
        assert "record_srn" in compiled

    def test_join_on_record_srn(self):
        """Can JOIN feature table with records table on record_srn."""
        feature_table = build_feature_table("detect", _schema(("score", "number", True)))

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
        table = build_feature_table(
            "detect", _schema(("score", "number", True), ("volume", "number", True))
        )

        stmt = sa.select(
            table.c.record_srn,
            sa.func.avg(table.c.score).label("avg_score"),
            sa.func.count().label("pocket_count"),
        ).group_by(table.c.record_srn)

        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "avg" in compiled.lower()
        assert "GROUP BY" in compiled
