"""PostgresFeatureReader — reads feature data for record enrichment."""

from __future__ import annotations

from typing import Any

from sqlalchemy import String, func, literal, select, type_coerce, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
    data_columns,
)
from osa.infrastructure.persistence.tables import feature_tables_table


class PostgresFeatureReader:
    """Queries feature_tables catalog and dynamic feature tables for a record."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_features_for_record(
        self, record_srn: RecordSRN
    ) -> dict[str, list[dict[str, Any]]]:
        # Get all feature tables from catalog
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        )
        result = await self.session.execute(stmt)
        catalog_rows = result.mappings().all()

        if not catalog_rows:
            return {}

        # Build a single UNION ALL query across all feature tables (avoid N+1).
        # Use jsonb_build_object with explicit data columns to exclude auto columns
        # at the SQL level.
        parts = []
        for row in catalog_rows:
            schema = FeatureSchema.model_validate(row["feature_schema"])
            ft = build_feature_table(row["pg_table"], schema)
            dcols = data_columns(ft)

            # Build jsonb_build_object('col1', col1, 'col2', col2, ...)
            jsonb_args: list[Any] = []
            for col in dcols:
                jsonb_args.extend([type_coerce(literal(col.key), String), col])

            row_data_expr = (
                func.jsonb_build_object(*jsonb_args) if jsonb_args else func.jsonb_build_object()
            )

            parts.append(
                select(
                    literal(row["hook_name"]).label("hook_name"),
                    row_data_expr.label("row_data"),
                )
                .select_from(ft)
                .where(ft.c.record_srn == str(record_srn))
            )

        combined = union_all(*parts)
        feat_result = await self.session.execute(combined)

        features: dict[str, list[dict[str, Any]]] = {}
        for feat_row in feat_result.mappings():
            hook_name: str = feat_row["hook_name"]
            row_data: dict[str, Any] = feat_row["row_data"]
            features.setdefault(hook_name, []).append(row_data)

        return features
