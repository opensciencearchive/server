"""PostgresFeatureReader — reads feature data for record enrichment."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import quoted_name

from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.tables import feature_tables_table


class PostgresFeatureReader:
    """Queries feature_tables catalog and dynamic feature tables for a record."""

    AUTO_COLUMNS = {"id", "created_at"}

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_features_for_record(
        self, record_srn: RecordSRN
    ) -> dict[str, list[dict[str, Any]]]:
        # Get all feature tables from catalog
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.pg_table,
        )
        result = await self.session.execute(stmt)
        catalog_rows = result.mappings().all()

        if not catalog_rows:
            return {}

        features: dict[str, list[dict[str, Any]]] = {}
        for row in catalog_rows:
            hook_name = row["hook_name"]
            pg_table = row["pg_table"]

            # Query the dynamic feature table for this record
            safe_table = quoted_name(pg_table, quote=True)
            query = text(
                f"SELECT * FROM features.{safe_table} WHERE record_srn = :srn"  # noqa: S608
            )
            feat_result = await self.session.execute(query, {"srn": str(record_srn)})
            feat_rows = feat_result.mappings().all()

            if feat_rows:
                rows_list: list[dict[str, Any]] = []
                for feat_row in feat_rows:
                    row_dict = {
                        k: v
                        for k, v in dict(feat_row).items()
                        if k not in self.AUTO_COLUMNS and k != "record_srn"
                    }
                    rows_list.append(row_dict)
                features[hook_name] = rows_list

        return features
