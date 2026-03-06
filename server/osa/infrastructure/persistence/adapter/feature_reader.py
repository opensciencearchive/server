"""PostgresFeatureReader — reads feature data for record enrichment."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.tables import feature_tables_table


def _quote_ident(name: str) -> str:
    """Double-quote a SQL identifier, escaping embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


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

        # Build a single UNION ALL query across all feature tables (avoid N+1).
        # to_jsonb serialises each heterogeneous row into a uniform shape.
        parts: list[str] = []
        params: dict[str, Any] = {"srn": str(record_srn)}
        for i, row in enumerate(catalog_rows):
            quoted = _quote_ident(row["pg_table"])
            hook_param = f"hook_{i}"
            params[hook_param] = row["hook_name"]
            parts.append(  # noqa: S608
                f"SELECT :{hook_param} AS hook_name, to_jsonb(t) AS row_data "
                f"FROM features.{quoted} t "
                f"WHERE t.record_srn = :srn"
            )
        combined = text(" UNION ALL ".join(parts))
        feat_result = await self.session.execute(combined, params)

        features: dict[str, list[dict[str, Any]]] = {}
        for feat_row in feat_result.mappings():
            hook_name: str = feat_row["hook_name"]
            row_data: dict[str, Any] = feat_row["row_data"]
            filtered = {
                k: v
                for k, v in row_data.items()
                if k not in self.AUTO_COLUMNS and k != "record_srn"
            }
            features.setdefault(hook_name, []).append(filtered)

        return features
