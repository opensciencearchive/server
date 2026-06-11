"""Reads which feature tables belong to a schema (via its conventions).

A ``features.<hook>`` table is global (UNIQUE(hook_name)) and shared by every
convention that registers the hook name, across schemas. This reader answers
"which feature tables does this schema expose" (through its conventions) and
counts a feature table's rows scoped to one schema's records. Composed by both
``/data/`` read adapters (table streaming + catalog/manifest).
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.persistence.feature_table import FeatureSchema
from osa.infrastructure.persistence.tables import (
    conventions_table,
    feature_tables_table,
    records_table,
)


class SchemaFeatureReader:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def feature_tables(self, schema_id: SchemaId) -> list[tuple[str, FeatureSchema]]:
        """(hook_name, FeatureSchema) for every materialized feature table on the schema."""
        hook_names = await self._hook_names(schema_id)
        if not hook_names:
            return []
        stmt = select(
            feature_tables_table.c.hook_name, feature_tables_table.c.feature_schema
        ).where(feature_tables_table.c.hook_name.in_(hook_names))
        result = await self.session.execute(stmt)
        return [
            (row["hook_name"], FeatureSchema.model_validate(row["feature_schema"]))
            for row in result.mappings()
        ]

    async def count_rows(self, ft: sa.Table, schema_id: SchemaId) -> int:
        """Row count of a feature table scoped to the schema's records."""
        stmt = (
            select(func.count())
            .select_from(ft.join(records_table, records_table.c.srn == ft.c.record_srn))
            .where(and_(*self.records_scope(schema_id)))
        )
        return int((await self.session.execute(stmt)).scalar_one())

    @staticmethod
    def records_scope(schema_id: SchemaId) -> list[Any]:
        """Records-join conditions scoping a shared feature table to one schema."""
        return [
            records_table.c.schema_id == schema_id.id.root,
            records_table.c.schema_version == schema_id.version.root,
        ]

    async def _hook_names(self, schema_id: SchemaId) -> set[str]:
        """Hook names registered on the schema's conventions (the schema→feature link)."""
        stmt = select(conventions_table.c.hooks).where(
            conventions_table.c.schema_id == schema_id.id.root,
            conventions_table.c.schema_version == schema_id.version.root,
        )
        result = await self.session.execute(stmt)
        names: set[str] = set()
        for (hooks,) in result.all():
            for hook in hooks or []:
                names.add(hook["name"])
        return names
