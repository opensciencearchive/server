"""Shared schema→feature-table resolution used by both /data/ read adapters.

A ``features.<hook>`` table is global (UNIQUE(hook_name)) and shared by every
convention that registers the hook name, across schemas. These helpers answer
"which feature tables belong to this schema" (via its conventions) and provide
the records-join condition that scopes feature rows to one schema's records.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.persistence.feature_table import FeatureSchema
from osa.infrastructure.persistence.tables import (
    conventions_table,
    feature_tables_table,
    records_table,
)


async def schema_hook_names(session: AsyncSession, schema_id: SchemaId) -> set[str]:
    """Hook names registered on the schema's conventions (the schema→feature link)."""
    stmt = select(conventions_table.c.hooks).where(
        conventions_table.c.schema_id == schema_id.id.root,
        conventions_table.c.schema_version == schema_id.version.root,
    )
    result = await session.execute(stmt)
    names: set[str] = set()
    for (hooks,) in result.all():
        for hook in hooks or []:
            names.add(hook["name"])
    return names


async def schema_feature_tables(
    session: AsyncSession, schema_id: SchemaId
) -> list[tuple[str, FeatureSchema]]:
    """(hook_name, FeatureSchema) for every materialized feature table on the schema."""
    hook_names = await schema_hook_names(session, schema_id)
    if not hook_names:
        return []
    stmt = select(feature_tables_table.c.hook_name, feature_tables_table.c.feature_schema).where(
        feature_tables_table.c.hook_name.in_(hook_names)
    )
    result = await session.execute(stmt)
    return [
        (row["hook_name"], FeatureSchema.model_validate(row["feature_schema"]))
        for row in result.mappings()
    ]


def feature_schema_scope(schema_id: SchemaId) -> list[Any]:
    """Records-join conditions scoping shared feature tables to one schema."""
    return [
        records_table.c.schema_id == schema_id.id.root,
        records_table.c.schema_version == schema_id.version.root,
    ]


def feature_count_stmt(ft: sa.Table, schema_id: SchemaId) -> sa.Select[Any]:
    """COUNT of a feature table's rows scoped to the schema's records."""
    return (
        select(sa.func.count())
        .select_from(ft.join(records_table, records_table.c.srn == ft.c.record_srn))
        .where(sa.and_(*feature_schema_scope(schema_id)))
    )
