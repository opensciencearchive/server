"""Postgres adapter for the ``DataCatalogReadStore`` port.

Catalog, manifest, latest-schema resolution, and single-record-by-id — the
non-streaming reads behind ``GET /data``, ``GET /data/{schema}``, and
``GET /data/records/{id}``. Table streaming lives in
:class:`~osa.infrastructure.data.postgres_table_read_store.PostgresTableReadStore`.
"""

from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.data.model.catalog import (
    CatalogEntry,
    NodeCatalog,
    TableResourceSummary,
)
from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    IMPLICIT_RECORD_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.data.model.skill import AuthorDocs, SampleValue
from osa.domain.semantics.model.value import (
    FieldDefinition,
    FieldType,
    NumberConstraints,
    TermConstraints,
)
from osa.domain.shared.model.ids import RecordId
from osa.domain.shared.model.srn import Domain, RecordSRN, SchemaId
from osa.infrastructure.data.schema_feature_reader import SchemaFeatureReader
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
)
from osa.infrastructure.persistence.tables import (
    conventions_table,
    records_table,
    schemas_table,
)

logger = logging.getLogger(__name__)

# A feature column's JSON-primitive type → the manifest's semantic FieldType.
_JSON_TYPE_TO_FIELD_TYPE: dict[str, FieldType] = {
    "string": FieldType.TEXT,
    "number": FieldType.NUMBER,
    "integer": FieldType.NUMBER,
    "boolean": FieldType.BOOLEAN,
    "array": FieldType.TEXT,
    "object": FieldType.TEXT,
}

# All URL-exposed format suffixes (mirrors the route-layer FORMATS registry).
_ALL_FORMATS = ["", "csv", "csv.gz"]


class PostgresCatalogReadStore:
    def __init__(self, session: AsyncSession, node_domain: Domain) -> None:
        self.session = session
        # Only the node's DNS domain is needed (to render SRNs in the catalog /
        # manifest) — not the whole Config.
        self.node_domain = node_domain
        self._features = SchemaFeatureReader(session)

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    # ------------------------------------------------------------------ #
    # Single record by ID
    # ------------------------------------------------------------------ #

    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary | None:
        # The records PK is the SRN ``urn:osa:{domain}:rec:{id}@{version}``.
        # Match the id segment; resolve version (pin or latest published).
        pattern = f"urn:osa:%:rec:{self._escape_like(str(id))}@%"
        t = records_table
        stmt = (
            select(t.c.srn, t.c.schema_id, t.c.schema_version, t.c.published_at, t.c.metadata)
            .where(t.c.srn.like(pattern, escape="\\"))
            .order_by(t.c.published_at.desc())
        )
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            return None

        chosen = None
        for row in rows:
            srn = RecordSRN.parse(row["srn"])
            if srn.id.root != str(id):
                continue
            if version is None:
                chosen = (srn, row)
                break
            if int(srn.version.root) == version:
                chosen = (srn, row)
                break
        if chosen is None:
            return None
        srn, row = chosen
        return RecordSummary(
            id=RecordId(srn.id.root),
            srn=srn,
            schema_id=SchemaId.parse(f"{row['schema_id']}@{row['schema_version']}"),
            version=int(srn.version.root),
            metadata=row["metadata"] or {},
            created_at=row["published_at"],
        )

    # ------------------------------------------------------------------ #
    # Catalog & manifest
    # ------------------------------------------------------------------ #

    async def get_node_catalog(self) -> NodeCatalog:
        stmt = select(schemas_table.c.id, schemas_table.c.version)
        result = await self.session.execute(stmt)
        schema_rows = [(row["id"], row["version"]) for row in result.mappings()]
        entries: list[CatalogEntry] = []
        for short_id, version in schema_rows:
            schema_id = SchemaId.parse(f"{short_id}@{version}")
            resources = [TableResourceSummary(name="records", kind=TableKind.RECORDS)]
            for hook_name, _ in await self._features.feature_tables(schema_id):
                resources.append(TableResourceSummary(name=hook_name, kind=TableKind.FEATURE))
            entries.append(
                CatalogEntry(
                    id=short_id,
                    version=version,
                    srn=schema_id.to_srn(self.node_domain).render(),
                    table_resources=resources,
                )
            )
        return NodeCatalog(node_domain=self.node_domain.root, schemas=entries)

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest | None:
        stmt = select(schemas_table.c.title, schemas_table.c.fields).where(
            schemas_table.c.id == schema_id.id.root,
            schemas_table.c.version == schema_id.version.root,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None

        field_specs: list[FieldSpec] = []
        column_specs: list[ColumnSpec] = []
        for f in row["fields"]:
            # The blob IS a serialized FieldDefinition — validate it back into
            # the domain model and read typed attributes, never raw dict keys.
            fd = FieldDefinition.model_validate(f)
            ontology_id: str | None = None
            ontology_version: str | None = None
            unit: str | None = None
            if isinstance(fd.constraints, TermConstraints):
                ontology_id = fd.constraints.ontology_srn.id.root
                ontology_version = fd.constraints.ontology_srn.version.root
            elif isinstance(fd.constraints, NumberConstraints):
                # Hoisted for consumers; the constraints union stays internal.
                unit = fd.constraints.unit
            field_specs.append(
                FieldSpec(
                    name=fd.name,
                    type=fd.type,
                    ontology_id=ontology_id,
                    ontology_version=ontology_version,
                    description=fd.description,
                    unit=unit,
                    examples=fd.examples,
                )
            )
            column_specs.append(ColumnSpec(name=fd.name, type=fd.type))

        record_count = await self._records_count(schema_id)
        records_resource = TableResource(
            name="records",
            kind=TableKind.RECORDS,
            # Implicit columns (id, srn, schema_id, version, created_at) precede
            # the schema's declared metadata fields — this is the CSV header order.
            columns=[*IMPLICIT_RECORD_COLUMN_SPECS, *column_specs],
            row_count=record_count,
            formats=list(_ALL_FORMATS),
        )
        feature_resources = await self._feature_resources(schema_id)
        return SchemaManifest(
            id=schema_id.id.root,
            version=schema_id.version.root,
            srn=schema_id.to_srn(self.node_domain).render(),
            title=row["title"],
            fields=field_specs,
            table_resources=[records_resource, *feature_resources],
        )

    async def _feature_resources(self, schema_id: SchemaId) -> list[TableResource]:
        """Build a TableResource for each feature table registered on the schema."""
        resources: list[TableResource] = []
        for hook_name, fschema in await self._features.feature_tables(schema_id):
            ft = build_feature_table(hook_name, fschema)
            count = await self._features.count_rows(ft, schema_id)
            resources.append(
                TableResource(
                    name=hook_name,
                    kind=TableKind.FEATURE,
                    # Implicit columns (id, record_srn, created_at) precede the
                    # hook's declared data columns — this is the CSV header order.
                    columns=[*IMPLICIT_FEATURE_COLUMN_SPECS, *self._feature_column_specs(fschema)],
                    row_count=count,
                    formats=list(_ALL_FORMATS),
                )
            )
        return resources

    # ------------------------------------------------------------------ #
    # Skill surface projections (#151)
    # ------------------------------------------------------------------ #

    async def get_author_docs(self, schema_id: SchemaId) -> AuthorDocs | None:
        """Docs of the schema's owning convention — a read-model projection over
        the deposition-owned ``conventions`` table (latest deploy wins)."""
        stmt = (
            select(conventions_table.c.docs)
            .where(
                conventions_table.c.schema_id == schema_id.id.root,
                conventions_table.c.schema_version == schema_id.version.root,
            )
            .order_by(conventions_table.c.created_at.desc())
            .limit(1)
        )
        result = await self.session.execute(stmt)
        row = result.first()
        if row is None:
            return None
        return AuthorDocs.model_validate(row[0])

    async def sample_value(
        self, schema_id: SchemaId, table: str, column: str
    ) -> SampleValue | None:
        """One non-null value for example templating (research §9).

        Records sampling extracts the JSONB metadata element (bound parameter,
        no identifier interpolation); feature tables go through the existing
        quoted dynamic-table machinery. ``None`` on empty/unknown columns.
        """
        if table == "records":
            t = records_table
            element = t.c.metadata[column]
            stmt = (
                select(element)
                .where(
                    t.c.schema_id == schema_id.id.root,
                    t.c.schema_version == schema_id.version.root,
                    # ->> is SQL NULL for both a missing key and a JSON null.
                    t.c.metadata[column].astext.isnot(None),
                )
                .limit(1)
            )
        else:
            fschema = next(
                (
                    fs
                    for name, fs in await self._features.feature_tables(schema_id)
                    if name == table
                ),
                None,
            )
            if fschema is None:
                return None
            ft = build_feature_table(table, fschema)
            if column not in ft.c:
                return None
            stmt = (
                select(ft.c[column])
                .select_from(ft.join(records_table, records_table.c.srn == ft.c.record_srn))
                .where(
                    records_table.c.schema_id == schema_id.id.root,
                    records_table.c.schema_version == schema_id.version.root,
                    ft.c[column].isnot(None),
                )
                .limit(1)
            )
        result = await self.session.execute(stmt)
        row = result.first()
        if row is None or not isinstance(row[0], (str, int, float, bool)):
            return None
        return SampleValue(value=row[0])

    async def get_latest_schema_id(self, schema_short_id: str) -> SchemaId | None:
        stmt = select(schemas_table.c.version).where(schemas_table.c.id == schema_short_id)
        result = await self.session.execute(stmt)
        versions = [row[0] for row in result.all()]
        if not versions:
            return None
        # Pick the highest SemVer (string sort is wrong for e.g. 1.10.0 vs 1.9.0).
        latest = max(versions, key=lambda v: tuple(int(p) for p in v.split("-")[0].split(".")))
        return SchemaId.parse(f"{schema_short_id}@{latest}")

    async def _records_count(self, schema_id: SchemaId) -> int:
        t = records_table
        stmt = (
            select(func.count())
            .select_from(t)
            .where(
                t.c.schema_id == schema_id.id.root,
                t.c.schema_version == schema_id.version.root,
            )
        )
        return int((await self.session.execute(stmt)).scalar_one())

    @staticmethod
    def _feature_column_specs(fschema: FeatureSchema) -> list[ColumnSpec]:
        """Map a feature table's declared columns to manifest ColumnSpecs."""
        return [
            ColumnSpec(
                name=c.name,
                type=_JSON_TYPE_TO_FIELD_TYPE[c.json_type],
                format=c.format,
                description=c.description,
                unit=c.unit,
            )
            for c in fschema.columns
        ]
