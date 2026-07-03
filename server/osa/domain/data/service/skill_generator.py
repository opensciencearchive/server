"""SkillGeneratorService — assembles the skill-surface documents (#151).

Composes ``Config`` (node identity), the catalog/manifest reads (live row
counts — FR-005, no cache), author docs, and value samples, and feeds the
pure :class:`SkillRenderer`. All I/O goes through the data read-store port.
"""

from __future__ import annotations

from osa.config import Config
from osa.domain.data.model.skill import (
    DatasetEntry,
    NodeIdentity,
    RootDiscovery,
    SampleValue,
)
from osa.domain.data.port.data_read_store import DataCatalogReadStore
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.skill_renderer import SkillRenderer
from osa.domain.shared.service import Service


class SkillGeneratorService(Service):
    catalog_service: DataCatalogService
    read_store: DataCatalogReadStore
    renderer: SkillRenderer
    config: Config

    def _node_identity(self) -> NodeIdentity:
        return NodeIdentity(
            name=self.config.name,
            domain=self.config.domain,
            description=self.config.description,
            osa_version=self.config.version,
        )

    def _base_url(self) -> str:
        return self.config.base_url.rstrip("/")

    async def root_discovery(self, openapi_path: str) -> RootDiscovery:
        """The ``GET /`` document. ``openapi_path`` is the app's actual
        configured OpenAPI path (research §7) — passed in by the route."""
        base = self._base_url()
        catalog = await self.catalog_service.get_node_catalog()
        return RootDiscovery(
            node=self._node_identity(),
            skill_url=f"{base}/SKILL.md",
            reference_base=f"{base}/api/v1/data/",
            data_url=f"{base}/api/v1/data",
            openapi_url=f"{base}{openapi_path}",
            schemas=[f"{entry.id}@{entry.version}" for entry in catalog.schemas],
        )

    async def skill_document(self) -> str:
        """Render ``SKILL.md`` from the live catalog (FR-005)."""
        catalog = await self.catalog_service.get_node_catalog()
        datasets: list[DatasetEntry] = []
        docs = []
        for entry in catalog.schemas:
            schema_id = await self.catalog_service.resolve_schema(f"{entry.id}@{entry.version}")
            manifest = await self.catalog_service.get_schema_manifest(schema_id)
            records = next(t for t in manifest.table_resources if t.name == "records")
            datasets.append(
                DatasetEntry(
                    schema_id=entry.id,
                    schema_ref=f"{entry.id}@{entry.version}",
                    title=manifest.title,
                    row_count=records.row_count,
                )
            )
            author_docs = await self.read_store.get_author_docs(entry.id, entry.version)
            if author_docs is not None:
                docs.append(author_docs)
        return self.renderer.render_skill(
            node=self._node_identity(),
            base_url=self._base_url(),
            datasets=datasets,
            docs=docs,
        )

    async def schema_reference(self, schema: str) -> str:
        """Render the reference doc for one schema (markdown representation of
        the schema resource). Unknown/reserved ids 404 via ``resolve_schema``."""
        schema_id = await self.catalog_service.resolve_schema(schema)
        manifest = await self.catalog_service.get_schema_manifest(schema_id)
        docs = await self.read_store.get_author_docs(schema_id.id.root, schema_id.version.root)
        sample_field = self.renderer.filter_example_field(manifest)
        sample: SampleValue | None = None
        if sample_field is not None:
            sample = await self.read_store.sample_value(schema_id, "records", sample_field)
        return self.renderer.render_reference(
            manifest=manifest,
            docs=docs,
            base_url=self._base_url(),
            sample_field=sample_field,
            sample=sample,
        )
