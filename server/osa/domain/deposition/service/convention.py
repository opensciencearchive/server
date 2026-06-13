from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookDeploySpec
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import (
    ConventionId,
    Domain,
    LocalId,
    SchemaId,
    SchemaIdentifier,
    Semver,
)
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service
from osa.domain.validation.service.hook_registry import HookRegistryService


class ConventionService(Service):
    convention_repo: ConventionRepository
    schema_service: SchemaService  # TODO: replace with a port?
    metadata_service: MetadataService  # TODO: replace with a port?
    hook_registry: HookRegistryService
    outbox: Outbox
    node_domain: Domain

    async def deploy(
        self,
        *,
        convention_slug: SchemaIdentifier,
        version: str,
        title: str,
        file_requirements: FileRequirements,
        schema_slug: SchemaIdentifier,
        schema_version: str,
        schema_fields: list[FieldDefinition],
        hooks: list[HookDeploySpec] | None = None,
        ingester: IngesterDefinition | None = None,
        description: str | None = None,
        built_by: str | None = None,
    ) -> Convention:
        """Bundled deploy: create schema + hooks (+ releases) + convention in one
        transaction (FR-012). Fans out into the schema, metadata, and hook
        registries, then creates the convention referencing hooks by name.

        Idempotent on ``convention_slug@version``: an identical re-deploy returns
        the existing convention; a differing one raises ``ConflictError`` (FR-018).
        Feature-table creation is handled asynchronously by ``CreateFeatureTables``
        reacting to ``ConventionRegistered``.
        """
        hooks = hooks or []
        convention_id = ConventionId(
            id=LocalId(convention_slug.root),
            version=Semver.from_string(version),
        )

        existing = await self.convention_repo.get(convention_id)
        if existing is not None:
            if self._is_same_deploy(existing, schema_slug, schema_version, hooks):
                return existing  # idempotent no-op (FR-018)
            raise ConflictError(
                f"Convention {convention_id.render()} already exists with different content"
            )

        # 1) Schema (+ typed metadata table) — same transaction, no async gap.
        created_schema = await self.schema_service.create_schema(
            id=schema_slug,
            title=title,
            version=schema_version,
            fields=schema_fields,
        )
        await self.metadata_service.ensure_table(
            schema_id=created_schema.id,
            fields=created_schema.fields,
        )

        # 2) Hooks: upsert each identity (reject a differing contract) + mint its
        #    release (advancing the live pointer).
        for spec in hooks:
            await self.hook_registry.upsert_identity(spec.identity.name, spec.identity.feature)
            await self.hook_registry.create_release(
                spec.identity.name, spec.runtime, spec.source_ref, built_by
            )

        # 3) Convention referencing hooks by name.
        convention = Convention(
            id=convention_id,
            title=title,
            description=description,
            schema_id=created_schema.id,
            file_requirements=file_requirements,
            hooks=[spec.identity.name for spec in hooks],
            ingester=ingester,
            created_at=datetime.now(UTC),
        )
        await self.convention_repo.save(convention)

        # 4) Async feature-table creation reacts to this (hook names + specs).
        await self.outbox.append(
            ConventionRegistered(
                id=EventId(uuid4()),
                convention_id=convention_id,
                schema_id=created_schema.id,
                schema_fields=created_schema.fields,
                hooks=[spec.identity for spec in hooks],
            )
        )
        return convention

    @staticmethod
    def _is_same_deploy(
        existing: Convention,
        schema_slug: SchemaIdentifier,
        schema_version: str,
        hooks: list[HookDeploySpec],
    ) -> bool:
        same_schema = existing.schema_id == SchemaId(
            id=LocalId(schema_slug.root), version=Semver.from_string(schema_version)
        )
        same_hooks = list(existing.hooks) == [spec.identity.name for spec in hooks]
        return same_schema and same_hooks

    async def get_convention(self, id: ConventionId) -> Convention:
        convention = await self.convention_repo.get(id)
        if convention is None:
            raise NotFoundError(f"Convention not found: {id.render()}")
        return convention

    async def list_conventions(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[Convention]:
        return await self.convention_repo.list(limit=limit, offset=offset)

    async def list_conventions_with_source(self) -> list[Convention]:
        """Return conventions that have a source configured."""
        return await self.convention_repo.list_with_source()
