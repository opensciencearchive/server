from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.deploy import HookDeploy
from osa.domain.deposition.model.docs import ConventionDocs
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import (
    ConventionSlug,
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
        slug: ConventionSlug,
        title: str,
        file_requirements: FileRequirements,
        schema_slug: SchemaIdentifier,
        schema_version: str,
        schema_fields: list[FieldDefinition],
        description: str,
        docs: ConventionDocs,
        hooks: list[HookDeploy] | None = None,
        ingester: IngesterDefinition | None = None,
        built_by: str | None = None,
    ) -> Convention:
        """Bundled deploy: schema + hooks (+ releases) + convention in one
        transaction (FR-012). Fans out into the schema, metadata, and hook
        registries, then upserts the convention referencing hooks by name.

        Declarative upsert keyed by ``slug`` (design-revisions §3): conventions
        are unversioned and mutable, so re-declaring the same state is a no-op and
        a differing declaration updates the convention in place — no caller version,
        no conflict path. The schema (versioned, immutable) and each hook release
        (idempotent on digest) are reused when already present. Feature-table
        creation is handled asynchronously by ``CreateFeatureTables`` reacting to
        ``ConventionRegistered``.
        """
        hooks = hooks or []

        # 1) Schema (+ typed metadata table) — same transaction, no async gap.
        #    Schemas are versioned/immutable: reuse the existing one if this exact
        #    id@version is already registered (idempotent re-deploy).
        schema_id = SchemaId(
            id=LocalId(schema_slug.root), version=Semver.from_string(schema_version)
        )
        existing_schema = await self._existing_schema(schema_id)
        if existing_schema is not None:
            created_schema = existing_schema
        else:
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
        #    release (idempotent on digest, advancing the live pointer).
        for spec in hooks:
            await self.hook_registry.upsert_identity(spec.identity.name, spec.identity.feature)
            await self.hook_registry.create_release(
                spec.identity.name, spec.runtime, spec.source_ref, built_by
            )

        # 3) Convention referencing hooks by name (upsert by slug).
        convention = Convention(
            id=slug,
            title=title,
            description=description,
            schema_id=created_schema.id,
            file_requirements=file_requirements,
            hooks=[spec.identity.name for spec in hooks],
            ingester=ingester,
            docs=docs,
            created_at=datetime.now(UTC),
        )
        await self.convention_repo.save(convention)

        # 4) Async feature-table creation reacts to this (hook names + specs).
        await self.outbox.append(
            ConventionRegistered(
                id=EventId(uuid4()),
                convention_id=slug,
                schema_id=created_schema.id,
                schema_fields=created_schema.fields,
                hooks=[spec.identity for spec in hooks],
            )
        )
        return convention

    async def _existing_schema(self, schema_id: SchemaId):
        """Return the schema if already registered, else ``None`` (idempotency)."""
        try:
            return await self.schema_service.get_schema(schema_id)
        except NotFoundError:
            return None

    async def get_convention(self, id: ConventionSlug) -> Convention:
        convention = await self.convention_repo.get(id)
        if convention is None:
            raise NotFoundError(f"Convention not found: {id.root}")
        return convention

    async def list_conventions(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[Convention]:
        return await self.convention_repo.list(limit=limit, offset=offset)

    async def list_conventions_with_source(self) -> list[Convention]:
        """Return conventions that have a source configured."""
        return await self.convention_repo.list_with_source()
