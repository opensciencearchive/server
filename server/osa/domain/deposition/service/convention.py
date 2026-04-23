from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import (
    ConventionSRN,
    Domain,
    LocalId,
    SchemaIdentifier,
    Semver,
)
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service


class ConventionService(Service):
    convention_repo: ConventionRepository
    schema_service: SchemaService
    metadata_service: MetadataService
    outbox: Outbox
    node_domain: Domain

    async def create_convention(
        self,
        id: SchemaIdentifier,
        title: str,
        version: str,
        schema: list[FieldDefinition],
        file_requirements: FileRequirements,
        description: str | None = None,
        hooks: list[HookDefinition] | None = None,
        ingester: IngesterDefinition | None = None,
    ) -> Convention:
        """Create a convention with an inline schema.

        The schema is created as a separate Schema row internally,
        and the convention references it via schema_id.

        Feature table creation is handled asynchronously by the
        CreateFeatureTables handler reacting to ConventionRegistered.
        """
        # Create Schema row from inline field definitions
        created_schema = await self.schema_service.create_schema(
            id=id,
            title=title,
            version=version,
            fields=schema,
        )

        # Create (or additively evolve) the typed metadata table in the same
        # transaction — no async window where records can publish against a
        # convention whose typed table doesn't exist yet.
        await self.metadata_service.ensure_table(
            schema_id=created_schema.id,
            fields=created_schema.fields,
        )

        srn = ConventionSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())[:20]),
            version=Semver.from_string(version),
        )
        convention = Convention(
            srn=srn,
            title=title,
            description=description,
            schema_id=created_schema.id,
            file_requirements=file_requirements,
            hooks=hooks or [],
            ingester=ingester,
            created_at=datetime.now(UTC),
        )

        await self.convention_repo.save(convention)
        await self.outbox.append(
            ConventionRegistered(
                id=EventId(uuid4()),
                convention_srn=srn,
                schema_id=created_schema.id,
                schema_fields=created_schema.fields,
                hooks=convention.hooks,
            )
        )
        return convention

    async def get_convention(self, srn: ConventionSRN) -> Convention:
        convention = await self.convention_repo.get(srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {srn}")
        return convention

    async def list_conventions(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> list[Convention]:
        return await self.convention_repo.list(limit=limit, offset=offset)

    async def list_conventions_with_source(self) -> list[Convention]:
        """Return conventions that have a source configured."""
        return await self.convention_repo.list_with_source()
