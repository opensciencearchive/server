from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.feature.service.feature import FeatureService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, Domain, LocalId, Semver
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service


class ConventionService(Service):
    convention_repo: ConventionRepository
    schema_service: SchemaService
    feature_service: FeatureService
    outbox: Outbox
    node_domain: Domain

    async def create_convention(
        self,
        title: str,
        version: str,
        schema: list[FieldDefinition],
        file_requirements: FileRequirements,
        description: str | None = None,
        hooks: list[HookDefinition] | None = None,
        source: SourceDefinition | None = None,
    ) -> Convention:
        """Create a convention with an inline schema.

        The schema is created as a separate Schema row internally,
        and the convention references it via schema_srn.
        """
        # Create Schema row from inline field definitions
        created_schema = await self.schema_service.create_schema(
            title=title,
            version=version,
            fields=schema,
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
            schema_srn=created_schema.srn,
            file_requirements=file_requirements,
            hooks=hooks or [],
            source=source,
            created_at=datetime.now(UTC),
        )
        # Create feature tables BEFORE persisting convention row.
        # If DDL fails, no orphaned convention row is left behind.
        for hook_def in convention.hooks:
            await self.feature_service.create_table(hook_def)

        await self.convention_repo.save(convention)
        await self.outbox.append(
            ConventionRegistered(
                id=EventId(uuid4()),
                convention_srn=srn,
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
