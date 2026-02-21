from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.feature.service.feature import FeatureService
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import ConventionSRN, Domain, LocalId, SchemaSRN, Semver
from osa.domain.shared.service import Service


class ConventionService(Service):
    convention_repo: ConventionRepository
    schema_reader: SchemaReader
    feature_service: FeatureService
    node_domain: Domain

    async def create_convention(
        self,
        title: str,
        version: str,
        schema_srn: SchemaSRN,
        file_requirements: FileRequirements,
        description: str | None = None,
        hooks: list[HookDefinition] | None = None,
    ) -> Convention:
        if not await self.schema_reader.schema_exists(schema_srn):
            raise ValidationError(f"Schema '{schema_srn}' not found")

        srn = ConventionSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid4())[:20]),
            version=Semver.from_string(version),
        )
        convention = Convention(
            srn=srn,
            title=title,
            description=description,
            schema_srn=schema_srn,
            file_requirements=file_requirements,
            hooks=hooks or [],
            created_at=datetime.now(UTC),
        )
        # Create feature tables BEFORE persisting convention row.
        # If DDL fails, no orphaned convention row is left behind.
        if convention.hooks:
            convention_id = str(convention.srn.id)
            await self.feature_service.create_tables(convention_id, convention.hooks)

        await self.convention_repo.save(convention)
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
