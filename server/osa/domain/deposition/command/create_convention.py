from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import (
    HookDeploySpec,
    HookIdentity,
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionId, SchemaId, SchemaIdentifier


class SchemaBlock(BaseModel):
    """The deploy's nested ``schema`` sub-structure (== POST /schemas body)."""

    id: SchemaIdentifier
    version: str
    fields: list[FieldDefinition] = []


class ReleaseInput(BaseModel):
    """A hook's release block (== POST /hooks/{name}/releases body)."""

    image: str
    digest: str
    config: dict = Field(default_factory=dict)
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str  # REQUIRED — reproducibility anchor (FR-005)


class HookDeployEntry(BaseModel):
    """One hook in the bundled deploy: identity (name + fixed feature) + release."""

    name: HookName
    feature: TableFeatureSpec
    release: ReleaseInput

    def to_spec(self) -> HookDeploySpec:
        return HookDeploySpec(
            identity=HookIdentity(name=self.name, feature=self.feature),
            runtime=OciConfig(
                image=self.release.image,
                digest=self.release.digest,
                config=self.release.config,
                limits=self.release.limits,
            ),
            source_ref=self.release.source_ref,
        )


class DeployConvention(Command):
    """Bundled deploy: schema + hooks (+ first releases) + convention, atomically."""

    model_config = ConfigDict(populate_by_name=True)

    id: SchemaIdentifier
    """Convention slug — combines with ``version`` into ``ConventionId``."""

    version: str
    title: str
    description: str | None = None
    file_requirements: FileRequirements
    schema_block: SchemaBlock = Field(alias="schema")
    hooks: list[HookDeployEntry] = []
    ingester: IngesterDefinition | None = None


class ConventionCreated(Result):
    id: ConventionId
    title: str
    description: str | None
    schema_id: SchemaId
    hooks: list[str]
    created_at: datetime


class DeployConventionHandler(CommandHandler[DeployConvention, ConventionCreated]):
    # Conventions are curated registry artifacts; deploy is an admin/automation
    # operation. US5 narrows this from admin-only to the ``conventions:write`` scope.
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    convention_service: ConventionService

    async def run(self, cmd: DeployConvention) -> ConventionCreated:
        built_by = str(self.principal.user_id) if self.principal.user_id else None
        convention = await self.convention_service.deploy(
            convention_slug=cmd.id,
            version=cmd.version,
            title=cmd.title,
            description=cmd.description,
            file_requirements=cmd.file_requirements,
            schema_slug=cmd.schema_block.id,
            schema_version=cmd.schema_block.version,
            schema_fields=cmd.schema_block.fields,
            hooks=[h.to_spec() for h in cmd.hooks],
            ingester=cmd.ingester,
            built_by=built_by,
        )
        return ConventionCreated(
            id=convention.id,
            title=convention.title,
            description=convention.description,
            schema_id=convention.schema_id,
            hooks=list(convention.hooks),
            created_at=convention.created_at,
        )
