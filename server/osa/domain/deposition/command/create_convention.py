from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from osa.domain.auth.model.principal import Principal
from osa.domain.deposition.model.deploy import HookDeploy
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import (
    HookIdentity,
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSlug, SchemaId, SchemaIdentifier


class DeployConventionSchema(BaseModel):
    """The deploy's nested ``schema`` sub-structure (== POST /schemas body)."""

    id: SchemaIdentifier
    version: str
    fields: list[FieldDefinition] = []


class DeployConventionRelease(BaseModel):
    """A hook's release block (== POST /hooks/{name}/releases body)."""

    image: str
    digest: str
    config: dict = Field(default_factory=dict)
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str  # REQUIRED — reproducibility anchor (FR-005)


class DeployConventionHook(BaseModel):
    """One hook in the bundled deploy: identity (name + fixed feature) + release."""

    name: HookName
    feature: TableFeatureSpec
    release: DeployConventionRelease

    def to_deploy(self) -> HookDeploy:
        return HookDeploy(
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
    """Bundled deploy: schema + hooks (+ first releases) + convention, atomically.

    Conventions are unversioned and mutable (design-revisions §3): deploy is a
    declarative upsert keyed by ``slug`` — re-declaring the same state is a no-op,
    a different declaration updates the convention in place. No caller-supplied
    version, no conflict path.
    """

    model_config = ConfigDict(populate_by_name=True)

    slug: ConventionSlug
    """The convention's identity — a bare, unversioned slug."""

    title: str
    description: str | None = None
    file_requirements: FileRequirements
    schema_block: DeployConventionSchema = Field(alias="schema")
    hooks: list[DeployConventionHook] = []
    ingester: IngesterDefinition | None = None


class ConventionCreated(Result):
    slug: ConventionSlug
    title: str
    description: str | None
    schema_id: SchemaId
    hooks: list[str]
    created_at: datetime


class DeployConventionHandler(CommandHandler[DeployConvention, ConventionCreated]):
    # Conventions are curated registry artifacts; deploy is an admin/automation
    # operation. Authorized by the ``conventions:write`` M2M scope OR an ADMIN
    # role (#145, US5).
    __auth__ = requires_scope("conventions:write")
    principal: Principal
    convention_service: ConventionService

    async def run(self, cmd: DeployConvention) -> ConventionCreated:
        built_by = str(self.principal.user_id) if self.principal.user_id else None
        convention = await self.convention_service.deploy(
            slug=cmd.slug,
            title=cmd.title,
            description=cmd.description,
            file_requirements=cmd.file_requirements,
            schema_slug=cmd.schema_block.id,
            schema_version=cmd.schema_block.version,
            schema_fields=cmd.schema_block.fields,
            hooks=[h.to_deploy() for h in cmd.hooks],
            ingester=cmd.ingester,
            built_by=built_by,
        )
        return ConventionCreated(
            slug=convention.id,
            title=convention.title,
            description=convention.description,
            schema_id=convention.schema_id,
            hooks=[name.root for name in convention.hooks],
            created_at=convention.created_at,
        )
