from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from osa.domain.auth.model.principal import Principal
from osa.domain.deposition.model.deploy import HookDeploy
from osa.domain.deposition.model.docs import (
    MIN_DISTINCT_TRIGGER_QUESTIONS,
    ConventionDocs,
    Example,
)
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

    model_config = ConfigDict(extra="forbid")

    id: SchemaIdentifier
    version: str
    fields: list[FieldDefinition] = []


class DeployConventionRelease(BaseModel):
    """A hook's release block (== POST /hooks/{name}/releases body).

    ``extra="forbid"`` + a required ``config`` make a client/server payload-shape
    mismatch fail loudly at deploy (422, naming the offending field) rather than
    being silently swallowed into an empty config that only fails at container
    runtime. ``limits`` keeps its defaults — omitting resource limits is a valid,
    explicit choice; a *misnamed* limits field is still caught by ``extra``.
    """

    model_config = ConfigDict(extra="forbid")

    image: str
    digest: str
    # Opaque, image-defined JSON object forwarded verbatim to the container —
    # OSA never reads its keys. Required (don't default a dropped config to {}).
    config: dict[str, Any]
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str  # REQUIRED — reproducibility anchor (FR-005)


class DeployConventionHook(BaseModel):
    """One hook in the bundled deploy: identity (name + fixed feature) + release."""

    model_config = ConfigDict(extra="forbid")

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


class ExamplePayload(BaseModel):
    """Edge mirror of the ``Example`` VO — a worked example, rendered verbatim.

    ``query`` is opaque: never parsed, executed, or validated (FR-011).
    """

    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1)
    query: str = Field(min_length=1)
    interpretation: str = Field(min_length=1)

    @field_validator("question", "query", "interpretation")
    @classmethod
    def _non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty or whitespace-only")
        return v

    def to_vo(self) -> Example:
        return Example(question=self.question, query=self.query, interpretation=self.interpretation)


class ConventionDocsPayload(BaseModel):
    """Edge mirror of the ``ConventionDocs`` VO (#151).

    The mandatory-docs minimum is repeated here so violations surface as a 422
    whose field-level errors name each gap, regardless of client (FR-015/016).
    There is no bypass flag.
    """

    model_config = ConfigDict(extra="forbid")

    purpose: str = Field(min_length=1)
    example_questions: list[str] = []
    examples: list[ExamplePayload] = Field(min_length=1)
    when_not_to_use: str | None = None
    see_also: list[str] | None = None

    @field_validator("purpose")
    @classmethod
    def _purpose_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("purpose must not be empty or whitespace-only")
        return v

    @field_validator("example_questions")
    @classmethod
    def _questions_non_blank(cls, v: list[str]) -> list[str]:
        if any(not q.strip() for q in v):
            raise ValueError("example_questions entries must not be empty")
        return v

    @model_validator(mode="after")
    def _require_trigger_breadth(self) -> "ConventionDocsPayload":
        distinct = {q.strip() for q in self.example_questions}
        distinct |= {e.question.strip() for e in self.examples}
        if len(distinct) < MIN_DISTINCT_TRIGGER_QUESTIONS:
            raise ValueError(
                f"need at least {MIN_DISTINCT_TRIGGER_QUESTIONS} distinct trigger "
                f"questions across example_questions and worked-example questions, "
                f"got {len(distinct)}"
            )
        return self

    def to_vo(self) -> ConventionDocs:
        return ConventionDocs(
            purpose=self.purpose,
            example_questions=self.example_questions,
            examples=[e.to_vo() for e in self.examples],
            when_not_to_use=self.when_not_to_use,
            see_also=self.see_also,
        )


class DeployConvention(Command):
    """Bundled deploy: schema + hooks (+ first releases) + convention, atomically.

    Conventions are unversioned and mutable (design-revisions §3): deploy is a
    declarative upsert keyed by ``slug`` — re-declaring the same state is a no-op,
    a different declaration updates the convention in place. No caller-supplied
    version, no conflict path.

    The identity ``slug`` is **derived server-side** from ``title`` (the API does
    not accept a slug). Because the slug is what deploy upserts on, the title is
    identity-bearing: a different title yields a different slug (a new convention).
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    title: str
    description: str = Field(min_length=1)  # required — every convention must describe itself
    file_requirements: FileRequirements
    schema_block: DeployConventionSchema = Field(alias="schema")
    hooks: list[DeployConventionHook] = []
    ingester: IngesterDefinition | None = None
    # Author semantics — required; documentation is mandatory (#151, FR-015).
    docs: ConventionDocsPayload


class ConventionCreated(Result):
    slug: ConventionSlug
    title: str
    description: str
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
            slug=ConventionSlug.from_title(cmd.title),
            title=cmd.title,
            description=cmd.description,
            file_requirements=cmd.file_requirements,
            schema_slug=cmd.schema_block.id,
            schema_version=cmd.schema_block.version,
            schema_fields=cmd.schema_block.fields,
            hooks=[h.to_deploy() for h in cmd.hooks],
            ingester=cmd.ingester,
            docs=cmd.docs.to_vo(),
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
