from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from osa.domain.auth.model.principal import Principal
from osa.domain.deposition.model.deploy import HookDeploy
from osa.domain.deposition.model.docs import (
    MIN_DISTINCT_TRIGGER_QUESTIONS,
    ConventionDocs,
    Example,
    NonBlankStr,
)
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.feature.service.feature import FeatureService
from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import ConflictError
from osa.domain.shared.model.hook import (
    HookIdentity,
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.shared.model.source import (
    IngesterDefinition,
    IngesterLimits,
    IngesterScheduleConfig,
    InitialRunConfig,
)
from osa.domain.shared.model.srn import ConventionSlug, SchemaId, SchemaIdentifier


class DeployConventionSchema(BaseModel):
    """The deploy's nested ``schema`` sub-structure (== POST /schemas body)."""

    model_config = ConfigDict(extra="forbid")

    id: SchemaIdentifier
    version: str
    fields: list[FieldDefinition] = []


class DeployConventionRelease(BaseModel):
    """A component's built release — a *pure build artifact*.

    ``config``/``limits`` are authored definition and live on the component, not
    here (a `limits` tweak is a definition change, not a rebuild). ``extra="forbid"``
    makes a payload-shape mismatch fail loudly at deploy (422, naming the field).
    """

    model_config = ConfigDict(extra="forbid")

    image: str
    digest: str
    source_ref: str  # REQUIRED — reproducibility anchor (FR-005)


class DeployConventionHook(BaseModel):
    """One hook in the bundled deploy: identity (name + fixed feature), authored
    runtime (``config``/``limits``), and its built ``release``."""

    model_config = ConfigDict(extra="forbid")

    name: HookName
    feature: TableFeatureSpec
    # Opaque, image-defined JSON forwarded verbatim to the container — OSA never
    # reads its keys. Required (don't default a dropped config to {}).
    config: dict[str, Any]
    limits: OciLimits = Field(default_factory=OciLimits)
    release: DeployConventionRelease

    def to_deploy(self) -> HookDeploy:
        # Re-gather authored config/limits (on the hook) with the built image
        # (in the release) into the internal runtime — internals are unchanged.
        return HookDeploy(
            identity=HookIdentity(name=self.name, feature=self.feature),
            runtime=OciConfig(
                image=self.release.image,
                digest=self.release.digest,
                config=self.config,
                limits=self.limits,
            ),
            source_ref=self.release.source_ref,
        )


class DeployConventionIngester(BaseModel):
    """The ingester in the bundled deploy — symmetric with a hook: authored
    ``config``/``limits``/schedule + its built ``release``."""

    model_config = ConfigDict(extra="forbid")

    name: str  # build-fan-out key (cloud); not persisted server-side
    config: dict[str, Any] | None = None
    limits: IngesterLimits = Field(default_factory=IngesterLimits)
    schedule: IngesterScheduleConfig | None = None
    initial_run: InitialRunConfig | None = None
    release: DeployConventionRelease

    def to_definition(self) -> IngesterDefinition:
        return IngesterDefinition(
            image=self.release.image,
            digest=self.release.digest,
            config=self.config,
            limits=self.limits,
            schedule=self.schedule,
            initial_run=self.initial_run,
            source_ref=self.release.source_ref,
        )


class ExamplePayload(BaseModel):
    """Edge mirror of the ``Example`` VO — a worked example, rendered verbatim.

    ``query`` is opaque: never parsed, executed, or validated (FR-011).
    """

    model_config = ConfigDict(extra="forbid")

    question: NonBlankStr
    query: NonBlankStr
    interpretation: NonBlankStr

    def to_vo(self) -> Example:
        return Example(question=self.question, query=self.query, interpretation=self.interpretation)


class ConventionDocsPayload(BaseModel):
    """Edge mirror of the ``ConventionDocs`` VO (#151).

    The mandatory-docs minimum is repeated here so violations surface as a 422
    whose field-level errors name each gap, regardless of client (FR-015/016).
    There is no bypass flag.
    """

    model_config = ConfigDict(extra="forbid")

    purpose: NonBlankStr
    example_questions: list[NonBlankStr] = []
    examples: list[ExamplePayload] = Field(min_length=1)
    when_not_to_use: str | None = None
    see_also: list[str] | None = None

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
    ingester: DeployConventionIngester | None = None
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
    # Cross-domain composition happens HERE at the entry point, not inside
    # ConventionService: domains must not import each other, so the deposition
    # service cannot create feature tables. The command handler (application-edge)
    # is the sanctioned place to fan out across the deposition and feature domains
    # (#160, decision 9).
    feature_service: FeatureService

    async def run(self, cmd: DeployConvention) -> ConventionCreated:
        """Deploy the convention, then materialise its feature tables.

        Table creation runs AFTER ``deploy`` succeeds, one ``create_table`` per
        declared hook, swallowing ``ConflictError`` (a table already created by a
        prior deploy). Because deploy is a declarative upsert, a client retry
        after a partial failure re-runs creation idempotently — and because the
        tables exist by the time this response returns, the old "table readiness
        is unsignalled" gap (formerly the async ``CreateFeatureTables`` handler)
        is closed.
        """
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
            ingester=cmd.ingester.to_definition() if cmd.ingester else None,
            docs=cmd.docs.to_vo(),
            built_by=built_by,
        )
        for hook in cmd.hooks:
            identity = HookIdentity(name=hook.name, feature=hook.feature)
            try:
                await self.feature_service.create_table(identity)
            except ConflictError:
                # Table already exists (idempotent re-deploy) — safe to skip.
                pass
        return ConventionCreated(
            slug=convention.id,
            title=convention.title,
            description=convention.description,
            schema_id=convention.schema_id,
            hooks=[name.root for name in convention.hooks],
            created_at=convention.created_at,
        )
