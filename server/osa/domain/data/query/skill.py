"""Skill-surface query handlers — root discovery, SKILL.md, schema reference (#151).

All public (``__auth__ = public()``): the surface documents published data
only (FR-004) and exists precisely so unauthenticated agents can bootstrap.
"""

from __future__ import annotations

from osa.domain.data.model.skill import RootDiscovery
from osa.domain.data.service.skill_generator import SkillGeneratorService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.query import Query, QueryHandler


class GetRootDiscovery(Query):
    # The app's actual configured OpenAPI path (research §7) — the route knows
    # it; the domain must not import the FastAPI app.
    openapi_path: str


class GetRootDiscoveryHandler(QueryHandler[GetRootDiscovery, RootDiscovery]):
    __auth__ = public()
    generator: SkillGeneratorService

    async def run(self, cmd: GetRootDiscovery) -> RootDiscovery:
        return await self.generator.root_discovery(cmd.openapi_path)


class GetSkillDocument(Query):
    pass


class GetSkillDocumentHandler(QueryHandler[GetSkillDocument, str]):
    __auth__ = public()
    generator: SkillGeneratorService

    async def run(self, cmd: GetSkillDocument) -> str:
        return await self.generator.skill_document()


class GetSchemaReference(Query):
    schema: str  # URL segment: ``<id>`` (latest) or ``<id>@<semver>``


class GetSchemaReferenceHandler(QueryHandler[GetSchemaReference, str]):
    __auth__ = public()
    generator: SkillGeneratorService

    async def run(self, cmd: GetSchemaReference) -> str:
        return await self.generator.schema_reference(cmd.schema)
