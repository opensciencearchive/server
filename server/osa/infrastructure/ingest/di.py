"""Dependency injection provider for ingest domain."""

from dishka import provide
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.command.start_ingest import StartIngestHandler
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.infrastructure.persistence.repository.ingest import PostgresIngestRunRepository
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class IngestProvider(Provider):
    """Provides IngestService, IngestRunRepository, and StartIngestHandler."""

    @provide(scope=Scope.UOW)
    def get_ingest_repo(self, session: AsyncSession) -> IngestRunRepository:
        return PostgresIngestRunRepository(session)

    @provide(scope=Scope.UOW)
    def get_ingest_service(
        self,
        ingest_repo: IngestRunRepository,
        convention_service: ConventionService,
        outbox: Outbox,
        node_domain: Domain,
    ) -> IngestService:
        return IngestService(
            ingest_repo=ingest_repo,
            convention_service=convention_service,
            outbox=outbox,
            node_domain=node_domain,
        )

    start_ingest_handler = provide(StartIngestHandler, scope=Scope.UOW)
