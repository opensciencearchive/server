"""Dependency injection provider for sources."""

from dishka import provide

from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.outbox import Outbox
from osa.domain.source.port.source_runner import SourceRunner
from osa.domain.source.service import SourceService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class SourceProvider(Provider):
    """Provides SourceService wired with OCI runner."""

    @provide(scope=Scope.UOW)
    def get_source_service(
        self,
        source_runner: SourceRunner,
        deposition_service: DepositionService,
        file_storage: FileStoragePort,
        convention_repo: ConventionRepository,
        outbox: Outbox,
    ) -> SourceService:
        return SourceService(
            source_runner=source_runner,
            deposition_service=deposition_service,
            file_storage=file_storage,
            convention_repo=convention_repo,
            outbox=outbox,
        )
