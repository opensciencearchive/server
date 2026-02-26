"""Dependency injection provider for sources."""

from dishka import provide

from osa.domain.shared.outbox import Outbox
from osa.domain.source.port.source_runner import SourceRunner
from osa.domain.source.port.storage import SourceStoragePort
from osa.domain.source.service import SourceService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class SourceProvider(Provider):
    """Provides SourceService wired with OCI runner."""

    @provide(scope=Scope.UOW)
    def get_source_service(
        self,
        source_runner: SourceRunner,
        source_storage: SourceStoragePort,
        outbox: Outbox,
    ) -> SourceService:
        return SourceService(
            source_runner=source_runner,
            source_storage=source_storage,
            outbox=outbox,
        )
