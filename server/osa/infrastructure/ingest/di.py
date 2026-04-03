"""Dependency injection provider for ingest domain."""

from dishka import provide
from sqlalchemy.ext.asyncio import AsyncSession

from osa.config import Config
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.command.start_ingest import StartIngestHandler
from osa.infrastructure.s3.client import S3Client
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.infrastructure.persistence.adapter.ingest_storage import FilesystemIngestStorage
from osa.infrastructure.persistence.repository.ingest import PostgresIngestRunRepository
from osa.infrastructure.storage.layout import StorageLayout
from osa.util.di.base import Provider
from osa.util.di.markers import K8S
from osa.util.di.scope import Scope
from osa.util.paths import OSAPaths


class IngestProvider(Provider):
    """Provides IngestService, IngestRunRepository, StorageLayout, and StartIngestHandler."""

    @provide(scope=Scope.APP)
    def get_storage_layout(self, paths: OSAPaths) -> StorageLayout:
        return StorageLayout(paths.data_dir)

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

    # Ingest storage — default (filesystem, for local/Docker)
    @provide(scope=Scope.APP)
    def get_ingest_storage(self, layout: StorageLayout) -> IngestStoragePort:
        return FilesystemIngestStorage(layout=layout)  # type: ignore[return-value]

    # Ingest storage — K8s (S3 via aioboto3, reuses S3Client from RunnerProvider)
    @provide(when=K8S, scope=Scope.APP)
    def get_ingest_storage_s3(
        self, layout: StorageLayout, config: Config, s3: S3Client
    ) -> IngestStoragePort:
        from osa.infrastructure.s3.ingest_storage import S3IngestStorage

        return S3IngestStorage(  # type: ignore[return-value]
            s3=s3,
            layout=layout,
            data_mount_path=config.runner.k8s.data_mount_path,
        )

    start_ingest_handler = provide(StartIngestHandler, scope=Scope.UOW)
