from typing import AsyncIterable

from dishka import provide

from osa.infrastructure.s3.client import S3Client
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.util.paths import OSAPaths
from osa.config import Config
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.ontology_reader import OntologyReader
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.record.port.feature_reader import FeatureReader
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.query.get_record import GetRecordHandler
from osa.domain.record.query.get_stats import GetStatsHandler
from osa.domain.record.service import RecordService
from osa.infrastructure.persistence.adapter.feature_reader import PostgresFeatureReader
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.validation.port.storage import HookStoragePort
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.domain.data.port.data_read_store import DataReadStore
from osa.infrastructure.data.postgres_data_read_store import PostgresDataReadStore
from osa.infrastructure.persistence.adapter.readers import (
    OntologyReaderAdapter,
    SchemaReaderAdapter,
)
from osa.infrastructure.persistence.adapter.storage import FilesystemStorageAdapter
from osa.infrastructure.persistence.database import (
    create_db_engine,
    create_session_factory,
)
from osa.infrastructure.persistence.repository.convention import (
    PostgresConventionRepository,
)
from osa.infrastructure.persistence.repository.deposition import (
    PostgresDepositionRepository,
)
from osa.infrastructure.persistence.repository.event import (
    SQLAlchemyEventRepository,
)
from osa.infrastructure.persistence.repository.ontology import (
    PostgresOntologyRepository,
)
from osa.infrastructure.persistence.repository.record import (
    PostgresRecordRepository,
)
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.domain.metadata.port.metadata_store import MetadataStore
from osa.infrastructure.persistence.repository.validation import (
    PostgresValidationRunRepository,
)
from osa.util.di.base import Provider
from osa.util.di.markers import K8S
from osa.util.di.scope import Scope


class PersistenceProvider(Provider):
    # APP-scoped factories
    @provide(scope=Scope.APP)
    def get_engine(self, config: Config) -> AsyncEngine:
        return create_db_engine(config)

    @provide(scope=Scope.APP)
    def get_session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return create_session_factory(engine)

    # UOW-scoped session (one per unit of work)
    @provide(scope=Scope.UOW)
    async def get_session(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterable[AsyncSession]:
        async with session_factory() as session:
            yield session
            await session.commit()

    # UOW-scoped repositories
    dep_repo = provide(PostgresDepositionRepository, scope=Scope.UOW, provides=DepositionRepository)
    record_repo = provide(PostgresRecordRepository, scope=Scope.UOW, provides=RecordRepository)
    validation_run_repo = provide(
        PostgresValidationRunRepository,
        scope=Scope.UOW,
        provides=ValidationRunRepository,
    )
    event_repo = provide(SQLAlchemyEventRepository, scope=Scope.UOW, provides=EventRepository)

    # Feature store
    @provide(scope=Scope.UOW)
    def get_feature_store(self, engine: AsyncEngine, session: AsyncSession) -> FeatureStore:
        return PostgresFeatureStore(engine=engine, session=session)

    # Metadata store
    @provide(scope=Scope.UOW)
    def get_metadata_store(self, engine: AsyncEngine, session: AsyncSession) -> MetadataStore:
        return PostgresMetadataStore(engine=engine, session=session)

    # Semantics repositories
    ontology_repo = provide(
        PostgresOntologyRepository, scope=Scope.UOW, provides=OntologyRepository
    )
    schema_repo = provide(
        PostgresSemanticsSchemaRepository, scope=Scope.UOW, provides=SchemaRepository
    )

    # Deposition repositories and adapters
    convention_repo = provide(
        PostgresConventionRepository, scope=Scope.UOW, provides=ConventionRepository
    )

    # Cross-domain readers
    schema_reader = provide(SchemaReaderAdapter, scope=Scope.UOW, provides=SchemaReader)
    ontology_reader = provide(OntologyReaderAdapter, scope=Scope.UOW, provides=OntologyReader)

    # File storage — default (OCI/Docker, filesystem)
    @provide(scope=Scope.APP)
    def get_file_storage(self, paths: "OSAPaths") -> FileStoragePort:
        return FilesystemStorageAdapter(base_path=str(paths.data_dir / "files"))

    # File storage — K8s (S3 via aioboto3, reuses S3Client from RunnerProvider)
    @provide(when=K8S, scope=Scope.APP)
    def get_file_storage_s3(self, config: Config, s3: "S3Client") -> FileStoragePort:
        from osa.infrastructure.s3.storage import S3StorageAdapter

        return S3StorageAdapter(s3=s3, data_mount_path=config.runner.k8s.data_mount_path)

    @provide(scope=Scope.APP)
    def get_hook_storage(self, file_storage: FileStoragePort) -> HookStoragePort:
        return file_storage  # type: ignore[return-value]

    @provide(scope=Scope.APP)
    def get_feature_storage(self, file_storage: FileStoragePort) -> FeatureStoragePort:
        return file_storage  # type: ignore[return-value]

    # Feature reader
    feature_reader = provide(PostgresFeatureReader, scope=Scope.UOW, provides=FeatureReader)

    @provide(scope=Scope.UOW)
    def get_record_service(
        self,
        record_repo: RecordRepository,
        convention_repo: ConventionRepository,
        metadata_service: MetadataService,
        outbox: Outbox,
        config: Config,
        feature_reader: FeatureReader,
    ) -> RecordService:
        """Provide RecordService for UOW scope.

        RecordService is UOW-scoped because it needs fresh Outbox per unit of work.
        """
        return RecordService(
            record_repo=record_repo,
            convention_repo=convention_repo,
            metadata_service=metadata_service,
            outbox=outbox,
            node_domain=Domain(config.domain),
            feature_reader=feature_reader,
        )

    # Data read surface adapter (unified /data/* engine)
    @provide(scope=Scope.UOW, provides=DataReadStore)
    def get_data_read_store(self, session: AsyncSession, config: Config) -> PostgresDataReadStore:
        return PostgresDataReadStore(session=session, node_domain=Domain(config.domain))

    # Record query handlers
    get_record_handler = provide(GetRecordHandler, scope=Scope.UOW)
    get_stats_handler = provide(GetStatsHandler, scope=Scope.UOW)
