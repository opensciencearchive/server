from typing import AsyncIterable

from dishka import provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.cli.util.paths import OSAPaths
from osa.config import Config
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.ontology_reader import OntologyReader
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.query.get_record import GetRecordHandler
from osa.domain.record.service import RecordService
from osa.domain.source.port.storage import SourceStoragePort
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.validation.port.storage import HookStoragePort
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.infrastructure.persistence.adapter.readers import (
    OntologyReaderAdapter,
    SchemaReaderAdapter,
)
from osa.infrastructure.persistence.adapter.storage import LocalFileStorageAdapter
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
from osa.infrastructure.persistence.repository.validation import (
    PostgresValidationRunRepository,
)
from osa.util.di.base import Provider
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

    # File storage
    @provide(scope=Scope.APP)
    def get_file_storage(self, paths: "OSAPaths") -> FileStoragePort:
        return LocalFileStorageAdapter(base_path=str(paths.data_dir / "files"))

    @provide(scope=Scope.APP)
    def get_source_storage(self, file_storage: FileStoragePort) -> SourceStoragePort:
        return file_storage  # type: ignore[return-value]

    @provide(scope=Scope.APP)
    def get_hook_storage(self, file_storage: FileStoragePort) -> HookStoragePort:
        return file_storage  # type: ignore[return-value]

    @provide(scope=Scope.APP)
    def get_feature_storage(self, file_storage: FileStoragePort) -> FeatureStoragePort:
        return file_storage  # type: ignore[return-value]

    @provide(scope=Scope.UOW)
    def get_record_service(
        self,
        record_repo: RecordRepository,
        outbox: Outbox,
        config: Config,
    ) -> RecordService:
        """Provide RecordService for UOW scope.

        RecordService is UOW-scoped because it needs fresh Outbox per unit of work.
        """
        return RecordService(
            record_repo=record_repo,
            outbox=outbox,
            node_domain=Domain(config.server.domain),
        )

    # Record query handlers
    get_record_handler = provide(GetRecordHandler, scope=Scope.UOW)
