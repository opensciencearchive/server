from typing import AsyncIterable

from dishka import provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.config import Config
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.service import RecordService
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.infrastructure.persistence.database import (
    create_db_engine,
    create_session_factory,
)
from osa.infrastructure.persistence.repository.deposition import (
    PostgresDepositionRepository,
)
from osa.infrastructure.persistence.repository.event import (
    SQLAlchemyEventRepository,
)
from osa.infrastructure.persistence.repository.record import (
    PostgresRecordRepository,
)
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

    # UOW-scoped repositories
    dep_repo = provide(PostgresDepositionRepository, scope=Scope.UOW, provides=DepositionRepository)
    record_repo = provide(PostgresRecordRepository, scope=Scope.UOW, provides=RecordRepository)
    validation_run_repo = provide(
        PostgresValidationRunRepository,
        scope=Scope.UOW,
        provides=ValidationRunRepository,
    )
    event_repo = provide(SQLAlchemyEventRepository, scope=Scope.UOW, provides=EventRepository)

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
