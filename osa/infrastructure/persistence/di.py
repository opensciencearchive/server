from typing import AsyncIterable

from dishka import Scope, provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.config import Config
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.shadow.port.repository import ShadowRepository
from osa.domain.validation.port.repository import TraitRepository, ValidationRunRepository
from osa.infrastructure.persistence.database import (
    create_db_engine,
    create_session_factory,
)
from osa.infrastructure.persistence.repository.deposition import (
    PostgresDepositionRepository,
)
from osa.infrastructure.persistence.repository.shadow import PostgresShadowRepository
from osa.infrastructure.persistence.repository.validation import (
    PostgresTraitRepository,
    PostgresValidationRunRepository,
)
from osa.util.di.base import Provider


class PersistenceProvider(Provider):
    # Factories require method syntax
    @provide(scope=Scope.APP)
    def get_engine(self, config: Config) -> AsyncEngine:
        return create_db_engine(config)

    @provide(scope=Scope.APP)
    def get_session_factory(
        self, engine: AsyncEngine
    ) -> async_sessionmaker[AsyncSession]:
        return create_session_factory(engine)

    @provide(scope=Scope.REQUEST)
    async def get_session(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterable[AsyncSession]:
        async with session_factory() as session:
            yield session

    # Repositories
    shadow_repo = provide(
        PostgresShadowRepository, scope=Scope.REQUEST, provides=ShadowRepository
    )

    dep_repo = provide(
        PostgresDepositionRepository, scope=Scope.REQUEST, provides=DepositionRepository
    )

    # Validation repositories
    trait_repo = provide(
        PostgresTraitRepository, scope=Scope.REQUEST, provides=TraitRepository
    )
    validation_run_repo = provide(
        PostgresValidationRunRepository, scope=Scope.REQUEST, provides=ValidationRunRepository
    )
