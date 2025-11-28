from dishka import Scope, provide

from osa.domain.shadow.adapter.ingestion import HttpIngestionAdapter
from osa.domain.shadow.event.listener import ValidationCompletedListener
from osa.domain.shadow.port.ingestion import IngestionPort
from osa.domain.shadow.service.orchestrator import ShadowOrchestrator
from osa.util.di.base import Provider


class ShadowProvider(Provider):
    # Services
    orchestrator = provide(ShadowOrchestrator, scope=Scope.REQUEST)

    # Event Listeners
    validation_listener = provide(ValidationCompletedListener, scope=Scope.REQUEST)

    # Ports & Adapters
    # Note: ShadowRepository is likely provided by an InfraProvider (SQLAlchemy),
    # but if we had a memory default we could put it here.
    # For now, we explicitly provide the Ingestion adapter.
    ingestion = provide(
        HttpIngestionAdapter, scope=Scope.REQUEST, provides=IngestionPort
    )
