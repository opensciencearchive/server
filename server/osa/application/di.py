from typing import Any

from dishka import AsyncContainer, make_async_container
from dishka import Provider as DishkaProvider

from osa.config import Config
from osa.domain.auth.util.di import AuthProvider
from osa.domain.deposition.util.di import DepositionProvider
from osa.domain.discovery.util.di import DiscoveryProvider
from osa.domain.feature.util.di import FeatureProvider
from osa.domain.semantics.util.di.provider import SemanticsProvider
from osa.domain.shared.event import EventHandler
from osa.domain.validation.util.di import ValidationProvider
from osa.infrastructure.auth import AuthInfraProvider
from osa.infrastructure.event.di import EventProvider
from osa.infrastructure.http.di import HttpProvider
from osa.infrastructure.index.di import IndexProvider
from osa.infrastructure.oci import OciProvider
from osa.infrastructure.persistence import PersistenceProvider
from osa.infrastructure.source.di import SourceProvider
from osa.util.di.scope import Scope
from osa.util.paths import OSAPaths


def create_container(
    *extra_providers: DishkaProvider,
    extra_handlers: list[type[EventHandler[Any]]] | None = None,
) -> AsyncContainer:
    """Create the DI container with all default providers.

    Args:
        extra_providers: Additional Dishka providers appended after defaults.
            Later providers override earlier ones for the same type, so these
            can replace any built-in binding (e.g. swap OciProvider for a
            Kubernetes runner).
        extra_handlers: Additional event handler types to register alongside
            the core handlers. They will be included in the subscription
            registry, WorkerPool, and DI resolution automatically.
    """
    config = Config()  # type: ignore[call-arg]
    paths = OSAPaths()

    return make_async_container(
        PersistenceProvider(),
        OciProvider(),
        IndexProvider(),
        SourceProvider(),
        EventProvider(extra_handlers=extra_handlers),
        HttpProvider(),
        DepositionProvider(),
        FeatureProvider(),
        SemanticsProvider(),
        ValidationProvider(),
        AuthProvider(),
        AuthInfraProvider(),
        DiscoveryProvider(),
        *extra_providers,
        context={Config: config, OSAPaths: paths},
        scopes=Scope,  # type: ignore[arg-type]  # Custom scope class
    )
