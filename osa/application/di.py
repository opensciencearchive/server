from dishka import AsyncContainer, make_async_container

from osa.config import Config
from osa.domain.deposition.util.di import DepositionProvider
from osa.domain.validation.util.di import ValidationProvider
from osa.infrastructure.event.di import EventProvider
from osa.infrastructure.index.di import IndexProvider
from osa.infrastructure.ingest.di import IngestProvider
from osa.infrastructure.oci import OciProvider
from osa.infrastructure.persistence import PersistenceProvider
from osa.util.di.scope import Scope


def create_container() -> AsyncContainer:
    config = Config()

    return make_async_container(
        PersistenceProvider(),
        OciProvider(),
        IndexProvider(),
        IngestProvider(),
        EventProvider(),
        DepositionProvider(),
        ValidationProvider(),
        context={Config: config},
        scopes=Scope,  # type: ignore[arg-type]  # Custom scope class
    )
