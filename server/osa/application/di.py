from dishka import AsyncContainer, make_async_container

from osa.cli.util.paths import OSAPaths
from osa.config import Config
from osa.domain.auth.util.di import AuthProvider
from osa.domain.deposition.util.di import DepositionProvider
from osa.domain.validation.util.di import ValidationProvider
from osa.infrastructure.auth import AuthInfraProvider
from osa.infrastructure.event.di import EventProvider
from osa.infrastructure.index.di import IndexProvider
from osa.infrastructure.source.di import SourceProvider
from osa.infrastructure.oci import OciProvider
from osa.infrastructure.persistence import PersistenceProvider
from osa.util.di.scope import Scope


def create_container() -> AsyncContainer:
    # Pydantic Settings populates from env vars at runtime
    config = Config()  # type: ignore[call-arg]

    # OSAPaths reads OSA_DATA_DIR from environment automatically
    paths = OSAPaths()

    return make_async_container(
        PersistenceProvider(),
        OciProvider(),
        IndexProvider(),
        SourceProvider(),
        EventProvider(),
        DepositionProvider(),
        ValidationProvider(),
        AuthProvider(),
        AuthInfraProvider(),
        context={Config: config, OSAPaths: paths},
        scopes=Scope,  # type: ignore[arg-type]  # Custom scope class
    )
