from dishka import make_async_container, AsyncContainer

from osa.config import Config
from osa.domain.deposition.util.di import DepositionProvider
from osa.domain.shadow.util.di import ShadowProvider
from osa.domain.validation.util.di import ValidationProvider
from osa.infrastructure.index import IndexProvider
from osa.infrastructure.ingest import IngestProvider
from osa.infrastructure.oci import OciProvider
from osa.infrastructure.persistence import PersistenceProvider
from osa.infrastructure.shared import SharedProvider


def create_container() -> AsyncContainer:
    config = Config()

    return make_async_container(
        SharedProvider(),
        PersistenceProvider(),
        OciProvider(),
        IndexProvider(),
        IngestProvider(),
        ShadowProvider(),
        DepositionProvider(),
        ValidationProvider(),
        context={
            Config: config
        }
    )
