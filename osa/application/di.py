from dishka import make_async_container, AsyncContainer

from osa.config import Config
from osa.infrastructure.persistence import PersistenceProvider
from osa.domain.shadow.util.di import ShadowProvider


def create_container() -> AsyncContainer:
    config = Config()
    
    return make_async_container(
        PersistenceProvider(),
        ShadowProvider(),
        context={
            Config: config
        }
    )
