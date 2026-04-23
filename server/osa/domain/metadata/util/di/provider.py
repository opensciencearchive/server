"""DI provider for the metadata bounded context."""

from dishka import provide

from osa.domain.metadata.service.metadata import MetadataService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class MetadataProvider(Provider):
    service = provide(MetadataService, scope=Scope.UOW)
