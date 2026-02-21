"""DI provider for the feature bounded context."""

from dishka import provide

from osa.domain.feature.service.feature import FeatureService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class FeatureProvider(Provider):
    service = provide(FeatureService, scope=Scope.UOW)
