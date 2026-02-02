from dishka import provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.service import ValidationService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class ValidationProvider(Provider):
    service = provide(ValidationService, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_node_domain(self, config: Config) -> Domain:
        return Domain(config.server.domain)
