from osa.util.di.scope import Scope
from dishka import provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.handler import BeginMockValidation
from osa.domain.validation.service import ValidationService
from osa.util.di.base import Provider


class ValidationProvider(Provider):
    validation_handler = provide(BeginMockValidation, scope=Scope.UOW)
    service = provide(ValidationService, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_node_domain(self, config: Config) -> Domain:
        return Domain(config.server.domain)
