from dishka import provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.service import ValidationService
from osa.domain.validation.service.hook import HookService
from osa.domain.validation.service.hook_registry import HookRegistryService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class ValidationProvider(Provider):
    service = provide(ValidationService, scope=Scope.UOW)
    hook_service = provide(HookService, scope=Scope.UOW)

    # Hook registry (feature #145). Release/live/catalog handlers are registered
    # in US3–US4 once their command/query handlers exist.
    hook_registry_service = provide(HookRegistryService, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_node_domain(self, config: Config) -> Domain:
        return Domain(config.domain)
