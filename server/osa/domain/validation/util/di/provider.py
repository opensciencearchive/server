from dishka import provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.command.create_release import CreateReleaseHandler
from osa.domain.validation.command.set_live import SetLiveHandler
from osa.domain.validation.query.get_release import GetReleaseHandler
from osa.domain.validation.query.list_hooks import ListHooksHandler
from osa.domain.validation.query.list_releases import ListReleasesHandler
from osa.domain.validation.service import ValidationService
from osa.domain.validation.service.hook import HookService
from osa.domain.validation.service.hook_registry import HookRegistryService
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class ValidationProvider(Provider):
    service = provide(ValidationService, scope=Scope.UOW)
    hook_service = provide(HookService, scope=Scope.UOW)

    # Hook registry (feature #145).
    hook_registry_service = provide(HookRegistryService, scope=Scope.UOW)

    # Release (US3) + live-pointer (US4) write handlers.
    create_release_handler = provide(CreateReleaseHandler, scope=Scope.UOW)
    set_live_handler = provide(SetLiveHandler, scope=Scope.UOW)

    # Catalog / history / detail read handlers (US3).
    list_hooks_handler = provide(ListHooksHandler, scope=Scope.UOW)
    list_releases_handler = provide(ListReleasesHandler, scope=Scope.UOW)
    get_release_handler = provide(GetReleaseHandler, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_node_domain(self, config: Config) -> Domain:
        return Domain(config.domain)
