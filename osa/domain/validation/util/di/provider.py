from dishka import Scope, provide

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.validation.handler import ValidationHandler
from osa.domain.validation.port.repository import TraitRepository, ValidationRunRepository
from osa.domain.validation.port.runner import ValidatorRunner
from osa.domain.validation.service import ValidationService
from osa.util.di.base import Provider


class ValidationProvider(Provider):
    validation_handler = provide(ValidationHandler, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    def get_node_domain(self, config: Config) -> Domain:
        return Domain(config.server.domain)

    @provide(scope=Scope.REQUEST)
    def get_validation_service(
        self,
        trait_repo: TraitRepository,
        run_repo: ValidationRunRepository,
        runner: ValidatorRunner,
        node_domain: Domain,
    ) -> ValidationService:
        return ValidationService(
            trait_repo=trait_repo,
            run_repo=run_repo,
            runner=runner,
            node_domain=node_domain,
        )
