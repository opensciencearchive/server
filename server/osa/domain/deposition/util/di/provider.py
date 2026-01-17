from osa.util.di.scope import Scope
from dishka import provide

from osa.domain.deposition.command.create import CreateDepositionHandler
from osa.domain.deposition.command.submit import SubmitDepositionHandler
from osa.domain.deposition.service.deposition import DepositionService
from osa.util.di.base import Provider


class DepositionProvider(Provider):
    service = provide(DepositionService, scope=Scope.UOW)

    # Command Handlers
    create_handler = provide(CreateDepositionHandler, scope=Scope.UOW)
    submit_handler = provide(SubmitDepositionHandler, scope=Scope.UOW)
