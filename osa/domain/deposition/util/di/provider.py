from dishka import Scope, provide

from osa.domain.deposition.command.create import CreateDepositionHandler
from osa.domain.deposition.command.delete_files import DeleteDepositionFilesHandler
from osa.domain.deposition.command.submit import SubmitDepositionHandler
from osa.domain.deposition.command.upload import UploadFileHandler
from osa.domain.deposition.service.deposition import DepositionService
from osa.util.di.base import Provider


class DepositionProvider(Provider):
    service = provide(DepositionService, scope=Scope.REQUEST)
    
    # Command Handlers
    create_handler = provide(CreateDepositionHandler, scope=Scope.REQUEST)
    upload_handler = provide(UploadFileHandler, scope=Scope.REQUEST)
    submit_handler = provide(SubmitDepositionHandler, scope=Scope.REQUEST)
    delete_files_handler = provide(DeleteDepositionFilesHandler, scope=Scope.REQUEST)
