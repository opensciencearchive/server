from dishka import provide

from osa.config import Config
from osa.domain.deposition.command.create import CreateDepositionHandler
from osa.domain.deposition.command.create_convention import CreateConventionHandler
from osa.domain.deposition.command.delete_files import DeleteFileHandler
from osa.domain.deposition.command.submit import SubmitDepositionHandler
from osa.domain.deposition.command.update import UpdateMetadataHandler
from osa.domain.deposition.command.upload import UploadFileHandler
from osa.domain.deposition.command.upload_spreadsheet import UploadSpreadsheetHandler
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.deposition.port.spreadsheet import SpreadsheetPort
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.deposition.query.download_file import DownloadFileHandler
from osa.domain.deposition.query.download_template import DownloadTemplateHandler
from osa.domain.deposition.query.get_convention import GetConventionHandler
from osa.domain.deposition.query.get_deposition import GetDepositionHandler
from osa.domain.deposition.query.list_conventions import ListConventionsHandler
from osa.domain.deposition.query.list_depositions import ListDepositionsHandler
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.feature.service.feature import FeatureService
from osa.infrastructure.persistence.adapter.spreadsheet import OpenpyxlSpreadsheetAdapter
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class DepositionProvider(Provider):
    @provide(scope=Scope.UOW)
    def get_deposition_service(
        self,
        deposition_repo: DepositionRepository,
        convention_repo: ConventionRepository,
        file_storage: FileStoragePort,
        outbox: Outbox,
        config: Config,
    ) -> DepositionService:
        return DepositionService(
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            outbox=outbox,
            node_domain=Domain(config.server.domain),
        )

    @provide(scope=Scope.UOW)
    def get_convention_service(
        self,
        convention_repo: ConventionRepository,
        schema_reader: SchemaReader,
        feature_service: FeatureService,
        config: Config,
    ) -> ConventionService:
        return ConventionService(
            convention_repo=convention_repo,
            schema_reader=schema_reader,
            feature_service=feature_service,
            node_domain=Domain(config.server.domain),
        )

    @provide(scope=Scope.APP)
    def get_spreadsheet_port(self) -> SpreadsheetPort:
        return OpenpyxlSpreadsheetAdapter()

    # Command Handlers
    create_handler = provide(CreateDepositionHandler, scope=Scope.UOW)
    submit_handler = provide(SubmitDepositionHandler, scope=Scope.UOW)
    update_handler = provide(UpdateMetadataHandler, scope=Scope.UOW)
    upload_handler = provide(UploadFileHandler, scope=Scope.UOW)
    delete_file_handler = provide(DeleteFileHandler, scope=Scope.UOW)
    upload_spreadsheet_handler = provide(UploadSpreadsheetHandler, scope=Scope.UOW)
    create_convention_handler = provide(CreateConventionHandler, scope=Scope.UOW)

    # Query Handlers
    get_deposition_handler = provide(GetDepositionHandler, scope=Scope.UOW)
    download_template_handler = provide(DownloadTemplateHandler, scope=Scope.UOW)
    get_convention_handler = provide(GetConventionHandler, scope=Scope.UOW)
    list_conventions_handler = provide(ListConventionsHandler, scope=Scope.UOW)
    list_depositions_handler = provide(ListDepositionsHandler, scope=Scope.UOW)
    download_file_handler = provide(DownloadFileHandler, scope=Scope.UOW)
