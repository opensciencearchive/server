from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class DeleteFile(Command):
    srn: DepositionSRN
    filename: str


class FileDeleted(Result):
    pass


class DeleteFileHandler(CommandHandler[DeleteFile, FileDeleted]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: DeleteFile) -> FileDeleted:
        await self.deposition_service.delete_file(cmd.srn, cmd.filename)
        return FileDeleted()
