from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.model.value import DepositionFile
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class UploadFile(Command):
    srn: DepositionSRN
    filename: str
    content: bytes
    size: int


class FileUploaded(Result):
    file: DepositionFile


class UploadFileHandler(CommandHandler[UploadFile, FileUploaded]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: UploadFile) -> FileUploaded:
        dep = await self.deposition_service.upload_file(
            cmd.srn,
            cmd.filename,
            cmd.content,
            cmd.size,
        )
        return FileUploaded(file=dep.files[-1])
