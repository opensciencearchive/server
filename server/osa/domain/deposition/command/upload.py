from typing import Any

import logfire

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class UploadFile(Command):
    srn: DepositionSRN
    filename: str
    stream: Any  # BinaryIO or similar


class FileUploaded(Result):
    pass


class UploadFileHandler(CommandHandler[UploadFile, FileUploaded]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal

    async def run(self, cmd: UploadFile) -> FileUploaded:
        with logfire.span("UploadFile"):
            # TODO: Implement actual file storage logic
            logfire.info("File uploaded", filename=cmd.filename)
            return FileUploaded()
