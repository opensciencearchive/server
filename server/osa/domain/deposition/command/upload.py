from typing import Any

import logfire

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class UploadFile(Command):
    srn: DepositionSRN
    filename: str
    stream: Any  # BinaryIO or similar


class FileUploaded(Result):
    pass


class UploadFileHandler(CommandHandler[UploadFile, FileUploaded]):
    __auth__ = requires_role(Role.DEPOSITOR)
    _principal: Principal | None = None

    async def run(self, cmd: UploadFile) -> FileUploaded:
        with logfire.span("UploadFile"):
            # TODO: Implement actual file storage logic
            logfire.info("File uploaded", filename=cmd.filename)
            return FileUploaded()
