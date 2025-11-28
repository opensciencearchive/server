from typing import Any

import logfire

from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import DepositionSRN


class UploadFile(Command):
    srn: DepositionSRN
    filename: str
    stream: Any  # BinaryIO or similar


class FileUploaded(Result):
    pass


class UploadFileHandler(CommandHandler[UploadFile, FileUploaded]):
    def run(self, cmd: UploadFile) -> FileUploaded:
        with logfire.span("UploadFile"):
            # TODO: Implement actual file storage logic
            logfire.info("File uploaded", filename=cmd.filename)
            return FileUploaded()
