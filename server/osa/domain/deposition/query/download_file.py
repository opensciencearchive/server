from collections.abc import AsyncIterator

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class DownloadFile(Query):
    srn: DepositionSRN
    filename: str


class FileStream(Result, arbitrary_types_allowed=True):
    stream: AsyncIterator[bytes]
    filename: str
    size: int
    content_type: str | None


class DownloadFileHandler(QueryHandler[DownloadFile, FileStream]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService

    async def run(self, cmd: DownloadFile) -> FileStream:
        stream, file_meta = await self.deposition_service.get_file_download(cmd.srn, cmd.filename)
        return FileStream(
            stream=stream,
            filename=file_meta.name,
            size=file_meta.size,
            content_type=file_meta.content_type,
        )
