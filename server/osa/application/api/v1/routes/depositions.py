"""Deposition REST routes."""

import re
from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, UploadFile
from fastapi.responses import StreamingResponse

from osa.domain.deposition.command.create import (
    CreateDeposition,
    CreateDepositionHandler,
    DepositionCreated,
)
from osa.domain.deposition.command.delete_files import (
    DeleteFile,
    DeleteFileHandler,
    FileDeleted,
)
from osa.domain.deposition.command.submit import (
    DepositionSubmitted,
    SubmitDeposition,
    SubmitDepositionHandler,
)
from osa.domain.deposition.command.update import (
    MetadataUpdated,
    UpdateMetadata,
    UpdateMetadataHandler,
)
from osa.domain.deposition.command.upload import (
    FileUploaded,
    UploadFile as UploadFileCommand,
    UploadFileHandler,
)
from osa.domain.deposition.command.upload_spreadsheet import (
    SpreadsheetUploaded,
    UploadSpreadsheet,
    UploadSpreadsheetHandler,
)
from osa.domain.deposition.query.download_file import (
    DownloadFile,
    DownloadFileHandler,
)
from osa.domain.deposition.query.download_template import (
    DownloadTemplate,
    DownloadTemplateHandler,
)
from osa.domain.deposition.query.get_deposition import (
    DepositionDetail,
    GetDeposition,
    GetDepositionHandler,
)
from osa.domain.deposition.query.list_depositions import (
    DepositionList,
    ListDepositions,
    ListDepositionsHandler,
)
from osa.domain.shared.model.srn import DepositionSRN

router = APIRouter(prefix="/depositions", tags=["Depositions"], route_class=DishkaRoute)


@router.post("", response_model=DepositionCreated, status_code=201)
async def create_deposition(
    body: CreateDeposition,
    handler: FromDishka[CreateDepositionHandler],
) -> DepositionCreated:
    return await handler.run(body)


@router.get("", response_model=DepositionList)
async def list_depositions(
    handler: FromDishka[ListDepositionsHandler],
) -> DepositionList:
    return await handler.run(ListDepositions())


@router.get("/{srn:path}/template")
async def download_template(
    srn: str,
    handler: FromDishka[GetDepositionHandler],
    template_handler: FromDishka[DownloadTemplateHandler],
) -> StreamingResponse:
    dep = await handler.run(GetDeposition(srn=DepositionSRN.parse(srn)))
    result = await template_handler.run(DownloadTemplate(convention_srn=dep.convention_srn))
    return StreamingResponse(
        iter([result.content]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{result.filename}"'},
    )


@router.post("/{srn:path}/spreadsheet", response_model=SpreadsheetUploaded)
async def upload_spreadsheet(
    srn: str,
    file: UploadFile,
    handler: FromDishka[UploadSpreadsheetHandler],
) -> SpreadsheetUploaded:
    content = await file.read()
    return await handler.run(UploadSpreadsheet(srn=DepositionSRN.parse(srn), content=content))


@router.post("/{srn:path}/files", response_model=FileUploaded)
async def upload_file(
    srn: str,
    file: UploadFile,
    handler: FromDishka[UploadFileHandler],
) -> FileUploaded:
    content = await file.read()
    return await handler.run(
        UploadFileCommand(
            srn=DepositionSRN.parse(srn),
            filename=file.filename or "unknown",
            content=content,
            size=len(content),
        )
    )


@router.get("/{srn:path}/files/{filename}")
async def download_file(
    srn: str,
    filename: str,
    handler: FromDishka[DownloadFileHandler],
) -> StreamingResponse:
    result = await handler.run(DownloadFile(srn=DepositionSRN.parse(srn), filename=filename))
    safe_name = _sanitize_header_filename(result.filename)
    return StreamingResponse(
        result.stream,
        media_type=result.content_type or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}"'},
    )


@router.delete("/{srn:path}/files/{filename}", response_model=FileDeleted)
async def delete_file(
    srn: str,
    filename: str,
    handler: FromDishka[DeleteFileHandler],
) -> FileDeleted:
    return await handler.run(DeleteFile(srn=DepositionSRN.parse(srn), filename=filename))


@router.patch("/{srn:path}/metadata", response_model=MetadataUpdated)
async def update_metadata(
    srn: str,
    body: dict[str, Any],
    handler: FromDishka[UpdateMetadataHandler],
) -> MetadataUpdated:
    return await handler.run(UpdateMetadata(srn=DepositionSRN.parse(srn), metadata=body))


@router.post("/{srn:path}/submit", response_model=DepositionSubmitted)
async def submit_deposition(
    srn: str,
    handler: FromDishka[SubmitDepositionHandler],
) -> DepositionSubmitted:
    return await handler.run(SubmitDeposition(srn=DepositionSRN.parse(srn)))


@router.get("/{srn:path}", response_model=DepositionDetail)
async def get_deposition(
    srn: str,
    handler: FromDishka[GetDepositionHandler],
) -> DepositionDetail:
    return await handler.run(GetDeposition(srn=DepositionSRN.parse(srn)))


def _sanitize_header_filename(filename: str) -> str:
    """Strip characters that could break Content-Disposition headers."""
    return re.sub(r'[\r\n"]', "_", filename)
