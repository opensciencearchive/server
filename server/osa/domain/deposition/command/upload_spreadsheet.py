from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.deposition.port.spreadsheet import SpreadsheetParseResult, SpreadsheetPort
from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import DepositionSRN


class UploadSpreadsheet(Command):
    srn: DepositionSRN
    content: bytes


class SpreadsheetUploaded(Result):
    parse_result: SpreadsheetParseResult


class UploadSpreadsheetHandler(CommandHandler[UploadSpreadsheet, SpreadsheetUploaded]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    deposition_service: DepositionService
    convention_repo: ConventionRepository
    schema_reader: SchemaReader
    spreadsheet: SpreadsheetPort

    async def run(self, cmd: UploadSpreadsheet) -> SpreadsheetUploaded:
        dep = await self.deposition_service.get(cmd.srn)

        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {dep.convention_srn}")

        schema = await self.schema_reader.get_schema(convention.schema_srn)
        if schema is None:
            raise NotFoundError(f"Schema not found: {convention.schema_srn}")

        parse_result = self.spreadsheet.parse_upload(schema, cmd.content)

        if not parse_result.errors:
            await self.deposition_service.update_metadata(cmd.srn, parse_result.metadata)

        return SpreadsheetUploaded(parse_result=parse_result)
