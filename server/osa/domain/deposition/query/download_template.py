from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.ontology_reader import OntologyReader
from osa.domain.deposition.port.schema_reader import SchemaReader
from osa.domain.deposition.port.spreadsheet import SpreadsheetPort
from osa.domain.semantics.model.value import TermConstraints
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class DownloadTemplate(Query):
    convention_srn: ConventionSRN


class TemplateResult(Result):
    content: bytes
    filename: str


class DownloadTemplateHandler(QueryHandler[DownloadTemplate, TemplateResult]):
    __auth__ = at_least(Role.DEPOSITOR)
    principal: Principal
    convention_repo: ConventionRepository
    schema_reader: SchemaReader
    ontology_reader: OntologyReader
    spreadsheet: SpreadsheetPort

    async def run(self, cmd: DownloadTemplate) -> TemplateResult:
        convention = await self.convention_repo.get(cmd.convention_srn)
        if convention is None:
            raise NotFoundError(f"Convention not found: {cmd.convention_srn}")

        schema = await self.schema_reader.get_schema(convention.schema_srn)
        if schema is None:
            raise NotFoundError(f"Schema not found: {convention.schema_srn}")

        # Collect ontology terms for fields that reference ontologies
        ontology_terms_by_srn: dict[str, list[str]] = {}
        for field in schema.fields:
            if isinstance(field.constraints, TermConstraints):
                onto_srn = field.constraints.ontology_srn
                if str(onto_srn) not in ontology_terms_by_srn:
                    onto = await self.ontology_reader.get_ontology(onto_srn)
                    if onto:
                        ontology_terms_by_srn[str(onto_srn)] = [t.term_id for t in onto.terms]

        content = self.spreadsheet.generate_template(schema, ontology_terms_by_srn)
        filename = f"{convention.title.lower().replace(' ', '_')}_template.xlsx"
        return TemplateResult(content=content, filename=filename)
