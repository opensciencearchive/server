from dishka import provide

from osa.config import Config
from osa.domain.semantics.command.create_ontology import CreateOntologyHandler
from osa.domain.semantics.command.create_schema import CreateSchemaHandler
from osa.domain.semantics.command.import_ontology import ImportOntologyHandler
from osa.domain.semantics.port.ontology_repository import OntologyRepository
from osa.domain.semantics.port.schema_repository import SchemaRepository
from osa.domain.semantics.query.get_ontology import GetOntologyHandler
from osa.domain.semantics.query.get_schema import GetSchemaHandler
from osa.domain.semantics.query.list_ontologies import ListOntologiesHandler
from osa.domain.semantics.query.list_schemas import ListSchemasHandler
from osa.domain.semantics.service.ontology import OntologyService
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.model.srn import Domain
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class SemanticsProvider(Provider):
    # Services
    @provide(scope=Scope.UOW)
    def get_ontology_service(
        self, ontology_repo: OntologyRepository, config: Config
    ) -> OntologyService:
        return OntologyService(
            ontology_repo=ontology_repo,
            node_domain=Domain(config.server.domain),
        )

    @provide(scope=Scope.UOW)
    def get_schema_service(
        self,
        schema_repo: SchemaRepository,
        ontology_repo: OntologyRepository,
        config: Config,
    ) -> SchemaService:
        return SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain(config.server.domain),
        )

    # Command Handlers
    create_ontology_handler = provide(CreateOntologyHandler, scope=Scope.UOW)
    create_schema_handler = provide(CreateSchemaHandler, scope=Scope.UOW)
    import_ontology_handler = provide(ImportOntologyHandler, scope=Scope.UOW)

    # Query Handlers
    get_ontology_handler = provide(GetOntologyHandler, scope=Scope.UOW)
    get_schema_handler = provide(GetSchemaHandler, scope=Scope.UOW)
    list_ontologies_handler = provide(ListOntologiesHandler, scope=Scope.UOW)
    list_schemas_handler = provide(ListSchemasHandler, scope=Scope.UOW)
