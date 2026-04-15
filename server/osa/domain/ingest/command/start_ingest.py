"""StartIngest command — initiates a bulk ingestion run for a convention."""

from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result


class StartIngest(Command):
    """Start an ingest run for a convention."""

    convention_srn: str
    batch_size: int = 1000
    limit: int | None = None  # Max total records to ingest (None = unlimited)


class IngestRunCreated(Result):
    """Result of starting an ingest run."""

    srn: str
    convention_srn: str
    status: str
    started_at: str


class StartIngestHandler(CommandHandler[StartIngest, IngestRunCreated]):
    """Thin command handler — delegates to IngestService."""

    __auth__ = at_least(Role.ADMIN)

    from osa.domain.auth.model.principal import Principal
    from osa.domain.ingest.service.ingest import IngestService

    principal: Principal
    service: IngestService

    async def run(self, cmd: StartIngest) -> IngestRunCreated:
        from osa.domain.shared.model.srn import Domain

        ingest_run = await self.service.start_ingest(
            convention_srn=cmd.convention_srn,
            batch_size=cmd.batch_size,
            limit=cmd.limit,
        )

        node_domain: Domain = self.service.node_domain
        srn = f"urn:osa:{node_domain.root}:ing:{ingest_run.id}"

        return IngestRunCreated(
            srn=srn,
            convention_srn=ingest_run.convention_srn,
            status=ingest_run.status,
            started_at=ingest_run.started_at.isoformat(),
        )
