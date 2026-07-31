"""StartIngest command — initiates a bulk ingestion run for a convention."""

from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result


class StartIngest(Command):
    """Start an ingest run for a convention."""

    convention_id: str
    batch_size: int = 1000
    limit: int | None = None  # Max total records to ingest (None = unlimited)


class IngestRunCreated(Result):
    """Result of starting an ingest run."""

    srn: str
    convention_id: str
    status: str
    started_at: str


class StartIngestHandler(CommandHandler[StartIngest, IngestRunCreated]):
    """Thin command handler — delegates to IngestService."""

    # Triggering an ingestion run is authorized by the ``ingestions:write``
    # M2M scope OR an ADMIN role (server#190). The scope lets the hosted
    # control plane broker CLI/dashboard triggers with a per-node, single-scope
    # token; ADMIN keeps human/self-host access unchanged.
    __auth__ = requires_scope("ingestions:write")

    # TODO: do we ned these imports to be lazy?
    from osa.domain.auth.model.principal import Principal
    from osa.domain.ingest.service.ingest import IngestService

    principal: Principal
    service: IngestService

    async def run(self, cmd: StartIngest) -> IngestRunCreated:
        from osa.domain.shared.model.srn import Domain  # TODO: lazy needed?

        ingest_run = await self.service.start_ingest(
            convention_id=cmd.convention_id,
            batch_size=cmd.batch_size,
            limit=cmd.limit,
        )

        node_domain: Domain = self.service.node_domain
        srn = f"urn:osa:{node_domain.root}:ing:{ingest_run.id}"  # TODO: use SRN class to build

        return IngestRunCreated(
            srn=srn,
            convention_id=ingest_run.convention_id,
            status=ingest_run.status,
            started_at=ingest_run.started_at.isoformat(),
        )
