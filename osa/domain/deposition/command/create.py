import logfire

from osa.domain.deposition.service.deposition import DepositionService
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


class CreateDeposition(Command):
    convention_srn: ConventionSRN


class DepositionCreated(Result):
    srn: DepositionSRN


class CreateDepositionHandler(CommandHandler[CreateDeposition, DepositionCreated]):
    deposition_service: DepositionService

    async def run(self, cmd: CreateDeposition) -> DepositionCreated:
        # TODO: Logfire span
        logfire.info("Deposition started", cmd=cmd)

        # Mock SRN for now since we don't have ID generation wired up
        mock_srn = DepositionSRN.parse("urn:osa:mock-node:dep:mock-id")

        return DepositionCreated(srn=mock_srn)
