"""ConvertDepositionToRecord - creates records when depositions are approved."""

from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.record.service import RecordService
from osa.domain.shared.event import EventHandler


class ConvertDepositionToRecord(EventHandler[DepositionApproved]):
    """Creates and persists records when depositions are approved.

    This handler delegates to RecordService for all business logic.
    """

    service: RecordService

    async def handle(self, event: DepositionApproved) -> None:
        """Delegate to RecordService to create and publish the record."""
        await self.service.publish_record(
            deposition_srn=event.deposition_srn,
            metadata=event.metadata,
            convention_srn=event.convention_srn,
            hooks=event.hooks,
            files_dir=event.files_dir,
        )
