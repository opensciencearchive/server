"""ConvertDepositionToRecord - creates records when depositions are approved."""

from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.service import RecordService
from osa.domain.shared.event import EventHandler
from osa.domain.shared.model.source import DepositionSource


class ConvertDepositionToRecord(EventHandler[DepositionApproved]):
    """Creates and persists records when depositions are approved.

    This handler delegates to RecordService for all business logic.
    """

    service: RecordService

    async def handle(self, event: DepositionApproved) -> None:
        """Build a RecordDraft from DepositionApproved and publish."""
        draft = RecordDraft(
            source=DepositionSource(id=str(event.deposition_srn)),
            metadata=event.metadata,
            convention_srn=event.convention_srn,
            expected_features=event.expected_features,
        )
        await self.service.publish_record(draft)
