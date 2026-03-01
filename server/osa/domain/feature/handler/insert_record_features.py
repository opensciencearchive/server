"""InsertRecordFeatures — deferred feature insertion on record publication."""

from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler


class InsertRecordFeatures(EventHandler[RecordPublished]):
    """Reads hook outputs from cold storage and inserts features with record_srn.

    Uses enriched RecordPublished event data (hooks list) instead of
    looking up the convention.
    """

    feature_service: FeatureService

    async def handle(self, event: RecordPublished) -> None:
        await self.feature_service.insert_features_for_record(
            deposition_srn=event.deposition_srn,
            record_srn=str(event.record_srn),
            hooks=event.hooks,
        )
