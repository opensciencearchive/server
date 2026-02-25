"""InsertRecordFeatures — deferred feature insertion on record publication."""

from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler


class InsertRecordFeatures(EventHandler[RecordPublished]):
    """Reads hook outputs from cold storage and inserts features with record_srn.

    Triggered after a record is published. Delegates to FeatureService which
    looks up the deposition's convention, reads features.json from each hook's
    output directory, and inserts them into the feature tables.
    """

    feature_service: FeatureService

    async def handle(self, event: RecordPublished) -> None:
        # todo: error handling?
        await self.feature_service.insert_features_for_record(
            event.deposition_srn, str(event.record_srn)
        )
