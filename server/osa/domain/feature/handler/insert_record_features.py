"""InsertRecordFeatures — deferred feature insertion on record publication."""

from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler


class InsertRecordFeatures(EventHandler[RecordPublished]):
    """Reads hook outputs from storage and inserts features with record_srn.

    Resolves the hook output directory from the record's source via the
    feature storage port, then delegates to FeatureService for insertion.
    """

    feature_service: FeatureService
    feature_storage: FeatureStoragePort

    async def handle(self, event: RecordPublished) -> None:
        hook_output_dir = self.feature_storage.get_hook_output_root(
            event.source.type, event.source.id
        )
        await self.feature_service.insert_features_for_record(
            hook_output_dir=hook_output_dir,
            record_srn=str(event.record_srn),
            expected_features=event.expected_features,
        )
