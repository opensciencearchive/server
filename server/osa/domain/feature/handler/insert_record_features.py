"""InsertRecordFeatures — deferred feature insertion on record publication."""

from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler
from osa.domain.validation.service.hook_registry import HookRegistryService


class InsertRecordFeatures(EventHandler[RecordPublished]):
    """Reads hook outputs from storage and inserts features with record_srn.

    Resolves the hook output directory from the record's source via the
    feature storage port, then delegates to FeatureService for insertion.
    """

    feature_service: FeatureService
    feature_storage: FeatureStoragePort
    hook_registry: HookRegistryService

    async def handle(self, event: RecordPublished) -> None:
        hook_output_dir = self.feature_storage.get_hook_output_root(
            event.source.type, event.source.id
        )
        # Deposition records carry the deposition SRN as their source id; the
        # hook_runs were recorded against it, so resolve {hook_name: run_id}.
        run_ids = await self.hook_registry.run_ids_for_deposition(event.source.id)
        await self.feature_service.insert_features_for_record(
            hook_output_dir=hook_output_dir,
            record_srn=str(event.record_srn),
            run_ids=run_ids,
            expected_features=event.expected_features,
        )
