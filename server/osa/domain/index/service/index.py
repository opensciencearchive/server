"""IndexService - orchestrates indexing of records into storage backends."""

import logging
from typing import Any

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.model.srn import RecordSRN
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class IndexService(Service):
    """Projects records into configured index backends.

    This service encapsulates the business logic for indexing that was previously
    embedded in the ProjectNewRecordToIndexes listener. It can be called from
    multiple entry points (event listeners, CLI commands, bulk operations).
    """

    indexes: IndexRegistry

    async def index_record(
        self,
        record_srn: RecordSRN,
        metadata: dict[str, Any],
    ) -> None:
        """Index a record into all configured backends.

        Args:
            record_srn: SRN of the record to index.
            metadata: The record metadata to index.

        Note:
            This method logs errors but does not raise exceptions for individual
            backend failures, allowing indexing to continue for other backends.
        """
        srn_str = str(record_srn)

        for name, backend in self.indexes.items():
            try:
                await backend.ingest(srn_str, metadata)
                logger.debug(f"Indexed {srn_str} into backend '{name}'")
            except Exception as e:
                logger.error(f"Failed to index {srn_str} into '{name}': {e}")
