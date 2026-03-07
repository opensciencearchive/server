"""FeatureReader port — cross-domain read port for feature data enrichment."""

from __future__ import annotations

from typing import Any, Protocol

from osa.domain.shared.model.srn import RecordSRN


class FeatureReader(Protocol):
    async def get_features_for_record(
        self, record_srn: RecordSRN
    ) -> dict[str, list[dict[str, Any]]]:
        """Return {hook_name: [row_dicts]} for all feature tables.

        Returns {} when no feature tables exist or record has no feature data.
        Excludes auto columns (id, created_at) from row dicts.
        """
        ...
