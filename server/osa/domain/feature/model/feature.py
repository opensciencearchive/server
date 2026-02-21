"""Feature table value object — represents a physical SQL table for hook features."""

from osa.domain.shared.model.hook import FeatureSchema
from osa.domain.shared.model.value import ValueObject


class FeatureTable(ValueObject):
    """Describes a physical SQL table for storing hook-derived features.

    Deterministically derived from convention ID + hook manifest.
    Not stored on Convention — computed by FeatureStore.
    """

    convention_id: str
    hook_name: str
    pg_schema: str
    table_name: str
    feature_schema: FeatureSchema
