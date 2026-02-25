"""Feature table value object — represents a physical SQL table for hook features."""

from osa.domain.shared.model.hook import FeatureSchema
from osa.domain.shared.model.value import ValueObject


class FeatureTable(ValueObject):
    """Describes a physical SQL table for storing hook-derived features.

    Deterministically derived from convention ID + hook manifest.
    Not stored on Convention — computed by FeatureStore.
    """

    convention_id: str  # todo: use a NewType
    hook_name: str  # possibly use a NewType?
    pg_schema: str  # possibly use a NewType?
    table_name: str  # possibly use a NewType?
    feature_schema: FeatureSchema
