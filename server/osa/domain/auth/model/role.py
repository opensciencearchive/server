"""Role hierarchy for authorization."""

from enum import IntEnum


class Role(IntEnum):
    """Hierarchical roles with numeric ordering.

    Higher values inherit all permissions of lower values.
    Gaps allow future role insertion without renumbering.
    """

    PUBLIC = 0
    DEPOSITOR = 10
    CURATOR = 20
    ADMIN = 30
    SUPERADMIN = 40
