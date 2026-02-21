"""OSA Python SDK — hooks and conventions for the Open Scientific Archive."""

from osa.authoring.convention import convention
from osa.authoring.hook import hook
from osa.authoring.validator import Reject
from osa.types.record import Record
from osa.types.schema import Field, MetadataSchema

__all__ = [
    "Field",
    "MetadataSchema",
    "Record",
    "Reject",
    "convention",
    "hook",
]
