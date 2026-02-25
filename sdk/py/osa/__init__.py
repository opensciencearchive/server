"""OSA Python SDK — hooks and conventions for the Open Scientific Archive."""

from osa.authoring.convention import convention
from osa.authoring.hook import hook
from osa.authoring.source import Source
from osa.authoring.validator import Reject
from osa.runtime.source_context import SourceContext
from osa.types.record import Record
from osa.types.schema import Field, MetadataSchema
from osa.types.source import InitialRun, SourceFileRef, SourceRecord, SourceSchedule

# Schema is a user-friendly alias for MetadataSchema
Schema = MetadataSchema

__all__ = [
    "Field",
    "InitialRun",
    "MetadataSchema",
    "Record",
    "Reject",
    "Schema",
    "Source",
    "SourceContext",
    "SourceFileRef",
    "SourceRecord",
    "SourceSchedule",
    "convention",
    "hook",
]
