"""Core data types — pure types with no behaviour beyond validation."""

from osa.types.files import File, FileCollection
from osa.types.record import Record
from osa.types.schema import Field, MetadataSchema
from osa.types.source import SourceFileRef, SourceRecord

__all__ = [
    "Field",
    "File",
    "FileCollection",
    "MetadataSchema",
    "Record",
    "SourceFileRef",
    "SourceRecord",
]
