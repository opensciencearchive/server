"""Generic record container for OSA depositions."""

from datetime import datetime
from typing import Generic, TypeVar

from osa.types.files import FileCollection
from osa.types.schema import MetadataSchema

T = TypeVar("T", bound=MetadataSchema)


class Record(Generic[T]):
    """A scientific data record parameterized by its metadata schema.

    Provides typed access to metadata, associated files, and record identity.
    """

    __slots__ = ("_id", "_srn", "_created_at", "_metadata", "_files")

    def __init__(
        self,
        *,
        id: str,
        created_at: datetime,
        metadata: T,
        files: FileCollection,
        srn: str = "",
    ) -> None:
        self._id = id
        self._srn = srn
        self._created_at = created_at
        self._metadata = metadata
        self._files = files

    @property
    def id(self) -> str:
        """Unique record identifier."""
        return self._id

    @property
    def srn(self) -> str:
        """Structured Resource Name (deposition SRN during validation)."""
        return self._srn

    @property
    def created_at(self) -> datetime:
        """Timestamp of record creation."""
        return self._created_at

    @property
    def metadata(self) -> T:
        """Typed metadata instance."""
        return self._metadata

    @property
    def files(self) -> FileCollection:
        """Associated data files."""
        return self._files
