"""Index domain listeners."""

from osa.domain.index.listener.flush_listener import FlushIndexesOnSourceComplete
from osa.domain.index.listener.index_projector import ProjectNewRecordToIndexes

__all__ = ["FlushIndexesOnSourceComplete", "ProjectNewRecordToIndexes"]
