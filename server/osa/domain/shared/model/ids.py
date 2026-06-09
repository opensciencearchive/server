"""Central semantic ID types used across the ``/data/`` read surface.

Per OSA's type-safety convention, semantic identifiers cross module
boundaries as ``NewType`` aliases rather than bare ``str``. ``HookName`` is
re-exported from :mod:`osa.domain.shared.model.hook` (its source of truth,
where the PG-identifier pattern validation lives) so callers have a single
import location for the IDs this surface deals in.
"""

from __future__ import annotations

from typing import NewType

from osa.domain.shared.model.hook import HookName

# Bare internal record identifier (UUIDv7 / ULID). Validation of the exact
# charset/length happens at the API boundary; internally it is opaque.
RecordId = NewType("RecordId", str)

__all__ = ["RecordId", "HookName"]
