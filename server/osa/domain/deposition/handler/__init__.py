"""Deposition domain event handlers."""

from osa.domain.deposition.handler.create_deposition_from_source import (
    CreateDepositionFromSource,
)
from osa.domain.deposition.handler.return_to_draft import ReturnToDraft

__all__ = ["CreateDepositionFromSource", "ReturnToDraft"]
