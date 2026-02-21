"""Decorators and registration functions for authoring hooks and conventions."""

from osa.authoring.convention import convention
from osa.authoring.hook import hook
from osa.authoring.validator import Reject

__all__ = [
    "Reject",
    "convention",
    "hook",
]
