"""Custom Dishka scopes for OSA."""

from dishka import BaseScope, new_scope


class Scope(BaseScope):  # type: ignore[misc]  # BaseScope is designed to be subclassed
    """OSA dependency injection scopes.

    Hierarchy: APP -> UOW

    - APP: Application lifetime (singletons)
    - UOW: Unit of Work (HTTP requests and background event handling)
    """

    APP = new_scope("APP")
    UOW = new_scope("UOW")
