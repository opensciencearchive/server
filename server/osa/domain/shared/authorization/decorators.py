"""Repository method decorators for resource-level authorization.

@reads(check): After method returns, check the result (skip if None).
@writes(check): Before method runs, check the first resource arg.

Both decorators access self._identity on the repo instance.
"""

from collections.abc import Callable
from functools import wraps
from typing import Any

from osa.domain.shared.authorization.resource import ResourceCheck


def reads(check: ResourceCheck) -> Callable:
    """After method returns, evaluate the check on the result.

    If the result is None (not found), the check is skipped.
    """

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = await fn(self, *args, **kwargs)
            if result is not None:
                check.evaluate(self._identity, result)
            return result

        return wrapper

    return decorator


def writes(check: ResourceCheck) -> Callable:
    """Before method runs, evaluate the check on the first resource arg."""

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(self: Any, resource: Any, *args: Any, **kwargs: Any) -> Any:
            check.evaluate(self._identity, resource)
            return await fn(self, resource, *args, **kwargs)

        return wrapper

    return decorator
