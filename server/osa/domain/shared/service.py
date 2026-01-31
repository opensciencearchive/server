from dataclasses import dataclass
from typing import dataclass_transform


@dataclass_transform()
class _ServiceMeta(type):
    """Metaclass that applies @dataclass to subclasses."""

    def __new__(mcs, name: str, bases: tuple, namespace: dict):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            return dataclass(cls)
        return cls


class Service(metaclass=_ServiceMeta):
    """Base class for domain services. Subclasses are automatically dataclasses."""

    pass
