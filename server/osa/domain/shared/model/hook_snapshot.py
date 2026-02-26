"""Serializable snapshot of hook configuration carried in event payloads.

Used across multiple domains' event payloads so consuming domains
have all data they need without querying the producing domain's repositories.
"""

from typing import Any

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.value import ValueObject


class HookSnapshot(ValueObject):
    """Snapshot of hook configuration carried in event payloads.

    Contains the subset of HookDefinition data that downstream
    consumers (validation, feature, source) need to operate
    without cross-domain imports.
    """

    name: str
    image: str
    digest: str = ""
    features: list[ColumnDef] = []
    config: dict[str, Any] = {}
