import abc
import re
from typing import ClassVar, Generic, Literal, Optional, Self, TypeGuard, TypeVar

from pydantic import BaseModel, Field, field_validator


# TODO: test this
_SRN_RE = re.compile(
    r"^urn:osa:(?P<node>(?:nuuid|dns|nkey)_[a-z0-9.\-]+):"
    r"(?P<type>rec|dep|schema|assur|val|snap|evt):"
    r"(?P<local>[a-z0-9\-]{20,64})"
    r"(?:@(?P<ver>[0-9]+|[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9a-z.\-]+)?))?$"
)


class InvalidSRN(ValueError):
    """The SRN is invalid"""

    # TODO: Use t-strings

    # template: ClassVar[Template] = t"{summary}: {detail}"
    #
    # def __init__(self, *args: Any, reason: str, **kwargs: Any) -> None:
    #     super().__init__(reason, *args, **kwargs)
    #     self._reason = reason
    #
    # def __str__(self) -> str:
    #     return t""


V = TypeVar("V", str, int)
SRNType = Literal["rec", "dep", "schema", "assur", "val", "snap", "evt"]


def guard_srn_type(v: str) -> TypeGuard[SRNType]:
    srn_types: set[SRNType] = {"rec", "dep", "schema", "assur", "val", "snap", "evt"}
    return v in srn_types


class SRN(BaseModel[str], abc.ABC, Generic[V]):
    _allowed_typ: ClassVar[SRNType]

    domain: Literal["osa"] = "osa"
    node: str = Field()  # TODO: validate lowercase, ascii, whitespace, and more?
    typ: SRNType
    id: str
    version: Optional[V] = None

    ### CONSTRUCT ###

    def __str__(self) -> str:
        """Render the SRN as a string"""
        base = f"urn:{self.domain}:{self.node}:{self.typ}:{self.id}"
        return f"{base}@{self.version}" if self.version else base

    ### VALIDATION ###

    @classmethod
    def from_string(cls, v: str) -> Self:
        """Validate a string as a SRN"""
        cls._validate_lower(v)
        cls._validate_ascii(v)
        cls._validate_whitespace(v)

        node, typ, id, ver = cls._split(v)
        if not guard_srn_type(typ):
            raise InvalidSRN(f"Unknown SRN type: {typ}")
        if typ != cls._allowed_typ:
            raise InvalidSRN(f"Invalid type. Expected {cls._allowed_typ}, got {typ}")

        if cls._can_have_semver(typ):
            cls._validate_semver(ver)
        if cls._can_have_intver(typ):
            cls._validate_intver(ver)

        return cls(node=node, typ=typ, id=id, version=ver if ver else None)

    @staticmethod
    def _can_have_semver(typ: Optional[str]):
        return typ in {"schema", "assur"}

    @staticmethod
    def _can_have_intver(typ: Optional[str]):
        return typ in {"record"}

    @staticmethod
    def _validate_semver(ver: Optional[str]):
        if ver and not re.match(r"^\d+\.\d+\.\d+", ver):
            raise InvalidSRN(f"Not a valid SemVer: {ver}")

    @staticmethod
    def _validate_intver(ver: Optional[str]):
        if ver and not ver.isdigit():
            raise InvalidSRN(f"Not a valid integer version: {ver}")

    @staticmethod
    def _split(v: str) -> tuple[str, str, str, Optional[str]]:
        match = _SRN_RE.match(v)
        if not match:
            raise InvalidSRN(f"Invalid SRN format: {v}")

        return (
            match.group("node"),
            match.group("type"),
            match.group("local"),
            match.group("ver"),
        )

    @staticmethod
    def _validate_lower(v: str):
        if v != v.lower():
            raise InvalidSRN("SRN must be lowercase")

    @staticmethod
    def _validate_ascii(v: str):
        if not v.isascii():
            raise InvalidSRN("SRN must be ascii")

    @staticmethod
    def _validate_whitespace(v: str):
        if v != v.strip():
            raise InvalidSRN("SRN cannot have whitespace")


class RecordSRN(SRN):
    """urn:osa:{node}:rec:{local}@{N}"""

    _allowed_typ = "rec"


class DepositionSRN(SRN):
    """urn:osa:{node}:dep:{local}"""

    _allowed_type = "dep"


class SchemaSRN(SRN):
    """urn:osa:{node}:schema:{name}@{MAJOR.MINOR.PATCH}"""

    _allowed_type = "schema"


class AssuranceSRN(SRN):
    """urn:osa:{node}:assur:{name}@{MAJOR.MINOR.PATCH}"""

    _allowed_type = "assur"


class ValidationSRN(SRN):
    """urn:osa:{node}:val:{local}"""

    _allowed_type = "val"


class SnapshotSRN(SRN):
    """urn:osa:{node}:snap:{stamp}"""

    _allowed_type = "snap"


class EventSRN(SRN):
    """urn:osa:{node}:evt:{local}"""

    _allowed_type = "evt"
