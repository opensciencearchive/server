from __future__ import annotations

import re
from enum import Enum
from string import Template
from typing import ClassVar, Generic, Self, Type, TypeVar, Union

from pydantic import (
    Field,
    RootModel,
    field_validator,
)

from osa.domain.shared.model.value import ValueObject

# ---------- Atomic (RootModel) parts ----------

T = TypeVar("T", bound=Union[str, int])


class Domain(RootModel[str]):
    """
    Node identity segment: a DNS domain name.
    Examples: osap.org, archive.university.edu, localhost
    """

    _re: ClassVar[re.Pattern] = re.compile(r"^[a-z0-9]([a-z0-9\-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9\-]*[a-z0-9])?)*$")

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid Domain (expected DNS domain name)")
        return v


class LocalId(RootModel[str]):
    """
    Opaque, node-scoped identifier (prefer UUIDv7/ULID; we only enforce charset/length here).
    """

    _re: ClassVar[re.Pattern] = re.compile(r"^[a-z0-9\-]{3,64}$")

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid LocalId (20–64 chars, [a-z0-9-])")
        return v


class Version(RootModel[T], Generic[T]):
    @classmethod
    def from_string(cls, s: str) -> "Version":
        raise NotImplementedError


class Semver(Version[str]):
    _re: ClassVar[re.Pattern] = re.compile(
        r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9a-z\-\.]+)?(?:\+[0-9a-z\-\.]+)?$"
    )

    @classmethod
    def from_string(cls, s: str) -> "Semver":
        return Semver.model_validate(s)

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid SemVer")
        return v

    def __str__(self) -> str:
        return self.root


class RecordVersion(Version[int]):
    @classmethod
    def from_string(cls, s: str) -> "RecordVersion":
        return RecordVersion.model_validate(int(s))

    @field_validator("root")
    @classmethod
    def _validate(cls, v: int) -> int:
        if v < 1:
            raise ValueError("record version must be >= 1")
        return v

    def __str__(self) -> str:
        return str(self.root)


# ---------- Shared enums / constants ----------


class ResourceType(str, Enum):
    rec = "rec"
    dep = "dep"
    schema = "schema"
    vocab = "vocab"
    snap = "snap"
    evt = "evt"
    val = "val"


URN_SCHEME = "urn"
URN_NID = "osa"

# ---------- Base SRN (parts stored as fields) ----------

S = TypeVar("S", bound="SRN")


class SRN(ValueObject):
    """
    Base SRN model: urn:osa:{domain}:{type}:{id}[@version]
    Stores parts, provides parse/render, and light invariants.
    Subclasses can tighten version rules per resource type.
    """

    scheme: str = Field(default=URN_SCHEME, frozen=True)
    nid: str = Field(default=URN_NID, frozen=True)
    domain: Domain
    type: ResourceType
    id: LocalId
    version: Union[Version, None] = Field(default=None)

    _tpl: ClassVar[Template] = Template("urn:osa:${domain}:${type}:${id}${version}")

    @field_validator("scheme")
    @classmethod
    def _scheme_ok(cls, v: str) -> str:
        if v != URN_SCHEME:
            raise ValueError("scheme must be 'urn'")
        return v

    @field_validator("nid")
    @classmethod
    def _nid_ok(cls, v: str) -> str:
        if v != URN_NID:
            raise ValueError("nid must be 'osa'")
        return v

    def __str__(self) -> str:
        return self.render()

    def render(self) -> str:
        ver = ""
        if self.version is not None:
            ver = f"@{self.version}"
        return self._tpl.substitute(
            domain=self.domain.root,
            type=self.type.value,
            id=self.id.root,
            version=ver,
        )

    # ---------- factory & parsing ----------

    @staticmethod
    def _extract_parts(srn: str) -> tuple[str, str, str, Version | None]:
        """
        Extract parts from SRN string.
        Returns (domain, type, id, version).
        Raises ValueError if malformed.
        """
        srn = srn.strip().lower()
        if not srn.startswith("urn:osa:"):
            raise ValueError("not an OSA SRN")
        parts = srn.split(":")
        if len(parts) != 5:
            raise ValueError(
                "malformed SRN (expected urn:osa:{domain}:{type}:{id}[...])"
            )
        _, _, domain, typ, rest = (
            parts[0],
            parts[1],
            parts[2],
            parts[3],
            parts[4],
        )
        if "@" in rest:
            id_str, ver_str = rest.split("@", 1)
            try:
                version = Semver.from_string(ver_str)
            except ValueError:
                try:
                    version = RecordVersion.from_string(ver_str)
                except ValueError:
                    raise ValueError("invalid version format in SRN")
        else:
            id_str, version = rest, None

        return domain, typ, id_str, version

    @classmethod
    def parse_as(cls, srn: str, type_: Type[S]) -> S:
        domain, typ, id_str, version = cls._extract_parts(srn)
        return type_(
            domain=Domain(domain),
            type=ResourceType(typ),
            id=LocalId(id_str),
            version=version,
        )

    @classmethod
    def parse(cls, srn: str) -> Self:
        domain, typ, id_str, version = cls._extract_parts(srn)
        return cls(
            domain=Domain(domain),
            type=ResourceType(typ),
            id=LocalId(id_str),
            version=version,
        )


# ---------- Per-type SRNs (tighten version constraints) ----------


class RecordSRN(SRN):
    type: ResourceType = Field(default=ResourceType.rec, frozen=True)
    version: RecordVersion  # type: ignore


class SchemaSRN(SRN):
    type: ResourceType = Field(default=ResourceType.schema, frozen=True)
    version: Semver  # type: ignore


class VocabSRN(SRN):
    type: ResourceType = Field(default=ResourceType.vocab, frozen=True)
    version: Semver  # type: ignore


class DepositionSRN(SRN):
    type: ResourceType = Field(default=ResourceType.dep, frozen=True)
    version: None = None  # type: ignore


class ValidationRunSRN(SRN):
    type: ResourceType = Field(default=ResourceType.val, frozen=True)
    version: None = None  # type: ignore


class SnapshotSRN(SRN):
    type: ResourceType = Field(default=ResourceType.snap, frozen=True)
    version: None = None  # type: ignore


class EventSRN(SRN):
    type: ResourceType = Field(default=ResourceType.evt, frozen=True)
    version: None = None  # type: ignore
