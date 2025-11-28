# python 3.12+ / pydantic v2
from __future__ import annotations

import re
from enum import Enum
from string import Template
from typing import Annotated, Generic, Optional, Type, TypeVar, Union

from pydantic import (
    Field,
    RootModel,
    field_validator,
    model_validator,
)

from osa.domain.shared.model.value import ValueObject

# ---------- Atomic (RootModel) parts ----------


class NodeId(RootModel[str]):
    """
    Node identity segment.
    Allowed forms (draft): nuuid_<ulid/uuid> | dns_<domain> | nkey_<pubkey-hash>
    """

    _re = re.compile(r"^(nuuid|dns|nkey)_[a-z0-9.\-]+$")

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid NodeId (expected nuuid_*, dns_*, or nkey_*)")
        return v


class LocalId(RootModel[str]):
    """
    Opaque, node-scoped identifier (prefer UUIDv7/ULID; we only enforce charset/length here).
    """

    _re = re.compile(r"^[a-z0-9\-]{20,64}$")

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid LocalId (20–64 chars, [a-z0-9-])")
        return v


class Semver(RootModel[str]):
    _re = re.compile(
        r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9a-z\-\.]+)?(?:\+[0-9a-z\-\.]+)?$"
    )

    @field_validator("root")
    @classmethod
    def _validate(cls, v: str) -> str:
        v = v.strip().lower()
        if not cls._re.match(v):
            raise ValueError("invalid SemVer")
        return v

    def __str__(self) -> str:
        return self.root


class RecordVersion(RootModel[int]):
    @field_validator("root")
    @classmethod
    def _validate(cls, v: int) -> int:
        if v < 1:
            raise ValueError("record version must be >= 1")
        return v

    def __str__(self) -> str:
        return str(self.root)


Version = Annotated[Union[Semver, RecordVersion], "SRN version segment"]

# ---------- Shared enums / constants ----------


class ResourceType(str, Enum):
    rec = "rec"
    dep = "dep"
    schema = "schema"
    guarantee = "guarantee"
    val = "val"
    snap = "snap"
    evt = "evt"
    profile = "profile"


URN_SCHEME = "urn"
URN_NID = "osa"

# ---------- Base SRN (parts stored as fields) ----------

V = TypeVar("V", bound=Union[Version, None])


class SRN(ValueObject, Generic[V]):
    """
    Base SRN model: urn:osa:{node}:{type}:{local}[@version]
    Stores parts, provides parse/render, and light invariants.
    Subclasses can tighten version rules per resource type.
    """

    scheme: str = Field(default=URN_SCHEME, frozen=True)
    nid: str = Field(default=URN_NID, frozen=True)
    node: NodeId
    type: ResourceType
    local: LocalId
    version: Union[V, None] = Field(default=None)

    _tpl = Template("urn:osa:${node}:${type}:${local}${version}")

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
            node=self.node.root,
            type=self.type.value,
            local=self.local.root,
            version=ver,
        )

    # ---------- factory & parsing ----------

    @classmethod
    def parse(cls, srn: str) -> SRN[Union[Version, None]]:
        """
        Parse any SRN into the appropriate subclass (when possible), else base SRN.
        """
        srn = srn.strip().lower()
        # quick sanity
        if not srn.startswith("urn:osa:"):
            raise ValueError("not an OSA SRN")
        # split into up to 5 segments: urn, osa, node, type, local[@version]
        parts = srn.split(":")
        if len(parts) < 5:
            raise ValueError(
                "malformed SRN (expected urn:osa:{node}:{type}:{local}[...])"
            )
        _, _, node, typ, rest = (
            parts[0],
            parts[1],
            parts[2],
            parts[3],
            ":".join(parts[4:]),
        )
        # rest can contain ':' only if local contains ':' (we prohibit), so split on '@'
        if "@" in rest:
            local_str, ver_str = rest.split("@", 1)
            version: Optional[Version] = _parse_version(ver_str)
        else:
            local_str, version = rest, None

        # dispatch to stricter subclasses
        t = ResourceType(typ)

        # Using Any to bypass static dict typing issues with generic classes
        srn_types: dict[ResourceType, Type[SRN]] = {
            ResourceType.rec: RecordSRN,
            ResourceType.schema: SchemaSRN,
            ResourceType.guarantee: GuaranteeSRN,
            ResourceType.dep: DepositionSRN,
            ResourceType.val: ValidationSRN,
            ResourceType.snap: SnapshotSRN,
            ResourceType.evt: EventSRN,
            ResourceType.profile: DepositionProfileSRN,
        }

        sub_cls = srn_types.get(t, SRN)
        return sub_cls(
            node=NodeId(node),
            type=ResourceType(typ),
            local=LocalId(local_str),
            version=version,
        )


# ---------- Per-type SRNs (tighten version constraints) ----------


class RecordSRN(SRN[Optional[RecordVersion]]):
    type: ResourceType = Field(default=ResourceType.rec, frozen=True)
    version: Optional[RecordVersion] = Field(default=None)

    @model_validator(mode="after")
    def _ensure_version(self) -> "RecordSRN":
        # optional: require explicit version
        if self.version is None:
            raise ValueError("Record SRN must include @<int> version (e.g., @1)")
        return self

    def render(self) -> str:
        if self.version is not None:
            # The base render() already handles version.
            pass
        return super().render()


class SchemaSRN(SRN[Semver]):
    type: ResourceType = Field(default=ResourceType.schema, frozen=True)
    version: Semver = Field()  # type: ignore


class GuaranteeSRN(SRN[Semver]):
    type: ResourceType = Field(default=ResourceType.guarantee, frozen=True)
    version: Semver = Field()  # type: ignore # required and must be semver


class DepositionProfileSRN(SRN[Semver]):
    type: ResourceType = Field(default=ResourceType.profile, frozen=True)
    version: Semver = Field()  # type: ignore # required and must be semver


class DepositionSRN(SRN[Optional[Version]]):
    type: ResourceType = Field(default=ResourceType.dep, frozen=True)
    version: Optional[Version] = None  # typically unversioned


class ValidationSRN(SRN[Optional[Version]]):
    type: ResourceType = Field(default=ResourceType.val, frozen=True)
    version: Optional[Version] = None  # job ids are separate; SRN usually unversioned


class SnapshotSRN(SRN[Optional[Version]]):
    type: ResourceType = Field(default=ResourceType.snap, frozen=True)
    version: Optional[Version] = None  # snapshot has its own id in 'local'


class EventSRN(SRN[Optional[Version]]):
    type: ResourceType = Field(default=ResourceType.evt, frozen=True)
    version: Optional[Version] = None


# ---------- Helpers ----------


def _parse_version(ver_str: str) -> Version:
    # try int (RecordVersion), else Semver
    if re.fullmatch(r"[1-9]\d*", ver_str):
        return RecordVersion(int(ver_str))
    return Semver(ver_str)


# ---------- Examples ----------

if __name__ == "__main__":
    s1 = "urn:osa:nuuid_01j6z6y6m3z7q9x6e5yb1s6v:rec:01jb7r3z1emch1t290zq3gzj9v@3"
    s2 = "urn:osa:dns_cam.ac.uk:schema:binding-measurement@1.0.0"
    s3 = "urn:osa:nuuid_01j6...:dep:01j4zq3w7e4w6k8d9h3v2v3b7x"

    rec = SRN.parse(s1)  # -> RecordSRN
    sch = SRN.parse(s2)  # -> SchemaSRN
    dep = SRN.parse(s3)  # -> DepositionSRN

    print(type(rec), str(rec))
    print(type(sch), sch.render())
    print(type(dep), f"{dep}")
