"""Deposition-domain input for the bundled convention deploy (#145).

The bundled deploy's *edge* shape is the command DTO
:class:`~osa.domain.deposition.command.create_convention.DeployConventionHook`
(§4). This is its *internal* representation — the handler maps each edge DTO to
a :class:`HookDeploy` and hands the list to :class:`ConventionService.deploy`,
which upserts the identity and mints the release.

It lives in the deposition domain (not ``shared``) on purpose: design-revisions
§1 removed the *shared* deploy/payload value object. The single persisted hook
artifact remains :class:`~osa.domain.validation.model.hook_release.HookRelease`.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from osa.domain.shared.model.hook import HookIdentity, OciConfig
from osa.domain.shared.model.value import ValueObject


class HookDeploy(ValueObject):
    """One hook in a bundled deploy: its fixed identity + the release to mint."""

    identity: HookIdentity
    runtime: Annotated[OciConfig, Field(discriminator="type")]
    source_ref: str
