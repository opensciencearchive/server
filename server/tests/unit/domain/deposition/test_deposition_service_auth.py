"""Tests for DepositionService authorization — T029.

Tests that:
- Owner can read/update/submit their own deposition via Guarded[Deposition]
- Non-owner depositor is denied access to another's deposition
- owner_id is set from principal at creation time
"""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.shared.authorization.action import Action
from osa.domain.shared.authorization.guarded import Guarded
from osa.domain.shared.authorization.policy_set import POLICY_SET
from osa.domain.shared.error import AuthorizationError
from osa.domain.shared.model.srn import DepositionSRN


def _make_principal(
    user_id: UserId | None = None, roles: frozenset[Role] | None = None
) -> Principal:
    return Principal(
        user_id=user_id or UserId.generate(),
        identity=ProviderIdentity(provider="test", external_id="test-ext"),
        roles=roles or frozenset({Role.DEPOSITOR}),
    )


def _make_deposition(owner_id: UserId) -> Deposition:
    return Deposition(
        srn=DepositionSRN.parse("urn:osa:localhost:dep:00000000-0000-0000-0000-000000000001"),
        status=DepositionStatus.DRAFT,
        metadata={},
        owner_id=owner_id,
    )


class TestDepositionOwnership:
    """Guarded[Deposition] enforces ownership rules via POLICY_SET."""

    def test_owner_can_read_own_deposition(self) -> None:
        owner = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, owner, POLICY_SET)

        result = guarded.check(Action.DEPOSITION_READ)
        assert result is dep

    def test_owner_can_update_own_deposition(self) -> None:
        owner = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, owner, POLICY_SET)

        result = guarded.check(Action.DEPOSITION_UPDATE)
        assert result is dep

    def test_owner_can_submit_own_deposition(self) -> None:
        owner = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, owner, POLICY_SET)

        result = guarded.check(Action.DEPOSITION_SUBMIT)
        assert result is dep

    def test_non_owner_depositor_cannot_read_others_deposition(self) -> None:
        owner = _make_principal()
        other = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, other, POLICY_SET)

        with pytest.raises(AuthorizationError):
            guarded.check(Action.DEPOSITION_READ)

    def test_non_owner_depositor_cannot_update_others_deposition(self) -> None:
        owner = _make_principal()
        other = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, other, POLICY_SET)

        with pytest.raises(AuthorizationError):
            guarded.check(Action.DEPOSITION_UPDATE)

    def test_non_owner_depositor_cannot_submit_others_deposition(self) -> None:
        owner = _make_principal()
        other = _make_principal()
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, other, POLICY_SET)

        with pytest.raises(AuthorizationError):
            guarded.check(Action.DEPOSITION_SUBMIT)

    def test_curator_can_read_any_deposition(self) -> None:
        """Curators can read all depositions without ownership."""
        owner = _make_principal()
        curator = _make_principal(roles=frozenset({Role.CURATOR}))
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, curator, POLICY_SET)

        result = guarded.check(Action.DEPOSITION_READ)
        assert result is dep

    def test_admin_can_read_any_deposition(self) -> None:
        """Admins inherit curator permissions via role hierarchy."""
        owner = _make_principal()
        admin = _make_principal(roles=frozenset({Role.ADMIN}))
        dep = _make_deposition(owner_id=owner.user_id)
        guarded = Guarded(dep, admin, POLICY_SET)

        result = guarded.check(Action.DEPOSITION_READ)
        assert result is dep


class TestDepositionOwnerIdAssignment:
    """Deposition aggregate tracks owner_id."""

    def test_deposition_has_owner_id_field(self) -> None:
        owner_id = UserId.generate()
        dep = _make_deposition(owner_id=owner_id)
        assert dep.owner_id == owner_id

    def test_deposition_owner_id_defaults_to_none(self) -> None:
        dep = Deposition(
            srn=DepositionSRN.parse("urn:osa:localhost:dep:00000000-0000-0000-0000-000000000001"),
            status=DepositionStatus.DRAFT,
            metadata={},
        )
        assert dep.owner_id is None
