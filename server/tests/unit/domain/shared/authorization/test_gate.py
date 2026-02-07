"""Tests for gate module: Gate base class, Public, AtLeast, factory functions."""

from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import AtLeast, Gate, Public, at_least, public


class TestGateHierarchy:
    def test_public_is_gate(self) -> None:
        assert isinstance(Public(), Gate)

    def test_at_least_is_gate(self) -> None:
        assert isinstance(AtLeast(role=Role.ADMIN), Gate)


class TestPublic:
    def test_public_returns_public_instance(self) -> None:
        assert isinstance(public(), Public)

    def test_public_always_returns_same_object(self) -> None:
        assert public() is public()


class TestAtLeast:
    def test_at_least_creates_dataclass(self) -> None:
        gate = at_least(Role.ADMIN)
        assert isinstance(gate, AtLeast)
        assert gate.role is Role.ADMIN

    def test_at_least_different_roles(self) -> None:
        depositor_gate = at_least(Role.DEPOSITOR)
        admin_gate = at_least(Role.ADMIN)
        assert depositor_gate.role is Role.DEPOSITOR
        assert admin_gate.role is Role.ADMIN

    def test_at_least_is_frozen(self) -> None:
        gate = at_least(Role.ADMIN)
        assert hash(gate) is not None  # frozen dataclass is hashable

    def test_at_least_equality(self) -> None:
        gate1 = at_least(Role.ADMIN)
        gate2 = at_least(Role.ADMIN)
        assert gate1 == gate2

    def test_at_least_inequality(self) -> None:
        gate1 = at_least(Role.ADMIN)
        gate2 = at_least(Role.DEPOSITOR)
        assert gate1 != gate2
