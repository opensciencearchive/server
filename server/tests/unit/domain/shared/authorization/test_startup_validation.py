"""Tests for startup validation of handler __auth__ declarations.

Tests that all handlers must declare __auth__ as public() or at_least(Role).
"""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least, public
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import ConfigurationError
from osa.domain.shared.query import Query, QueryHandler
from osa.domain.shared.query import Result as QueryResult


class TestStartupValidation:
    def test_validation_catches_missing_auth_on_command_handler(self) -> None:
        """A CommandHandler without __auth__ should fail startup."""

        class UnprotectedCommand(Command):
            pass

        class UnprotectedResult(Result):
            pass

        class UnprotectedHandler(CommandHandler[UnprotectedCommand, UnprotectedResult]):
            async def run(self, cmd: UnprotectedCommand) -> UnprotectedResult:
                return UnprotectedResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        with pytest.raises(ConfigurationError, match="UnprotectedHandler"):
            _check_handler_class(UnprotectedHandler)

    def test_validation_passes_for_protected_handler(self) -> None:
        """A handler with __auth__ = at_least(...) should pass validation."""

        class ProtectedCommand(Command):
            pass

        class ProtectedResult(Result):
            pass

        class ProtectedHandler(CommandHandler[ProtectedCommand, ProtectedResult]):
            __auth__ = at_least(Role.ADMIN)
            principal: Principal

            async def run(self, cmd: ProtectedCommand) -> ProtectedResult:
                return ProtectedResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        # Should not raise
        _check_handler_class(ProtectedHandler)

    def test_validation_passes_for_public_handler(self) -> None:
        """A handler with __auth__ = public() should pass validation."""

        class PublicCommand(Command):
            pass

        class PublicResult(Result):
            pass

        class PublicHandler(CommandHandler[PublicCommand, PublicResult]):
            __auth__ = public()

            async def run(self, cmd: PublicCommand) -> PublicResult:
                return PublicResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        # Should not raise
        _check_handler_class(PublicHandler)

    def test_validation_catches_missing_auth_on_query_handler(self) -> None:
        """A QueryHandler without __auth__ should fail."""

        class UnprotectedQuery(Query):
            pass

        class UnprotectedQueryResult(QueryResult):
            pass

        class UnprotectedQueryHandler(QueryHandler[UnprotectedQuery, UnprotectedQueryResult]):
            async def run(self, cmd: UnprotectedQuery) -> UnprotectedQueryResult:
                return UnprotectedQueryResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        with pytest.raises(ConfigurationError, match="UnprotectedQueryHandler"):
            _check_handler_class(UnprotectedQueryHandler)

    def test_validation_catches_invalid_auth_type(self) -> None:
        """A handler with a non-Gate __auth__ should fail validation."""

        class BadCommand(Command):
            pass

        class BadResult(Result):
            pass

        class BadHandler(CommandHandler[BadCommand, BadResult]):
            __auth__ = "not_a_valid_gate"

            async def run(self, cmd: BadCommand) -> BadResult:
                return BadResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        with pytest.raises(ConfigurationError, match="no __auth__ declaration"):
            _check_handler_class(BadHandler)
