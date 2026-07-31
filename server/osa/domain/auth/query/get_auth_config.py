"""GetAuthConfig — the node's sign-in configuration (provider + admins).

All values are configuration, not DB state: the ORCID ``client_id`` and the
bootstrap admin ORCID list both come from ``Config``. ADMIN-gated because the
admin list is semi-sensitive; the ``client_secret`` is never returned.
"""

from __future__ import annotations

from osa.config import Config
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.query import Query, QueryHandler, Result


class GetAuthConfig(Query):
    pass


class AuthConfigResult(Result):
    provider: str
    client_id: str
    admin_orcids: list[str]


class GetAuthConfigHandler(QueryHandler[GetAuthConfig, AuthConfigResult]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    config: Config

    async def run(self, cmd: GetAuthConfig) -> AuthConfigResult:
        return AuthConfigResult(
            provider="orcid",
            client_id=self.config.auth.providers.orcid.client_id,
            admin_orcids=list(self.config.auth.admins.orcid),
        )
