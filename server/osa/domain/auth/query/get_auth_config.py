"""GetAuthConfig — the node's sign-in configuration (provider + admins).

All values are configuration, not DB state: the ORCID ``client_id`` and the
bootstrap admin ORCID list both come from ``Config``; the ``client_secret`` is
never returned. Scoped ``auth:read`` (or ADMIN) rather than ADMIN-only (#184):
these are non-secret config, so a hosted control plane's read-proxy can surface
them to the dashboard with a per-node scoped token — same model as the other
read surfaces.
"""

from __future__ import annotations

from osa.config import Config
from osa.domain.auth.model.principal import Principal
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.query import Query, QueryHandler, Result


class GetAuthConfig(Query):
    pass


class AuthConfigResult(Result):
    provider: str
    client_id: str
    admin_orcids: list[str]


class GetAuthConfigHandler(QueryHandler[GetAuthConfig, AuthConfigResult]):
    # Scoped M2M read (#184): `auth:read` OR ADMIN. The result is non-secret
    # sign-in config (provider, client_id, admin ORCIDs) — the client_secret is
    # never returned — so a scoped read token is appropriate.
    __auth__ = requires_scope("auth:read")
    principal: Principal
    config: Config

    async def run(self, cmd: GetAuthConfig) -> AuthConfigResult:
        return AuthConfigResult(
            provider="orcid",
            client_id=self.config.auth.providers.orcid.client_id,
            admin_orcids=list(self.config.auth.admins.orcid),
        )
