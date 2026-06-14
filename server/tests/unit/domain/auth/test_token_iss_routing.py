"""Unit tests for TokenService `iss`-routed validation (#145, US5, T051).

Primary HS256 path is byte-identical when no second issuer is configured
(SC-007); a configured second issuer verifies its own RS256 tokens; failures on
one path never leak into the other.
"""

from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from osa.config import ExtraIssuerConfig, JwtConfig
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService

ISSUER = "https://deploy.example.org"
AUDIENCE = "osa-deploy"


@pytest.fixture(scope="module")
def keypair() -> tuple[bytes, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public_pem = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


def _jwt_config() -> JwtConfig:
    return JwtConfig(secret="a" * 40, algorithm="HS256")


def _m2m_token(private_pem: bytes, *, scope: str = "hooks:write", iss: str = ISSUER) -> str:
    return jwt.encode(
        {
            "iss": iss,
            "aud": AUDIENCE,
            "sub": "deploy-bot",
            "scope": scope,
            "exp": int(time.time()) + 3600,
        },
        private_pem,
        algorithm="RS256",
    )


class TestPrimaryPathUnchanged:
    def test_user_token_validates_without_extra_issuer(self) -> None:
        service = TokenService(_config=_jwt_config())
        token = service.create_access_token(
            UserId.generate(), ProviderIdentity(provider="local", external_id="alice")
        )
        payload = service.validate_access_token(token)
        assert payload["provider"] == "local"
        assert "iss" not in payload  # primary tokens carry no issuer claim

    def test_user_token_validates_with_extra_issuer_configured(self, keypair) -> None:
        _, public_pem = keypair
        extra = ExtraIssuerConfig(
            issuer=ISSUER, public_key=public_pem, algorithm="RS256", audience=AUDIENCE
        )
        service = TokenService(_config=_jwt_config(), _extra_issuer=extra)
        token = service.create_access_token(
            UserId.generate(), ProviderIdentity(provider="local", external_id="alice")
        )
        # Primary token still verifies via HS256 even with the second issuer set.
        assert service.validate_access_token(token)["provider"] == "local"


class TestExtraIssuerPath:
    def test_m2m_token_validates_and_carries_scope(self, keypair) -> None:
        private_pem, public_pem = keypair
        extra = ExtraIssuerConfig(
            issuer=ISSUER, public_key=public_pem, algorithm="RS256", audience=AUDIENCE
        )
        service = TokenService(_config=_jwt_config(), _extra_issuer=extra)
        payload = service.validate_access_token(_m2m_token(private_pem))
        assert payload["iss"] == ISSUER
        assert payload["scope"] == "hooks:write"

    def test_m2m_token_rejected_when_no_extra_issuer(self, keypair) -> None:
        private_pem, _ = keypair
        service = TokenService(_config=_jwt_config())  # no second issuer
        # Falls through to HS256 verification, which cannot verify an RS256 token.
        with pytest.raises(jwt.InvalidTokenError):
            service.validate_access_token(_m2m_token(private_pem))

    def test_wrong_key_is_rejected(self, keypair) -> None:
        _, public_pem = keypair
        extra = ExtraIssuerConfig(
            issuer=ISSUER, public_key=public_pem, algorithm="RS256", audience=AUDIENCE
        )
        service = TokenService(_config=_jwt_config(), _extra_issuer=extra)
        # Token claims the trusted issuer but is signed by a different key.
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        other_pem = other.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        with pytest.raises(jwt.InvalidTokenError):
            service.validate_access_token(_m2m_token(other_pem))

    def test_wrong_audience_is_rejected(self, keypair) -> None:
        private_pem, public_pem = keypair
        extra = ExtraIssuerConfig(
            issuer=ISSUER, public_key=public_pem, algorithm="RS256", audience="other-aud"
        )
        service = TokenService(_config=_jwt_config(), _extra_issuer=extra)
        with pytest.raises(jwt.InvalidTokenError):
            service.validate_access_token(_m2m_token(private_pem))
