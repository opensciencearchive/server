"""Unit tests for TokenService JWT creation and validation."""

import time
from uuid import uuid4

import jwt
import pytest

from osa.config import JwtConfig
from osa.domain.auth.model.value import UserId
from osa.domain.auth.service.token import TokenService


class TestTokenServiceAccessToken:
    """Tests for JWT access token creation and validation."""

    def make_service(self, secret: str = "test-secret-key-256-bits-long-xx") -> TokenService:
        """Create a TokenService with test config."""
        config = JwtConfig(
            secret=secret,
            algorithm="HS256",
            access_token_expire_minutes=60,
            refresh_token_expire_days=7,
        )
        return TokenService(_config=config)

    def test_create_access_token_returns_valid_jwt(self):
        """create_access_token should return a decodable JWT."""
        service = self.make_service()
        user_id = UserId(uuid4())
        orcid_id = "0000-0001-2345-6789"

        token = service.create_access_token(user_id, orcid_id)

        # Should be decodable
        payload = jwt.decode(
            token,
            "test-secret-key-256-bits-long-xx",
            algorithms=["HS256"],
            audience="authenticated",
        )
        assert payload["sub"] == str(user_id)
        assert payload["orcid_id"] == orcid_id
        assert payload["aud"] == "authenticated"

    def test_create_access_token_includes_expiry(self):
        """create_access_token should set exp claim."""
        service = self.make_service()
        user_id = UserId(uuid4())

        token = service.create_access_token(user_id, "0000-0001-2345-6789")

        payload = jwt.decode(
            token,
            "test-secret-key-256-bits-long-xx",
            algorithms=["HS256"],
            audience="authenticated",
        )
        assert "exp" in payload
        assert "iat" in payload
        # Expiry should be ~60 minutes from now
        assert payload["exp"] > payload["iat"]
        assert payload["exp"] - payload["iat"] == 60 * 60  # 60 minutes in seconds

    def test_create_access_token_includes_jti(self):
        """create_access_token should include unique jti claim."""
        service = self.make_service()
        user_id = UserId(uuid4())

        token1 = service.create_access_token(user_id, "0000-0001-2345-6789")
        token2 = service.create_access_token(user_id, "0000-0001-2345-6789")

        payload1 = jwt.decode(
            token1,
            "test-secret-key-256-bits-long-xx",
            algorithms=["HS256"],
            audience="authenticated",
        )
        payload2 = jwt.decode(
            token2,
            "test-secret-key-256-bits-long-xx",
            algorithms=["HS256"],
            audience="authenticated",
        )

        assert "jti" in payload1
        assert "jti" in payload2
        assert payload1["jti"] != payload2["jti"]

    def test_create_access_token_with_additional_claims(self):
        """create_access_token should include additional claims if provided."""
        service = self.make_service()
        user_id = UserId(uuid4())

        token = service.create_access_token(
            user_id,
            "0000-0001-2345-6789",
            additional_claims={"custom": "value"},
        )

        payload = jwt.decode(
            token,
            "test-secret-key-256-bits-long-xx",
            algorithms=["HS256"],
            audience="authenticated",
        )
        assert payload["custom"] == "value"

    def test_validate_access_token_returns_payload(self):
        """validate_access_token should return decoded payload."""
        service = self.make_service()
        user_id = UserId(uuid4())
        orcid_id = "0000-0001-2345-6789"

        token = service.create_access_token(user_id, orcid_id)
        payload = service.validate_access_token(token)

        assert payload["sub"] == str(user_id)
        assert payload["orcid_id"] == orcid_id

    def test_validate_access_token_rejects_invalid_token(self):
        """validate_access_token should raise for invalid tokens."""
        service = self.make_service()

        with pytest.raises(jwt.InvalidTokenError):
            service.validate_access_token("invalid-token")

    def test_validate_access_token_rejects_wrong_secret(self):
        """validate_access_token should reject tokens signed with wrong secret."""
        service1 = self.make_service(secret="secret-one-that-is-long-enough")
        service2 = self.make_service(secret="secret-two-that-is-long-enough")

        token = service1.create_access_token(UserId(uuid4()), "0000-0001-2345-6789")

        with pytest.raises(jwt.InvalidTokenError):
            service2.validate_access_token(token)

    def test_validate_access_token_rejects_expired_token(self):
        """validate_access_token should reject expired tokens."""
        config = JwtConfig(
            secret="test-secret-key-256-bits-long-xx",
            algorithm="HS256",
            access_token_expire_minutes=0,  # Immediate expiry
            refresh_token_expire_days=7,
        )
        service = TokenService(_config=config)

        # Create token that's already expired
        token = service.create_access_token(UserId(uuid4()), "0000-0001-2345-6789")

        # Small delay to ensure expiry
        time.sleep(0.1)

        with pytest.raises(jwt.ExpiredSignatureError):
            service.validate_access_token(token)


class TestTokenServiceRefreshToken:
    """Tests for opaque refresh token creation."""

    def make_service(self) -> TokenService:
        """Create a TokenService with test config."""
        config = JwtConfig(
            secret="test-secret",
            algorithm="HS256",
            access_token_expire_minutes=60,
            refresh_token_expire_days=7,
        )
        return TokenService(_config=config)

    def test_create_refresh_token_returns_tuple(self):
        """create_refresh_token should return (raw_token, token_hash)."""
        service = self.make_service()

        raw_token, token_hash = service.create_refresh_token()

        assert isinstance(raw_token, str)
        assert isinstance(token_hash, str)
        assert len(raw_token) > 0
        assert len(token_hash) == 64  # SHA256 hex = 64 chars

    def test_create_refresh_token_unique_each_time(self):
        """create_refresh_token should generate unique tokens."""
        service = self.make_service()

        raw1, hash1 = service.create_refresh_token()
        raw2, hash2 = service.create_refresh_token()

        assert raw1 != raw2
        assert hash1 != hash2

    def test_hash_token_consistent(self):
        """hash_token should produce consistent hashes."""
        raw_token = "test-token-value"

        hash1 = TokenService.hash_token(raw_token)
        hash2 = TokenService.hash_token(raw_token)

        assert hash1 == hash2
        assert len(hash1) == 64

    def test_hash_token_different_for_different_tokens(self):
        """hash_token should produce different hashes for different tokens."""
        hash1 = TokenService.hash_token("token-one")
        hash2 = TokenService.hash_token("token-two")

        assert hash1 != hash2

    def test_created_refresh_token_hash_matches(self):
        """The hash from create_refresh_token should match hash_token(raw)."""
        service = self.make_service()

        raw_token, token_hash = service.create_refresh_token()

        assert TokenService.hash_token(raw_token) == token_hash


class TestTokenServiceProperties:
    """Tests for TokenService property accessors."""

    def test_access_token_expire_seconds(self):
        """access_token_expire_seconds should convert minutes to seconds."""
        config = JwtConfig(
            secret="test",
            algorithm="HS256",
            access_token_expire_minutes=30,
            refresh_token_expire_days=7,
        )
        service = TokenService(_config=config)

        assert service.access_token_expire_seconds == 30 * 60

    def test_refresh_token_expire_days(self):
        """refresh_token_expire_days should return configured value."""
        config = JwtConfig(
            secret="test",
            algorithm="HS256",
            access_token_expire_minutes=60,
            refresh_token_expire_days=14,
        )
        service = TokenService(_config=config)

        assert service.refresh_token_expire_days == 14
