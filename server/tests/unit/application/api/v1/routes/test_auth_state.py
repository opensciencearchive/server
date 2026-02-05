"""Unit tests for OAuth state token signing/verification."""

import time

import pytest

from osa.config import JwtConfig
from osa.domain.auth.service.token import STATE_EXPIRY_SECONDS, TokenService


@pytest.fixture
def token_service() -> TokenService:
    """Create a TokenService with test config."""
    config = JwtConfig(
        secret="test-secret-key-for-signing-min-32",
        algorithm="HS256",
        access_token_expire_minutes=15,
        refresh_token_expire_days=7,
    )
    return TokenService(_config=config)


@pytest.fixture
def token_service_alt_secret() -> TokenService:
    """Create a TokenService with a different secret."""
    config = JwtConfig(
        secret="different-secret-key-for-testing",
        algorithm="HS256",
        access_token_expire_minutes=15,
        refresh_token_expire_days=7,
    )
    return TokenService(_config=config)


class TestSignedStateCreation:
    """Tests for TokenService.create_oauth_state."""

    def test_creates_state_with_redirect_uri(self, token_service: TokenService):
        """Should create a signed state containing the redirect URI."""
        redirect_uri = "https://example.com/callback"

        state = token_service.create_oauth_state(redirect_uri)

        # State should be format: payload.signature
        assert "." in state
        parts = state.split(".")
        assert len(parts) == 2

    def test_different_nonces_produce_different_states(self, token_service: TokenService):
        """Each state should have a unique nonce."""
        redirect_uri = "https://example.com"

        state1 = token_service.create_oauth_state(redirect_uri)
        state2 = token_service.create_oauth_state(redirect_uri)

        assert state1 != state2

    def test_state_is_url_safe(self, token_service: TokenService):
        """State should only contain URL-safe characters."""
        redirect_uri = "https://example.com/path?query=value"

        state = token_service.create_oauth_state(redirect_uri)

        # URL-safe base64 uses only these characters
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.")
        assert all(c in allowed for c in state)


class TestSignedStateVerification:
    """Tests for TokenService.verify_oauth_state."""

    def test_verifies_valid_state(self, token_service: TokenService):
        """Should return redirect_uri for valid state."""
        redirect_uri = "https://example.com/after-login"

        state = token_service.create_oauth_state(redirect_uri)
        result = token_service.verify_oauth_state(state)

        assert result == redirect_uri

    def test_rejects_tampered_payload(self, token_service: TokenService):
        """Should reject state with tampered payload."""
        state = token_service.create_oauth_state("https://example.com")

        # Tamper with the payload (change a character)
        parts = state.split(".")
        tampered_payload = "x" + parts[0][1:]
        tampered_state = f"{tampered_payload}.{parts[1]}"

        result = token_service.verify_oauth_state(tampered_state)
        assert result is None

    def test_rejects_tampered_signature(self, token_service: TokenService):
        """Should reject state with tampered signature."""
        state = token_service.create_oauth_state("https://example.com")

        # Tamper with the signature
        parts = state.split(".")
        tampered_sig = "x" + parts[1][1:]
        tampered_state = f"{parts[0]}.{tampered_sig}"

        result = token_service.verify_oauth_state(tampered_state)
        assert result is None

    def test_rejects_wrong_secret(
        self, token_service: TokenService, token_service_alt_secret: TokenService
    ):
        """Should reject state signed with different secret."""
        state = token_service.create_oauth_state("https://example.com")

        result = token_service_alt_secret.verify_oauth_state(state)
        assert result is None

    def test_rejects_expired_state(self, token_service: TokenService, monkeypatch):
        """Should reject expired state."""
        state = token_service.create_oauth_state("https://example.com")

        # Fast-forward time past expiry
        future_time = time.time() + STATE_EXPIRY_SECONDS + 1
        monkeypatch.setattr(time, "time", lambda: future_time)

        result = token_service.verify_oauth_state(state)
        assert result is None

    def test_rejects_malformed_state(self, token_service: TokenService):
        """Should reject malformed state strings."""
        # No dot separator
        assert token_service.verify_oauth_state("nodot") is None

        # Empty parts
        assert token_service.verify_oauth_state(".") is None
        assert token_service.verify_oauth_state("payload.") is None
        assert token_service.verify_oauth_state(".signature") is None

        # Too many parts
        assert token_service.verify_oauth_state("a.b.c") is None

        # Invalid base64
        assert token_service.verify_oauth_state("!!!.???") is None

    def test_rejects_empty_state(self, token_service: TokenService):
        """Should reject empty state."""
        assert token_service.verify_oauth_state("") is None

    def test_handles_special_characters_in_redirect_uri(self, token_service: TokenService):
        """Should handle redirect URIs with special characters."""
        redirect_uri = "https://example.com/path?foo=bar&baz=qux#fragment"

        state = token_service.create_oauth_state(redirect_uri)
        result = token_service.verify_oauth_state(state)

        assert result == redirect_uri
