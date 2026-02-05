"""Unit tests for OAuth state token signing/verification."""

import time


from osa.application.api.v1.routes.auth import (
    _STATE_EXPIRY_SECONDS,
    _create_signed_state,
    _verify_signed_state,
)


class TestSignedStateCreation:
    """Tests for _create_signed_state."""

    def test_creates_state_with_redirect_uri(self):
        """Should create a signed state containing the redirect URI."""
        secret = "test-secret-key-for-signing"
        redirect_uri = "https://example.com/callback"

        state = _create_signed_state(secret, redirect_uri)

        # State should be format: payload.signature
        assert "." in state
        parts = state.split(".")
        assert len(parts) == 2

    def test_different_nonces_produce_different_states(self):
        """Each state should have a unique nonce."""
        secret = "test-secret-key"
        redirect_uri = "https://example.com"

        state1 = _create_signed_state(secret, redirect_uri)
        state2 = _create_signed_state(secret, redirect_uri)

        assert state1 != state2

    def test_state_is_url_safe(self):
        """State should only contain URL-safe characters."""
        secret = "test-secret"
        redirect_uri = "https://example.com/path?query=value"

        state = _create_signed_state(secret, redirect_uri)

        # URL-safe base64 uses only these characters
        allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.")
        assert all(c in allowed for c in state)


class TestSignedStateVerification:
    """Tests for _verify_signed_state."""

    def test_verifies_valid_state(self):
        """Should return redirect_uri for valid state."""
        secret = "test-secret-key"
        redirect_uri = "https://example.com/after-login"

        state = _create_signed_state(secret, redirect_uri)
        result = _verify_signed_state(secret, state)

        assert result == redirect_uri

    def test_rejects_tampered_payload(self):
        """Should reject state with tampered payload."""
        secret = "test-secret-key"
        state = _create_signed_state(secret, "https://example.com")

        # Tamper with the payload (change a character)
        parts = state.split(".")
        tampered_payload = "x" + parts[0][1:]
        tampered_state = f"{tampered_payload}.{parts[1]}"

        result = _verify_signed_state(secret, tampered_state)
        assert result is None

    def test_rejects_tampered_signature(self):
        """Should reject state with tampered signature."""
        secret = "test-secret-key"
        state = _create_signed_state(secret, "https://example.com")

        # Tamper with the signature
        parts = state.split(".")
        tampered_sig = "x" + parts[1][1:]
        tampered_state = f"{parts[0]}.{tampered_sig}"

        result = _verify_signed_state(secret, tampered_state)
        assert result is None

    def test_rejects_wrong_secret(self):
        """Should reject state signed with different secret."""
        state = _create_signed_state("secret-one", "https://example.com")

        result = _verify_signed_state("secret-two", state)
        assert result is None

    def test_rejects_expired_state(self, monkeypatch):
        """Should reject expired state."""
        secret = "test-secret-key"
        state = _create_signed_state(secret, "https://example.com")

        # Fast-forward time past expiry
        future_time = time.time() + _STATE_EXPIRY_SECONDS + 1
        monkeypatch.setattr(time, "time", lambda: future_time)

        result = _verify_signed_state(secret, state)
        assert result is None

    def test_rejects_malformed_state(self):
        """Should reject malformed state strings."""
        secret = "test-secret"

        # No dot separator
        assert _verify_signed_state(secret, "nodot") is None

        # Empty parts
        assert _verify_signed_state(secret, ".") is None
        assert _verify_signed_state(secret, "payload.") is None
        assert _verify_signed_state(secret, ".signature") is None

        # Too many parts
        assert _verify_signed_state(secret, "a.b.c") is None

        # Invalid base64
        assert _verify_signed_state(secret, "!!!.???") is None

    def test_rejects_empty_state(self):
        """Should reject empty state."""
        assert _verify_signed_state("secret", "") is None

    def test_handles_special_characters_in_redirect_uri(self):
        """Should handle redirect URIs with special characters."""
        secret = "test-secret"
        redirect_uri = "https://example.com/path?foo=bar&baz=qux#fragment"

        state = _create_signed_state(secret, redirect_uri)
        result = _verify_signed_state(secret, state)

        assert result == redirect_uri
