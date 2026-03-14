"""Unit tests for login command polling loop (T039)."""

from unittest.mock import MagicMock, patch

import httpx

from osa.cli.login import _poll_for_token


def _make_response(status_code: int, json_data: dict) -> httpx.Response:
    """Create a mock httpx.Response."""
    return httpx.Response(
        status_code=status_code,
        json=json_data,
        request=httpx.Request("POST", "https://example.com/api/v1/auth/device/token"),
    )


class TestPollForToken:
    """Tests for the _poll_for_token polling loop."""

    def test_returns_tokens_on_success(self):
        """Poll should return tokens when server responds with 200."""
        client = MagicMock()
        client.post.return_value = _make_response(
            200,
            {
                "access_token": "at-123",
                "refresh_token": "rt-456",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )

        result = _poll_for_token(
            client=client,
            server="https://example.com",
            device_code="dc-abc",
            interval=1,
            expires_in=300,
        )

        assert result is not None
        assert result["access_token"] == "at-123"
        assert result["refresh_token"] == "rt-456"

    def test_polls_until_authorized(self):
        """Poll should keep polling on authorization_pending, then return on success."""
        pending_resp = _make_response(
            400,
            {"error": "authorization_pending", "error_description": "Not yet"},
        )
        success_resp = _make_response(
            200,
            {
                "access_token": "at",
                "refresh_token": "rt",
                "token_type": "Bearer",
                "expires_in": 3600,
            },
        )

        client = MagicMock()
        client.post.side_effect = [pending_resp, pending_resp, success_resp]

        with patch("osa.cli.login.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 1, 2, 3, 4, 5, 6]
            mock_time.sleep = MagicMock()

            result = _poll_for_token(
                client=client,
                server="https://example.com",
                device_code="dc",
                interval=1,
                expires_in=300,
            )

        assert result is not None
        assert client.post.call_count == 3

    def test_returns_none_on_expired(self):
        """Poll should return None when server responds with expired_token."""
        client = MagicMock()
        client.post.return_value = _make_response(
            400,
            {"error": "expired_token", "error_description": "Code expired"},
        )

        result = _poll_for_token(
            client=client,
            server="https://example.com",
            device_code="dc",
            interval=1,
            expires_in=300,
        )

        assert result is None

    def test_retries_on_network_error(self):
        """Poll should retry on transient network errors."""
        client = MagicMock()
        client.post.side_effect = [
            httpx.ConnectError("Connection refused"),
            _make_response(
                200,
                {
                    "access_token": "at",
                    "refresh_token": "rt",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            ),
        ]

        with patch("osa.cli.login.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 1, 2, 3, 4]
            mock_time.sleep = MagicMock()

            result = _poll_for_token(
                client=client,
                server="https://example.com",
                device_code="dc",
                interval=1,
                expires_in=300,
            )

        assert result is not None
        assert client.post.call_count == 2

    def test_retries_on_server_error(self):
        """Poll should retry on HTTP 5xx errors."""
        client = MagicMock()
        client.post.side_effect = [
            _make_response(500, {"error": "internal"}),
            _make_response(
                200,
                {
                    "access_token": "at",
                    "refresh_token": "rt",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            ),
        ]

        with patch("osa.cli.login.time") as mock_time:
            mock_time.monotonic.side_effect = [0, 1, 2, 3, 4]
            mock_time.sleep = MagicMock()

            result = _poll_for_token(
                client=client,
                server="https://example.com",
                device_code="dc",
                interval=1,
                expires_in=300,
            )

        assert result is not None

    def test_returns_none_on_timeout(self):
        """Poll should return None when total time exceeds expires_in."""
        client = MagicMock()
        client.post.return_value = _make_response(
            400,
            {"error": "authorization_pending"},
        )

        with patch("osa.cli.login.time") as mock_time:
            # Simulate time passing beyond expires_in
            mock_time.monotonic.side_effect = [0, 301]
            mock_time.sleep = MagicMock()

            result = _poll_for_token(
                client=client,
                server="https://example.com",
                device_code="dc",
                interval=5,
                expires_in=300,
            )

        assert result is None
