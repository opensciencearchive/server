"""Unit tests for the dev-JWT-secret boot safety check.

The well-known dev secret is public — it's safe to ship only when both:
  1. The operator explicitly opted into dev mode (OSA_DEV_MODE=true).
  2. The server is bound to loopback so the secret isn't network-reachable.

These tests pin the rule down so a future refactor can't quietly weaken it.
"""

import os
from unittest.mock import patch

import pytest

from osa.application.api.rest.app import _check_dev_secret_safety
from osa.config import DEV_JWT_SECRET, AuthConfig, Config, JwtConfig


def _config(*, secret: str, dev_mode: bool) -> Config:
    return Config(
        domain="localhost",
        base_url="http://localhost:8000",
        dev_mode=dev_mode,
        auth=AuthConfig(jwt=JwtConfig(secret=secret)),
    )


class TestRefusesUnsafeUseOfDevSecret:
    def test_refuses_dev_secret_without_dev_mode(self):
        config = _config(secret=DEV_JWT_SECRET, dev_mode=False)
        with pytest.raises(RuntimeError, match="OSA_DEV_MODE is not set"):
            _check_dev_secret_safety(config)

    def test_refuses_dev_secret_on_public_bind(self):
        config = _config(secret=DEV_JWT_SECRET, dev_mode=True)
        with patch.dict(os.environ, {"UVICORN_HOST": "0.0.0.0"}, clear=False):
            with pytest.raises(RuntimeError, match="not loopback"):
                _check_dev_secret_safety(config)


class TestAllowsSafeConfigurations:
    def test_allows_dev_secret_with_dev_mode_on_loopback(self):
        config = _config(secret=DEV_JWT_SECRET, dev_mode=True)
        env_no_bind = {k: v for k, v in os.environ.items() if k != "UVICORN_HOST"}
        with patch.dict(os.environ, env_no_bind, clear=True):
            _check_dev_secret_safety(config)  # no raise

    def test_allows_dev_secret_with_explicit_loopback_bind(self):
        config = _config(secret=DEV_JWT_SECRET, dev_mode=True)
        for host in ("127.0.0.1", "localhost", "::1"):
            with patch.dict(os.environ, {"UVICORN_HOST": host}, clear=False):
                _check_dev_secret_safety(config)  # no raise

    def test_allows_real_secret_regardless_of_dev_mode_or_bind(self):
        real_secret = "x" * 64
        for dev_mode in (True, False):
            config = _config(secret=real_secret, dev_mode=dev_mode)
            with patch.dict(os.environ, {"UVICORN_HOST": "0.0.0.0"}, clear=False):
                _check_dev_secret_safety(config)  # no raise


class TestDefaults:
    def test_admins_local_defaults_to_admin_at_osa_local(self):
        """The default admins.local list must match what the CLI signs into its dev JWT."""
        config = AuthConfig()
        assert config.admins.local == ["admin@osa.local"]

    def test_jwt_secret_defaults_to_dev_value(self):
        """JwtConfig defaults to the loud dev secret — relied on by `osa start`."""
        config = JwtConfig()
        assert config.secret == DEV_JWT_SECRET
