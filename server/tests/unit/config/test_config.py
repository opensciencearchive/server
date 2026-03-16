"""Unit tests for Config model restructure (US2 + US4)."""

import os
from unittest.mock import patch

import pytest
import yaml

from osa.config import Config


def make_config_yaml(data: dict) -> str:
    """Serialize config dict to YAML string."""
    return yaml.dump(data, default_flow_style=False)


def config_from_yaml(data: dict, env_overrides: dict[str, str] | None = None) -> Config:
    """Create a Config from a YAML dict, using a temp file and env vars.

    Requires at minimum: auth.jwt.secret set via env var.
    """
    import tempfile

    raw = make_config_yaml(data)
    env = {
        "OSA_AUTH__JWT__SECRET": "test-secret-key-that-is-at-least-32-chars-long",
        **(env_overrides or {}),
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(raw)
        f.flush()
        env["OSA_CONFIG_FILE"] = f.name

    with patch.dict(os.environ, env, clear=False):
        return Config()  # type: ignore[call-arg]


class TestConfigNewShape:
    """T001 — Config parses with top-level name, domain, auth.providers.orcid, auth.admins.orcid."""

    def test_top_level_name_and_domain(self):
        """Config accepts top-level name and domain fields."""
        cfg = config_from_yaml({"name": "My Archive", "domain": "archive.org"})
        assert cfg.name == "My Archive"
        assert cfg.domain == "archive.org"

    def test_defaults_for_name_and_domain(self):
        """Config has sensible defaults when name/domain not specified."""
        cfg = config_from_yaml({})
        assert cfg.name == "Open Science Archive"
        assert cfg.domain == "localhost"

    def test_config_without_operational_fields(self):
        """Config works without logging, database, worker fields (set via env vars)."""
        cfg = config_from_yaml({"name": "Test"})
        # Should parse successfully — operational fields have defaults
        assert cfg.name == "Test"
        assert cfg.database is not None
        assert cfg.logging is not None
        assert cfg.worker is not None

    def test_config_without_indexes_field(self):
        """Config works without indexes field (removed)."""
        cfg = config_from_yaml({"name": "Test"})
        # Should not have an indexes field at all
        assert not hasattr(cfg, "indexes")

    def test_server_field_not_accepted(self):
        """The old server field should no longer exist on Config."""
        assert not hasattr(Config, "model_fields") or "server" not in Config.model_fields


class TestAuthProvidersConfig:
    """T002 — auth.providers.orcid path provides OrcidConfig."""

    def test_providers_orcid_config(self):
        """auth.providers.orcid provides OrcidConfig with client_id, client_secret, sandbox."""
        cfg = config_from_yaml(
            {
                "auth": {
                    "providers": {
                        "orcid": {
                            "client_id": "APP-TEST123",
                            "client_secret": "secret-456",
                            "sandbox": False,
                        }
                    }
                }
            }
        )
        assert cfg.auth.providers.orcid.client_id == "APP-TEST123"
        assert cfg.auth.providers.orcid.client_secret == "secret-456"
        assert cfg.auth.providers.orcid.sandbox is False

    def test_admins_orcid_list(self):
        """auth.admins.orcid accepts list of valid ORCiD IDs."""
        cfg = config_from_yaml(
            {
                "auth": {
                    "admins": {
                        "orcid": ["0000-0001-2345-6789", "0000-0002-3456-7890"],
                    }
                }
            }
        )
        assert cfg.auth.admins.orcid == ["0000-0001-2345-6789", "0000-0002-3456-7890"]

    def test_admins_orcid_x_checksum(self):
        """ORCiD with X checksum digit is accepted."""
        cfg = config_from_yaml(
            {
                "auth": {
                    "admins": {
                        "orcid": ["0000-0001-2345-678X"],
                    }
                }
            }
        )
        assert cfg.auth.admins.orcid == ["0000-0001-2345-678X"]

    def test_invalid_orcid_format_raises(self):
        """Invalid ORCiD format raises validation error at parse time."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            config_from_yaml(
                {
                    "auth": {
                        "admins": {
                            "orcid": ["0000-0001-2345"],  # Too short
                        }
                    }
                }
            )

    def test_invalid_orcid_not_an_orcid_raises(self):
        """Non-ORCiD string raises validation error."""
        with pytest.raises(Exception):
            config_from_yaml(
                {
                    "auth": {
                        "admins": {
                            "orcid": ["not-an-orcid"],
                        }
                    }
                }
            )

    def test_invalid_orcid_empty_string_raises(self):
        """Empty string in ORCiD list raises validation error."""
        with pytest.raises(Exception):
            config_from_yaml(
                {
                    "auth": {
                        "admins": {
                            "orcid": [""],
                        }
                    }
                }
            )

    def test_empty_admins_default(self):
        """auth.admins defaults to empty orcid list."""
        cfg = config_from_yaml({})
        assert cfg.auth.admins.orcid == []

    def test_empty_providers_default(self):
        """auth.providers defaults to empty OrcidConfig."""
        cfg = config_from_yaml({})
        assert cfg.auth.providers.orcid.client_id == ""
