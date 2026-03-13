"""Credential storage for OSA CLI.

Stores and retrieves authentication tokens keyed by server URL.
Credentials file: ~/.config/osa/credentials.json (0600 permissions).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_DEFAULT_PATH = Path.home() / ".config" / "osa" / "credentials.json"


def _normalize_server(server: str) -> str:
    """Normalize server URL by stripping trailing slashes."""
    return server.rstrip("/")


def _read_file(path: Path) -> dict[str, Any]:
    """Read the credentials file, returning empty dict if missing or invalid."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_file(path: Path, data: dict[str, Any]) -> None:
    """Write data to credentials file with 0600 permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    path.chmod(0o600)


def write_credentials(
    server: str,
    *,
    access_token: str,
    refresh_token: str,
    path: Path = _DEFAULT_PATH,
) -> None:
    """Store credentials for a server URL.

    Creates the file if it doesn't exist. Overwrites existing entry
    for the same server. Preserves entries for other servers.
    """
    server = _normalize_server(server)
    data = _read_file(path)
    data[server] = {
        "access_token": access_token,
        "refresh_token": refresh_token,
    }
    _write_file(path, data)


def read_credentials(
    server: str,
    *,
    path: Path = _DEFAULT_PATH,
) -> dict[str, str] | None:
    """Read credentials for a server URL.

    Returns dict with access_token and refresh_token, or None if not found.
    """
    server = _normalize_server(server)
    data = _read_file(path)
    entry = data.get(server)
    if entry and "access_token" in entry:
        return entry
    return None


def remove_credentials(
    server: str,
    *,
    path: Path = _DEFAULT_PATH,
) -> bool:
    """Remove credentials for a server URL.

    Returns True if credentials were removed, False if not found.
    """
    server = _normalize_server(server)
    data = _read_file(path)
    if server not in data:
        return False
    del data[server]
    _write_file(path, data)
    return True


def refresh_access_token(
    server: str,
    *,
    path: Path = _DEFAULT_PATH,
) -> str | None:
    """Attempt to refresh the access token using the stored refresh token.

    On success, updates stored credentials and returns the new access token.
    On failure, returns None.
    """
    import httpx

    creds = read_credentials(server, path=path)
    if creds is None or "refresh_token" not in creds:
        return None

    url = f"{_normalize_server(server)}/api/v1/auth/refresh"
    try:
        resp = httpx.post(
            url,
            json={"refresh_token": creds["refresh_token"]},
            timeout=10.0,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()
        write_credentials(
            server,
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            path=path,
        )
        return data["access_token"]
    except (httpx.HTTPError, ValueError, KeyError):
        return None


def resolve_token(
    server: str,
    *,
    path: Path = _DEFAULT_PATH,
) -> str | None:
    """Resolve an access token for a server URL.

    Resolution chain:
    1. OSA_TOKEN environment variable (for CI/CD)
    2. Stored credentials file
    3. None (not authenticated)
    """
    env_token = os.environ.get("OSA_TOKEN")
    if env_token:
        return env_token

    creds = read_credentials(server, path=path)
    if creds:
        return creds["access_token"]

    return None
