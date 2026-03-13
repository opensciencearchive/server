"""OSA CLI logout command."""

from __future__ import annotations

from pathlib import Path

from osa.cli.credentials import _DEFAULT_PATH, remove_credentials


def logout(
    server: str,
    *,
    cred_path: Path = _DEFAULT_PATH,
) -> None:
    """Remove stored credentials for a server URL."""
    server = server.rstrip("/")
    removed = remove_credentials(server, path=cred_path)

    if removed:
        print(f"Logged out from {server}")
    else:
        print(f"No credentials found for {server}")
