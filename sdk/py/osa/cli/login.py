"""OSA CLI login command — device flow authentication."""

from __future__ import annotations

import logging
import sys
import time
import webbrowser
from pathlib import Path
from typing import Any

import httpx

from osa.cli.credentials import _DEFAULT_PATH, write_credentials

logger = logging.getLogger(__name__)

DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"


def _poll_for_token(
    *,
    client: httpx.Client,
    server: str,
    device_code: str,
    interval: int,
    expires_in: int,
) -> dict[str, Any] | None:
    """Poll the device token endpoint until authorized, expired, or timed out.

    Returns token dict on success, None on expiry/timeout.
    """
    url = f"{server.rstrip('/')}/api/v1/auth/device/token"
    payload = {
        "device_code": device_code,
        "grant_type": DEVICE_CODE_GRANT_TYPE,
    }

    start = time.monotonic()
    backoff = interval

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= expires_in:
            return None

        try:
            resp = client.post(url, json=payload)
        except httpx.HTTPError:
            # Transient network error — backoff and retry
            time.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 30)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code >= 500:
            # Server error — backoff and retry
            time.sleep(min(backoff, 30))
            backoff = min(backoff * 2, 30)
            continue

        # 400-level: check error code
        data = resp.json()
        error = data.get("error", "")

        if error == "authorization_pending":
            time.sleep(interval)
            backoff = interval  # Reset backoff on normal pending
            continue

        if error == "slow_down":
            interval = interval + 5  # RFC 8628: increase interval
            backoff = interval  # Sync backoff with new interval
            time.sleep(interval)
            continue

        if error == "expired_token":
            return None

        # Unknown error
        logger.error(
            "Device token error: %s — %s", error, data.get("error_description", "")
        )
        return None


def login(
    server: str,
    *,
    cred_path: Path = _DEFAULT_PATH,
) -> bool:
    """Run the device flow login.

    Returns True on success, False on failure.
    """
    server = server.rstrip("/")

    with httpx.Client(timeout=30.0) as client:
        # Step 1: Initiate device authorization
        try:
            resp = client.post(f"{server}/api/v1/auth/device")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            print(f"Error: Could not reach server at {server}", file=sys.stderr)
            logger.debug("Initiation failed: %s", e)
            return False

        data = resp.json()
        device_code = data["device_code"]
        user_code = data["user_code"]
        verification_uri = data["verification_uri"]
        expires_in = data["expires_in"]
        interval = data["interval"]

        # Step 2: Display code and URL
        print(f"Open this URL in your browser: {verification_uri}")
        print(f"Enter code: {user_code}")
        print()

        # Try to open browser
        try:
            webbrowser.open(verification_uri)
        except Exception:
            pass  # Non-critical — user can open manually

        # Step 3: Poll for token
        print("Waiting for authorization...", end=" ", flush=True)
        result = _poll_for_token(
            client=client,
            server=server,
            device_code=device_code,
            interval=interval,
            expires_in=expires_in,
        )

        if result is None:
            print("failed")
            print("Device code expired. Please try again.", file=sys.stderr)
            return False

        print("done")

        # Step 4: Store credentials
        write_credentials(
            server,
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            path=cred_path,
        )

        print("Token stored.")
        return True
