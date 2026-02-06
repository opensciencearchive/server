"""Daemon management for the OSA server."""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum

from pydantic import ValidationError

from osa.cli.util.paths import OSAPaths
from osa.cli.util.server_state import (
    read_server_state,
    remove_server_state,
    write_server_state,
)


class ServerStatus(Enum):
    """Server status."""

    RUNNING = "running"
    STOPPED = "stopped"
    STALE = "stale"  # State file exists but process is dead


class ConfigError(Exception):
    """Raised when configuration validation fails."""

    def __init__(self, message: str, details: list[str] | None = None) -> None:
        self.message = message
        self.details = details or []
        super().__init__(message)


@dataclass
class ServerInfo:
    """Information about the server."""

    status: ServerStatus
    pid: int | None = None
    host: str | None = None
    port: int | None = None
    started_at: str | None = None


class DaemonManager:
    """Manages the OSA server daemon."""

    def __init__(self, paths: OSAPaths | None = None) -> None:
        self._paths = paths or OSAPaths()

    @property
    def paths(self) -> OSAPaths:
        """Access to paths."""
        return self._paths

    def status(self) -> ServerInfo:
        """Get server status."""
        state = read_server_state(self._paths.server_state_file)

        if state is None:
            return ServerInfo(status=ServerStatus.STOPPED)

        if self._is_process_running(state.pid):
            return ServerInfo(
                status=ServerStatus.RUNNING,
                pid=state.pid,
                host=state.host,
                port=state.port,
                started_at=state.started_at,
            )

        # State file exists but process is dead - stale
        return ServerInfo(
            status=ServerStatus.STALE,
            pid=state.pid,
            host=state.host,
            port=state.port,
        )

    def start(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        config_file: str | None = None,
    ) -> ServerInfo:
        """Start the server in the background.

        Args:
            host: Host to bind to.
            port: Port to bind to.
            config_file: Path to YAML config file.

        Returns:
            ServerInfo with status and PID.

        Raises:
            RuntimeError: If server is already running.
        """
        current = self.status()

        if current.status == ServerStatus.RUNNING:
            raise RuntimeError(
                f"Server already running on http://{current.host}:{current.port} "
                f"(PID {current.pid})"
            )

        if current.status == ServerStatus.STALE:
            # Clean up stale state file
            remove_server_state(self._paths.server_state_file)

        self._paths.ensure_directories()

        # Lazy imports - only load heavy deps when actually starting server
        from osa.config import Config
        from osa.infrastructure.persistence.migrate import run_migrations

        # Load config and run migrations
        if config_file:
            os.environ["OSA_CONFIG_FILE"] = config_file

        try:
            # Pydantic Settings populates from env vars at runtime
            app_config = Config()  # type: ignore[call-arg]
        except ValidationError as e:
            details = []
            for err in e.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", "Unknown error")
                details.append(f"{loc}: {msg}")
            raise ConfigError(
                "Invalid configuration",
                details=details,
            ) from None

        if app_config.database.auto_migrate:
            print("Running database migrations...")
            run_migrations(app_config.database.url)
            print("Migrations complete.")

        # Build environment with config file and log path
        env = os.environ.copy()
        if config_file:
            env["OSA_CONFIG_FILE"] = config_file
        # Pass log file path for the app to configure logging directly
        env["OSA_LOG_FILE"] = str(self._paths.server_log)

        # Open log file for uvicorn output (append mode)
        log_file = open(self._paths.server_log, "a")

        # Use uvicorn directly via subprocess
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "osa.application.api.rest.app:app",
                "--host",
                host,
                "--port",
                str(port),
                "--access-log",  # Log each request
            ],
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,  # Detach from terminal
            env=env,
        )

        # Write server state
        state = write_server_state(self._paths.server_state_file, process.pid, host, port)

        # Wait briefly and verify it started
        time.sleep(0.5)
        if process.poll() is not None:
            # Process exited immediately - error
            remove_server_state(self._paths.server_state_file)
            raise RuntimeError(f"Server failed to start. Check logs at {self._paths.server_log}")

        return ServerInfo(
            status=ServerStatus.RUNNING,
            pid=process.pid,
            host=host,
            port=port,
            started_at=state.started_at,
        )

    def stop(self, timeout: float = 10.0) -> ServerInfo:
        """Stop the server.

        Args:
            timeout: Seconds to wait for graceful shutdown before SIGKILL.

        Returns:
            ServerInfo with stopped status.

        Raises:
            RuntimeError: If server is not running.
        """
        current = self.status()

        if current.status == ServerStatus.STOPPED:
            raise RuntimeError("Server is not running")

        if current.status == ServerStatus.STALE:
            # Just clean up the stale state file
            remove_server_state(self._paths.server_state_file)
            return ServerInfo(status=ServerStatus.STOPPED)

        pid = current.pid
        assert pid is not None

        # Send SIGTERM for graceful shutdown
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            # Process already dead
            remove_server_state(self._paths.server_state_file)
            return ServerInfo(status=ServerStatus.STOPPED)

        # Wait for process to exit
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self._is_process_running(pid):
                remove_server_state(self._paths.server_state_file)
                return ServerInfo(status=ServerStatus.STOPPED)
            time.sleep(0.1)

        # Timeout - force kill
        try:
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)
        except ProcessLookupError:
            pass

        remove_server_state(self._paths.server_state_file)
        return ServerInfo(status=ServerStatus.STOPPED)

    def _is_process_running(self, pid: int) -> bool:
        """Check if a process is running."""
        try:
            os.kill(pid, 0)  # Signal 0 just checks if process exists
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but we don't have permission to signal it
            return True
