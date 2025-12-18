"""Daemon management for the OSA server."""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from enum import Enum

from osa.infrastructure.local.paths import OSAPaths


class ServerStatus(Enum):
    """Server status."""

    RUNNING = "running"
    STOPPED = "stopped"
    STALE = "stale"  # State file exists but process is dead


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
        state = self._paths.read_server_state()

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
    ) -> ServerInfo:
        """Start the server in the background.

        Args:
            host: Host to bind to.
            port: Port to bind to.

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
            self._paths.remove_server_state()

        self._paths.ensure_directories()

        # Start server as subprocess (wipe log on each start)
        log_file = self._paths.server_log.open("w")

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
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # Detach from terminal
        )

        # Write server state
        state = self._paths.write_server_state(process.pid, host, port)

        # Wait briefly and verify it started
        time.sleep(0.5)
        if process.poll() is not None:
            # Process exited immediately - error
            self._paths.remove_server_state()
            raise RuntimeError(
                f"Server failed to start. Check logs at {self._paths.server_log}"
            )

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
            self._paths.remove_server_state()
            return ServerInfo(status=ServerStatus.STOPPED)

        pid = current.pid
        assert pid is not None

        # Send SIGTERM for graceful shutdown
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            # Process already dead
            self._paths.remove_server_state()
            return ServerInfo(status=ServerStatus.STOPPED)

        # Wait for process to exit
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self._is_process_running(pid):
                self._paths.remove_server_state()
                return ServerInfo(status=ServerStatus.STOPPED)
            time.sleep(0.1)

        # Timeout - force kill
        try:
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)
        except ProcessLookupError:
            pass

        self._paths.remove_server_state()
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
