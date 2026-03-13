"""OSA CLI commands: meta, emit, progress, reject, deploy."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def meta_command() -> str:
    """Generate and return manifest JSON from the hook registry."""
    from osa.manifest import generate_manifest

    manifest = generate_manifest()
    return manifest.model_dump_json(indent=2)


def emit_command(data: str) -> None:
    """Write feature data to $OSA_OUT/features.json."""
    output_dir = Path(os.environ.get("OSA_OUT", "/osa/out"))
    output_dir.mkdir(parents=True, exist_ok=True)
    parsed = json.loads(data)
    (output_dir / "features.json").write_text(json.dumps(parsed, indent=2))


def progress_command(
    *,
    step: str | None = None,
    status: str,
    message: str | None = None,
) -> None:
    """Append a progress entry to $OSA_OUT/progress.jsonl."""
    output_dir = Path(os.environ.get("OSA_OUT", "/osa/out"))
    output_dir.mkdir(parents=True, exist_ok=True)
    entry: dict = {"status": status}
    if step is not None:
        entry["step"] = step
    if message is not None:
        entry["message"] = message
    with (output_dir / "progress.jsonl").open("a") as f:
        f.write(json.dumps(entry) + "\n")


def reject_command(*, reason: str) -> None:
    """Write a rejection entry to $OSA_OUT/progress.jsonl."""
    output_dir = Path(os.environ.get("OSA_OUT", "/osa/out"))
    output_dir.mkdir(parents=True, exist_ok=True)
    entry = {"status": "rejected", "message": reason}
    with (output_dir / "progress.jsonl").open("a") as f:
        f.write(json.dumps(entry) + "\n")


def _parse_flag(args: list[str], flag: str) -> str | None:
    """Extract the value of a --flag from args, or None if absent."""
    for i, arg in enumerate(args):
        if arg == flag and i + 1 < len(args):
            return args[i + 1]
    return None


def app() -> None:
    """CLI entry point for the `osa` command."""
    args = sys.argv[1:]
    if not args:
        print("Usage: osa <command> [options]")
        print("Commands: meta, emit, progress, reject, deploy, login, logout, link")
        sys.exit(1)

    command = args[0]

    if command == "meta":
        print(meta_command())

    elif command == "emit":
        if len(args) < 2:
            print("Usage: osa emit <json-data>", file=sys.stderr)
            sys.exit(1)
        emit_command(args[1])

    elif command == "progress":
        kwargs: dict[str, str | None] = {"status": "info"}
        i = 1
        while i < len(args):
            if args[i] == "--step" and i + 1 < len(args):
                kwargs["step"] = args[i + 1]
                i += 2
            elif args[i] == "--status" and i + 1 < len(args):
                kwargs["status"] = args[i + 1]
                i += 2
            elif args[i] == "--message" and i + 1 < len(args):
                kwargs["message"] = args[i + 1]
                i += 2
            else:
                i += 1
        progress_command(**kwargs)  # type: ignore[arg-type]

    elif command == "reject":
        if len(args) < 2:
            print("Usage: osa reject <reason>", file=sys.stderr)
            sys.exit(1)
        reason = " ".join(args[1:])
        reject_command(reason=reason)

    elif command == "link":
        from osa.cli.link import write_link

        server_url = _parse_flag(args, "--server")
        if not server_url:
            print("Usage: osa link --server <url>", file=sys.stderr)
            sys.exit(1)

        config_path = write_link(server_url)
        print(f"Linked to {server_url.rstrip('/')}")
        print(f"Config written to {config_path}")

    elif command == "login":
        import logging

        from osa.cli.link import resolve_server
        from osa.cli.login import login

        logging.basicConfig(level=logging.INFO, format="%(message)s")
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

        server_url = resolve_server(flag=_parse_flag(args, "--server"))
        success = login(server_url)
        if not success:
            sys.exit(1)

    elif command == "logout":
        from osa.cli.link import resolve_server
        from osa.cli.logout import logout

        server_url = resolve_server(flag=_parse_flag(args, "--server"))
        logout(server_url)

    elif command == "deploy":
        import importlib
        import importlib.metadata
        import logging

        from osa.cli.deploy import deploy
        from osa.cli.link import resolve_server

        logging.basicConfig(level=logging.INFO, format="%(message)s")

        # Auto-discover convention packages via entry points
        for ep in importlib.metadata.entry_points(group="osa.conventions"):
            importlib.import_module(ep.value)

        server_url = resolve_server(flag=_parse_flag(args, "--server"))
        token = _parse_flag(args, "--token")

        # Resolve token: --token flag → OSA_TOKEN env → stored credentials
        if not token:
            from osa.cli.credentials import resolve_token

            token = resolve_token(server_url)
            if token is None:
                print(
                    "Error: Not authenticated. Run `osa login` first.",
                    file=sys.stderr,
                )
                sys.exit(1)

        result = deploy(server=server_url, token=token)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)
