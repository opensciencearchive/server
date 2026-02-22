"""OSA deploy CLI: build hook/source images and register conventions with the server."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from osa._registry import ConventionInfo, HookInfo, SourceInfo, _conventions, _hooks
from osa.manifest import generate_feature_schema

logger = logging.getLogger(__name__)


def _read_python_version(project_dir: Path) -> str:
    """Read requires-python from pyproject.toml, default to 3.13."""
    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(f"pyproject.toml not found in {project_dir}")
    content = pyproject_path.read_text()
    match = re.search(r'requires-python\s*=\s*">=(\d+\.\d+)"', content)
    return match.group(1) if match else "3.13"


def _find_sdk_path(project_dir: Path) -> Path | None:
    """Resolve the local OSA SDK path from [tool.uv.sources] in pyproject.toml."""
    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.exists():
        return None
    content = pyproject_path.read_text()
    match = re.search(r'osa\s*=\s*\{\s*path\s*=\s*"([^"]+)"', content)
    if match:
        sdk_path = (project_dir / match.group(1)).resolve()
        if sdk_path.exists():
            return sdk_path
    return None


def generate_hook_dockerfile(project_dir: Path) -> str:
    """Generate a Dockerfile for an OCI hook container."""
    python_version = _read_python_version(project_dir)
    sdk_path = _find_sdk_path(project_dir)

    if sdk_path:
        return f"""\
FROM python:{python_version}-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY .osa-sdk /app/.osa-sdk
RUN pip install --no-cache-dir /app/.osa-sdk
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["osa-run-hook"]
"""

    return f"""\
FROM python:{python_version}-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["osa-run-hook"]
"""


def generate_source_dockerfile(project_dir: Path) -> str:
    """Generate a Dockerfile for an OCI source container."""
    python_version = _read_python_version(project_dir)
    sdk_path = _find_sdk_path(project_dir)

    if sdk_path:
        return f"""\
FROM python:{python_version}-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY .osa-sdk /app/.osa-sdk
RUN pip install --no-cache-dir /app/.osa-sdk
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["osa-run-source"]
"""

    return f"""\
FROM python:{python_version}-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["osa-run-source"]
"""


# Keep old name as alias for backward compatibility in tests
generate_dockerfile = generate_hook_dockerfile


def _stage_sdk(project_dir: Path) -> Path | None:
    """Copy the local SDK into the build context. Returns the staged path or None."""
    sdk_path = _find_sdk_path(project_dir)
    if sdk_path is None:
        return None
    staged = project_dir / ".osa-sdk"
    if staged.exists():
        shutil.rmtree(staged)
    shutil.copytree(
        sdk_path,
        staged,
        ignore=shutil.ignore_patterns(
            "__pycache__",
            "*.pyc",
            ".venv",
            "*.egg-info",
        ),
    )
    return staged


def _build_image(
    name: str,
    dockerfile_content: str,
    project_dir: Path,
    tag_prefix: str,
) -> tuple[str, str]:
    """Build a Docker image and return (image_tag, digest)."""
    dockerfile_path = project_dir / f".osa-Dockerfile.{name}"
    dockerfile_path.write_text(dockerfile_content)
    staged_sdk = _stage_sdk(project_dir)

    tag = f"{tag_prefix}/{name}:latest"

    try:
        logger.info("Building image for %s → %s", name, tag)
        build = subprocess.run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile_path),
                "-t",
                tag,
                str(project_dir),
            ],
            capture_output=True,
            text=True,
        )
        if build.returncode != 0:
            logger.error(
                "Docker build failed for %s:\n%s", name, build.stderr or build.stdout
            )
            build.check_returncode()

        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.Id}}", tag],
            check=True,
            capture_output=True,
            text=True,
        )
        digest = result.stdout.strip()
        logger.info("Built %s → %s", tag, digest)
        return tag, digest
    finally:
        dockerfile_path.unlink(missing_ok=True)
        if staged_sdk and staged_sdk.exists():
            shutil.rmtree(staged_sdk)


def _build_hook_image(
    hook: HookInfo,
    project_dir: Path,
    tag_prefix: str,
) -> tuple[str, str]:
    """Build a Docker image for a hook and return (image_tag, digest)."""
    dockerfile_content = generate_hook_dockerfile(project_dir)
    return _build_image(hook.name, dockerfile_content, project_dir, tag_prefix)


def _build_source_image(
    source: SourceInfo,
    project_dir: Path,
    tag_prefix: str,
) -> tuple[str, str]:
    """Build a Docker image for a source and return (image_tag, digest)."""
    dockerfile_content = generate_source_dockerfile(project_dir)
    return _build_image(
        source.name, dockerfile_content, project_dir, f"{tag_prefix}-sources"
    )


def _hook_to_definition(
    hook: HookInfo,
    image: str,
    digest: str,
) -> dict[str, Any]:
    """Build a HookDefinition dict from a HookInfo + image details."""
    feature_schema: dict[str, Any] = {"columns": []}
    if hook.output_type is not None and hasattr(hook.output_type, "model_fields"):
        fs = generate_feature_schema(hook.output_type)
        feature_schema = fs.model_dump()

    return {
        "image": image,
        "digest": digest,
        "runner": "oci",
        "config": None,
        "limits": {
            "timeout_seconds": 300,
            "memory": "2g",
            "cpu": "2.0",
        },
        "manifest": {
            "name": hook.name,
            "record_schema": hook.schema_type.__name__ if hook.schema_type else "",
            "cardinality": hook.cardinality,
            "feature_schema": feature_schema,
            "runner": "oci",
        },
    }


def _convention_to_payload(
    conv: ConventionInfo,
    hook_definitions: list[dict[str, Any]],
    source_image: tuple[str, str] | None = None,
) -> dict[str, Any]:
    """Build the CreateConvention request payload."""
    schema_fields = conv.schema_type.to_field_definitions()

    file_reqs = conv.file_requirements
    if "min_count" not in file_reqs:
        file_reqs = {**file_reqs, "min_count": 0}

    source: dict[str, Any] | None = None
    if source_image is not None and conv.source_type is not None:
        image, digest = source_image
        config = None
        if hasattr(conv.source_type, "RuntimeConfig"):
            config = conv.source_type.RuntimeConfig().model_dump()

        schedule = None
        initial_run = None
        if conv.source_info is not None:
            if conv.source_info.schedule is not None:
                schedule = conv.source_info.schedule.model_dump()
            if conv.source_info.initial_run is not None:
                initial_run = conv.source_info.initial_run.model_dump()

        source = {
            "image": image,
            "digest": digest,
            "runner": "oci",
            "config": config,
            "limits": {
                "timeout_seconds": 3600,
                "memory": "4g",
                "cpu": "2.0",
            },
            "schedule": schedule,
            "initial_run": initial_run,
        }

    return {
        "title": conv.title,
        "version": conv.version,
        "schema": schema_fields,
        "file_requirements": file_reqs,
        "hooks": hook_definitions,
        "source": source,
    }


def deploy(
    server: str,
    project_dir: Path | None = None,
    tag_prefix: str = "osa-hooks",
    token: str | None = None,
) -> dict[str, Any]:
    """Build hook/source images and register conventions with the OSA server.

    Returns the server response for the created convention.
    """
    if project_dir is None:
        project_dir = Path.cwd()

    if not _conventions:
        raise RuntimeError(
            "No conventions registered. "
            "Make sure the convention package is imported before calling deploy."
        )

    # Build images for each hook
    hook_images: dict[str, tuple[str, str]] = {}
    for hook in _hooks:
        image, digest = _build_hook_image(hook, project_dir, tag_prefix)
        hook_images[hook.name] = (image, digest)

    # Build images for sources
    source_images: dict[str, tuple[str, str]] = {}
    for conv in _conventions:
        if conv.source_info is not None:
            name = conv.source_info.name
            if name not in source_images:
                image, digest = _build_source_image(
                    conv.source_info, project_dir, tag_prefix
                )
                source_images[name] = (image, digest)

    results: list[dict[str, Any]] = []

    for conv in _conventions:
        # Match hooks to this convention
        hook_defs = []
        for h in conv.hooks:
            name = h.__name__
            if name in hook_images:
                image, digest = hook_images[name]
                hook_info = next(hi for hi in _hooks if hi.name == name)
                hook_defs.append(_hook_to_definition(hook_info, image, digest))

        # Get source image if applicable
        source_img = None
        if conv.source_info is not None and conv.source_info.name in source_images:
            source_img = source_images[conv.source_info.name]

        payload = _convention_to_payload(conv, hook_defs, source_img)

        logger.info("Registering convention '%s' with %s", conv.title, server)

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        import httpx

        url = f"{server.rstrip('/')}/api/v1/conventions"
        resp = httpx.post(url, json=payload, headers=headers, timeout=30.0)
        resp.raise_for_status()
        result = resp.json()
        results.append(result)

        logger.info("Convention registered: %s", result.get("srn", ""))

    return results[0] if len(results) == 1 else {"conventions": results}
