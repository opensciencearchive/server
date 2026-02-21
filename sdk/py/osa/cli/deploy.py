"""OSA deploy CLI: Dockerfile generation and image building."""

from __future__ import annotations

import re
from pathlib import Path


def generate_dockerfile(project_dir: Path) -> str:
    """Generate a Dockerfile from a project's pyproject.toml."""
    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(f"pyproject.toml not found in {project_dir}")

    content = pyproject_path.read_text()

    # Extract Python version from requires-python
    match = re.search(r'requires-python\s*=\s*">=(\d+\.\d+)"', content)
    python_version = match.group(1) if match else "3.13"

    return f"""\
FROM python:{python_version}-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir .
ENTRYPOINT ["python", "-m", "osa.runtime.entrypoint"]
"""
