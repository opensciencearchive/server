"""Unit tests for OciIngesterRunner — what the container is actually handed.

The staging directory *is* the ingester contract: `$OSA_IN/session.json` is the
only continuation mechanism (#227), so what lands there decides whether a run
advances or loops.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.model.source import IngesterDefinition, IngesterLimits, IngesterName
from osa.domain.shared.model.srn import ConventionSlug
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterOutput
from osa.infrastructure.oci.ingester_runner import OciIngesterRunner

_CONV_SLUG = ConventionSlug("test")


def _make_ingester(config: dict | None = None) -> IngesterDefinition:
    return IngesterDefinition(
        name=IngesterName("from_pdb"),
        image="ghcr.io/example/ingester:v1",
        digest="sha256:abc123",
        config=config,
        limits=IngesterLimits(timeout_seconds=3600, memory="4g", cpu="2.0"),
        source_ref="git+https://example.com/r@deadbeef",
    )


def _make_runner(files_dir: Path) -> tuple[OciIngesterRunner, dict[str, str]]:
    """A runner that records what `$OSA_IN` held at the moment the container ran.

    The staging directory is removed in `run`'s `finally`, so it has to be read
    from where the container would read it — which is the right place to assert
    the contract anyway.
    """
    staged: dict[str, str] = {}

    async def capture(image_ref, staging_dir, files, output_dir, ingester, inputs):  # noqa: ANN001, ANN202
        staged.update({path.name: path.read_text() for path in staging_dir.iterdir()})
        return IngesterOutput(records=[], session=None, files_dir=files)

    runner = OciIngesterRunner(docker=MagicMock())
    runner._resolve_image = AsyncMock(return_value="ghcr.io/example/ingester@sha256:abc123")
    runner._run_container = capture
    return runner, staged


class TestSessionStaging:
    """Whatever the server holds as the session is what the ingester is handed.

    An empty session is a session: the server persists it and hands it back on
    the next pull, so the runner must stage it. Skipping it on falsiness writes
    no session.json at all, which an ingester reads as "first batch, start from
    the beginning" — the #227 loop, one layer below the guard that detects it.
    """

    @pytest.mark.asyncio
    async def test_empty_session_is_still_staged(self, tmp_path: Path):
        files_dir = tmp_path / "files"
        runner, staged = _make_runner(files_dir)

        inputs = IngesterInputs(convention_id=_CONV_SLUG, session={})
        await runner.run(_make_ingester(), inputs, files_dir, tmp_path / "work")

        assert "session.json" in staged, "empty session was not staged for the container"
        assert staged["session.json"] == "{}"

    @pytest.mark.asyncio
    async def test_populated_session_is_staged_verbatim(self, tmp_path: Path):
        files_dir = tmp_path / "files"
        runner, staged = _make_runner(files_dir)

        inputs = IngesterInputs(convention_id=_CONV_SLUG, session={"offset": 0})
        await runner.run(_make_ingester(), inputs, files_dir, tmp_path / "work")

        assert staged["session.json"] == '{"offset": 0}'

    @pytest.mark.asyncio
    async def test_absent_session_stages_nothing(self, tmp_path: Path):
        # No session at all (batch 0) stays distinguishable from an empty one.
        files_dir = tmp_path / "files"
        runner, staged = _make_runner(files_dir)

        inputs = IngesterInputs(convention_id=_CONV_SLUG, session=None)
        await runner.run(_make_ingester(), inputs, files_dir, tmp_path / "work")

        assert "session.json" not in staged
