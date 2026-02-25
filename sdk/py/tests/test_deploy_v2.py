"""Tests for osa deploy — convention payload building and server registration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from osa._registry import ConventionInfo, HookInfo, SourceInfo, clear


class FakeSchema:
    """Fake schema class that mimics MetadataSchema."""

    __name__ = "FakeSchema"

    @classmethod
    def to_field_definitions(cls) -> list[dict]:
        return [
            {
                "name": "title",
                "type": "text",
                "required": True,
                "cardinality": "exactly_one",
            },
            {
                "name": "score",
                "type": "number",
                "required": False,
                "cardinality": "exactly_one",
                "constraints": {"type": "number", "unit": "\u00c5"},
            },
        ]


class FakeSource:
    name = "test-source"
    schedule = None
    initial_run = None

    class RuntimeConfig:
        def model_dump(self) -> dict:
            return {"email": "", "batch_size": 100}


def fake_hook(record):
    pass


fake_hook.__name__ = "detect_pockets"


class TestConventionToPayload:
    def test_builds_payload_with_schema_fields(self) -> None:
        from osa.cli.deploy import _convention_to_payload

        conv = ConventionInfo(
            title="Test Convention",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={
                "accepted_types": [".csv"],
                "max_count": 10,
                "max_file_size": 1000,
            },
            hooks=[],
            source_type=None,
            source_info=None,
        )

        payload = _convention_to_payload(conv, [])
        assert payload["title"] == "Test Convention"
        assert payload["version"] == "1.0.0"
        assert len(payload["schema"]) == 2
        assert payload["schema"][0]["name"] == "title"
        assert payload["source"] is None

    def test_includes_source_definition(self) -> None:
        from osa.cli.deploy import _convention_to_payload

        source_info = SourceInfo(source_cls=FakeSource, name="test-source")

        conv = ConventionInfo(
            title="Test",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={
                "accepted_types": [".cif"],
                "max_count": 5,
                "max_file_size": 500,
            },
            hooks=[],
            source_type=FakeSource,
            source_info=source_info,
        )

        payload = _convention_to_payload(
            conv,
            [],
            source_image=("osa-hooks-sources/test-source:latest", "sha256:abc123"),
        )
        assert payload["source"] is not None
        assert payload["source"]["image"] == "osa-hooks-sources/test-source:latest"
        assert payload["source"]["digest"] == "sha256:abc123"
        assert payload["source"]["runner"] == "oci"
        assert payload["source"]["config"] == {"email": "", "batch_size": 100}
        assert payload["source"]["limits"]["timeout_seconds"] == 3600

    def test_source_none_when_no_source(self) -> None:
        from osa.cli.deploy import _convention_to_payload

        conv = ConventionInfo(
            title="Test",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={"accepted_types": [".csv"]},
            hooks=[],
            source_type=None,
            source_info=None,
        )

        payload = _convention_to_payload(conv, [])
        assert payload["source"] is None

    def test_source_none_when_no_source_image_provided(self) -> None:
        """Even with a source_type, if no source_image tuple is given, source is None."""
        from osa.cli.deploy import _convention_to_payload

        source_info = SourceInfo(source_cls=FakeSource, name="test-source")

        conv = ConventionInfo(
            title="Test",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={"accepted_types": [".cif"]},
            hooks=[],
            source_type=FakeSource,
            source_info=source_info,
        )

        payload = _convention_to_payload(conv, [])
        assert payload["source"] is None

    def test_adds_min_count_if_missing(self) -> None:
        from osa.cli.deploy import _convention_to_payload

        conv = ConventionInfo(
            title="Test",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={
                "accepted_types": [".csv"],
                "max_count": 10,
                "max_file_size": 1000,
            },
            hooks=[],
            source_type=None,
            source_info=None,
        )

        payload = _convention_to_payload(conv, [])
        assert payload["file_requirements"]["min_count"] == 0

    def test_includes_hook_definitions(self) -> None:
        from osa.cli.deploy import _convention_to_payload

        hook_defs = [
            {
                "image": "osa-hooks/detect_pockets:latest",
                "digest": "sha256:abc123",
                "runner": "oci",
                "config": None,
                "limits": {"timeout_seconds": 300, "memory": "2g", "cpu": "2.0"},
                "manifest": {
                    "name": "detect_pockets",
                    "record_schema": "FakeSchema",
                    "cardinality": "many",
                    "feature_schema": {"columns": []},
                    "runner": "oci",
                },
            }
        ]

        conv = ConventionInfo(
            title="Test",
            version="1.0.0",
            schema_type=FakeSchema,
            file_requirements={
                "accepted_types": [".csv"],
                "max_count": 10,
                "max_file_size": 1000,
            },
            hooks=[fake_hook],
            source_type=None,
            source_info=None,
        )

        payload = _convention_to_payload(conv, hook_defs)
        assert len(payload["hooks"]) == 1
        assert payload["hooks"][0]["image"] == "osa-hooks/detect_pockets:latest"


class TestHookToDefinition:
    def test_builds_hook_definition(self) -> None:
        from pydantic import BaseModel

        from osa.cli.deploy import _hook_to_definition

        class Pocket(BaseModel):
            pocket_id: int
            score: float

        hook_info = HookInfo(
            fn=fake_hook,
            name="detect_pockets",
            hook_type="hook",
            schema_type=FakeSchema,
            output_type=Pocket,
            cardinality="many",
        )

        defn = _hook_to_definition(
            hook_info, "osa-hooks/detect_pockets:latest", "sha256:abc"
        )
        assert defn["image"] == "osa-hooks/detect_pockets:latest"
        assert defn["digest"] == "sha256:abc"
        assert defn["manifest"]["name"] == "detect_pockets"
        assert defn["manifest"]["cardinality"] == "many"
        assert len(defn["manifest"]["feature_schema"]["columns"]) == 2

    def test_empty_feature_schema_when_no_output_type(self) -> None:
        from osa.cli.deploy import _hook_to_definition

        hook_info = HookInfo(
            fn=fake_hook,
            name="simple_hook",
            hook_type="hook",
            schema_type=FakeSchema,
            output_type=None,
            cardinality="one",
        )

        defn = _hook_to_definition(hook_info, "img:latest", "sha256:xyz")
        assert defn["manifest"]["feature_schema"]["columns"] == []


class TestDeployRaisesWithoutConventions:
    def setup_method(self) -> None:
        clear()

    def test_raises_if_no_conventions(self) -> None:
        from osa.cli.deploy import deploy

        with pytest.raises(RuntimeError, match="No conventions registered"):
            deploy(server="http://localhost:8000")


class TestDeployEndToEnd:
    def setup_method(self) -> None:
        clear()

    def test_builds_and_registers(self) -> None:
        from osa._registry import _conventions, _hooks

        from osa.cli.deploy import deploy

        source_info = SourceInfo(source_cls=FakeSource, name="test-source")

        # Register a fake convention and hook
        _hooks.append(
            HookInfo(
                fn=fake_hook,
                name="detect_pockets",
                hook_type="hook",
                schema_type=FakeSchema,
                output_type=None,
                cardinality="many",
            )
        )
        _conventions.append(
            ConventionInfo(
                title="PDB Structures",
                version="1.0.0",
                schema_type=FakeSchema,
                file_requirements={
                    "accepted_types": [".cif"],
                    "max_count": 5,
                    "max_file_size": 500_000_000,
                },
                hooks=[fake_hook],
                source_type=FakeSource,
                source_info=source_info,
            )
        )

        # Mock docker build + inspect
        mock_run = MagicMock()
        mock_run.return_value = MagicMock(stdout="sha256:fakedigest\n")

        # Mock httpx.post
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "srn": "urn:osa:localhost:conv:abc",
            "title": "PDB Structures",
        }
        mock_response.raise_for_status = MagicMock()

        mock_httpx = MagicMock()
        mock_httpx.post.return_value = mock_response

        with (
            patch("osa.cli.deploy.subprocess.run", mock_run),
            patch.dict("sys.modules", {"httpx": mock_httpx}),
            patch("osa.cli.deploy.Path.write_text"),
            patch("osa.cli.deploy.Path.unlink"),
        ):
            result = deploy(
                server="http://localhost:8000",
                token="fake-jwt",
            )

        assert result["srn"] == "urn:osa:localhost:conv:abc"

        # Verify POST was made to correct URL
        call_args = mock_httpx.post.call_args
        payload = call_args[1]["json"]
        assert payload["title"] == "PDB Structures"
        # source is now an object (not source_name/source_config)
        assert payload["source"] is not None
        assert payload["source"]["runner"] == "oci"
        assert payload["source"]["config"] == {"email": "", "batch_size": 100}
        assert "Bearer fake-jwt" in call_args[1]["headers"]["Authorization"]
