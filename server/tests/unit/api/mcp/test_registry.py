"""Tool registry + UI meta — the model-facing tool contract (#162).

The six view tools are model-visible and (except ``describe_dataset``) carry a
``ui://osa/*`` resource pointer; the two interaction tools are app-only —
their ``_meta.ui.visibility`` excludes ``"model"`` so hosts keep them out of
the model's tool list while widgets can still invoke them.
"""

import pytest

from osa.application.api.mcp.meta import MCP_APP_MIME, ResourceMeta, ToolMeta
from osa.application.api.mcp.models import FetchPageArgs, ListDatasetsArgs, ShowTableArgs
from osa.application.api.mcp.resources import WIDGETS, WidgetRegistry
from osa.application.api.mcp.tools import TOOLS, TOOLS_BY_NAME, Tool, ToolSpec
from osa.domain.shared.error import NotFoundError

MODEL_VISIBLE = {
    "list_datasets",
    "describe_dataset",
    "show_table",
    "show_chart",
    "show_record",
    "show_filter_panel",
}
APP_ONLY = {"fetch_page", "sample_values"}


class TestRegistry:
    def test_all_tools_present(self):
        assert set(TOOLS_BY_NAME) == MODEL_VISIBLE | APP_ONLY

    def test_app_only_flags(self):
        for cls in TOOLS:
            assert cls.spec.app_only == (cls.spec.name in APP_ONLY), cls.spec.name

    def test_view_tools_carry_ui_resource(self):
        expected = {
            "list_datasets": "ui://osa/dataset-overview",
            "show_table": "ui://osa/table",
            "show_chart": "ui://osa/chart",
            "show_record": "ui://osa/record",
            "show_filter_panel": "ui://osa/filter-panel",
        }
        for name, uri in expected.items():
            assert TOOLS_BY_NAME[name].spec.resource_uri == uri

    def test_describe_dataset_returns_data_only(self):
        # The manifest is for the model to reason over — no widget.
        assert TOOLS_BY_NAME["describe_dataset"].spec.resource_uri is None

    def test_app_only_tools_have_no_ui_resource(self):
        for name in APP_ONLY:
            assert TOOLS_BY_NAME[name].spec.resource_uri is None

    def test_input_models(self):
        assert TOOLS_BY_NAME["show_table"].spec.input_model is ShowTableArgs
        assert TOOLS_BY_NAME["fetch_page"].spec.input_model is FetchPageArgs

    def test_descriptions_ground_the_model(self):
        # Every tool must explain itself; the chart tool must state the
        # bounded-fetch ceiling rather than silently truncating.
        for cls in TOOLS:
            assert cls.spec.description and len(cls.spec.description) > 20, cls.spec.name
        assert "bounded" in TOOLS_BY_NAME["show_chart"].spec.description.lower()


class TestToolContractEnforcement:
    def test_missing_spec_fails_at_class_creation(self):
        from osa.domain.data.query.view import GetDatasetListHandler

        with pytest.raises(TypeError, match="spec"):

            class NoSpec(Tool[ListDatasetsArgs, GetDatasetListHandler, ListDatasetsArgs]):
                handler_type = GetDatasetListHandler

                async def run(self, args):  # pragma: no cover — never constructed
                    raise NotImplementedError

    def test_missing_handler_type_fails_at_class_creation(self):
        with pytest.raises(TypeError, match="handler_type"):

            class NoHandler(Tool[ListDatasetsArgs, object, ListDatasetsArgs]):  # type: ignore[type-var]
                spec = ToolSpec(
                    name="x",
                    title="x",
                    description="a description long enough to pass",
                    input_model=ListDatasetsArgs,
                )

                async def run(self, args):  # pragma: no cover — never constructed
                    raise NotImplementedError


class TestUiMeta:
    def test_model_visible_meta(self):
        meta = ToolMeta.build(resource_uri="ui://osa/table", app_only=False).dump()
        assert meta == {"ui": {"resourceUri": "ui://osa/table", "visibility": ["model", "app"]}}

    def test_app_only_meta_excludes_model(self):
        meta = ToolMeta.build(resource_uri=None, app_only=True).dump()
        assert "model" not in meta["ui"]["visibility"]
        assert "resourceUri" not in meta["ui"]

    def test_resource_meta_defaults_to_default_deny_csp(self):
        meta = ResourceMeta().dump()
        assert meta["ui"]["csp"]["connectDomains"] == []
        assert meta["ui"]["csp"]["resourceDomains"] == []


class TestWidgetRegistry:
    def test_five_widgets_registered(self):
        assert {w.uri for w in WIDGETS} == {
            "ui://osa/dataset-overview",
            "ui://osa/table",
            "ui://osa/chart",
            "ui://osa/record",
            "ui://osa/filter-panel",
        }

    def test_read_returns_bundle_html_with_app_mime(self, tmp_path):
        (tmp_path / "table.html").write_text("<!doctype html><html>t</html>")
        registry = WidgetRegistry(bundle_dir=tmp_path)
        contents = registry.read("ui://osa/table")
        assert contents.mime_type == MCP_APP_MIME
        assert "<!doctype html>" in contents.content

    def test_read_declares_default_deny_csp(self, tmp_path):
        (tmp_path / "chart.html").write_text("<html>c</html>")
        registry = WidgetRegistry(bundle_dir=tmp_path)
        contents = registry.read("ui://osa/chart")
        csp = contents.meta["ui"]["csp"]
        assert csp["connectDomains"] == []
        assert csp["resourceDomains"] == []

    def test_unknown_uri_raises_not_found(self, tmp_path):
        registry = WidgetRegistry(bundle_dir=tmp_path)
        with pytest.raises(NotFoundError):
            registry.read("ui://osa/nope")

    def test_missing_bundle_file_raises_actionable_error(self, tmp_path):
        registry = WidgetRegistry(bundle_dir=tmp_path)
        with pytest.raises(NotFoundError, match="widgets-build"):
            registry.read("ui://osa/table")
