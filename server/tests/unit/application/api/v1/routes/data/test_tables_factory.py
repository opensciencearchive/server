"""T033 — register_table_routes registers exactly 6 routes with stable op IDs."""

import pytest
from fastapi import APIRouter

from osa.application.api.v1.routes.data.tables import (
    format_key,
    path_for,
    register_table_routes,
)
from osa.domain.data.model.format import FORMATS


def _noop_builder(_fmt):
    async def endpoint() -> dict:  # pragma: no cover - never called in this test
        return {}

    return endpoint


def _routes(router: APIRouter):
    return [r for r in router.routes if getattr(r, "operation_id", None)]


def test_registers_six_routes() -> None:
    router = APIRouter()
    register_table_routes(router, "/{schema}/records", _noop_builder, _noop_builder, "records")
    assert len(_routes(router)) == 6


def test_stable_operation_ids_for_records() -> None:
    router = APIRouter()
    register_table_routes(router, "/{schema}/records", _noop_builder, _noop_builder, "records")
    ids = {r.operation_id for r in _routes(router)}
    assert ids == {
        "records_get_json",
        "records_post_json",
        "records_get_csv",
        "records_post_csv",
        "records_get_csv_gz",
        "records_post_csv_gz",
    }


def test_stable_operation_ids_for_feature() -> None:
    router = APIRouter()
    register_table_routes(router, "/{schema}/{feature}", _noop_builder, _noop_builder, "feature")
    ids = {r.operation_id for r in _routes(router)}
    assert ids == {
        "feature_get_json",
        "feature_post_json",
        "feature_get_csv",
        "feature_post_csv",
        "feature_get_csv_gz",
        "feature_post_csv_gz",
    }


def test_duplicate_resource_on_same_router_raises() -> None:
    # T114: a second registration with the same resource_name on the same router
    # would mint colliding operation IDs — caught loudly, not silently shipped.
    router = APIRouter()
    register_table_routes(router, "/{schema}/records", _noop_builder, _noop_builder, "records")
    with pytest.raises(ValueError, match="Duplicate operation_id"):
        register_table_routes(router, "/{schema}/other", _noop_builder, _noop_builder, "records")


def test_distinct_resources_on_same_router_coexist() -> None:
    # records + feature share one router in the real app — no false positive.
    router = APIRouter()
    register_table_routes(router, "/{schema}/records", _noop_builder, _noop_builder, "records")
    register_table_routes(router, "/{schema}/{feature}", _noop_builder, _noop_builder, "feature")
    assert len(_routes(router)) == 12


def test_paths_use_suffix() -> None:
    json_fmt = next(f for f in FORMATS if f.suffix == "")
    csv_fmt = next(f for f in FORMATS if f.suffix == "csv")
    gz_fmt = next(f for f in FORMATS if f.suffix == "csv.gz")
    assert path_for("/{schema}/records", json_fmt) == "/{schema}/records"
    assert path_for("/{schema}/records", csv_fmt) == "/{schema}/records.csv"
    assert path_for("/{schema}/records", gz_fmt) == "/{schema}/records.csv.gz"


def test_format_key_mapping() -> None:
    keys = {format_key(f) for f in FORMATS}
    assert keys == {"json", "csv", "csv_gz"}
