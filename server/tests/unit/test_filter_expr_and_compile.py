"""US1 tests: FilterExpr AND-tree validation via DiscoveryService bounds."""

from unittest.mock import AsyncMock

import pytest

from osa.config import Config
from osa.domain.discovery.model.refs import FeatureFieldRef, MetadataFieldRef
from osa.domain.discovery.model.value import (
    And,
    FilterOperator,
    Predicate,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaId


SCHEMA = SchemaId.parse("bio-sample@1.0.0")


def _config(overrides: dict | None = None) -> Config:
    import os

    os.environ.setdefault("OSA_AUTH__JWT__SECRET", "a" * 64)
    os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
    cfg = Config()  # type: ignore[call-arg]
    if overrides:
        for k, v in overrides.items():
            setattr(cfg, k, v)
    return cfg


def _svc(
    *,
    field_map: dict[str, FieldType] | None = None,
    max_depth: int | None = None,
    max_preds: int | None = None,
    max_joins: int | None = None,
) -> DiscoveryService:
    read_store = AsyncMock()
    read_store.search_records.return_value = []
    read_store.get_feature_catalog.return_value = []

    reader = AsyncMock()
    fm = field_map or {
        "title": FieldType.TEXT,
        "resolution": FieldType.NUMBER,
    }
    reader.get_all_field_types.return_value = fm
    reader.get_fields_for_schema.return_value = fm

    overrides = {}
    if max_depth is not None:
        overrides["discovery_max_filter_depth"] = max_depth
    if max_preds is not None:
        overrides["discovery_max_predicates"] = max_preds
    if max_joins is not None:
        overrides["discovery_max_cross_domain_joins"] = max_joins

    return DiscoveryService(read_store=read_store, field_reader=reader, config=_config(overrides))


def _pred(field: str, op: FilterOperator, value: object) -> Predicate:
    return Predicate(field=MetadataFieldRef(field=field), op=op, value=value)


class TestAndOnlyTrees:
    async def test_accepts_and_of_predicates(self) -> None:
        svc = _svc()
        tree = And(
            operands=[
                _pred("title", FilterOperator.EQ, "x"),
                _pred("resolution", FilterOperator.GTE, 3.0),
            ]
        )
        await svc.search_records(
            filter_expr=tree,
            schema_id=SCHEMA,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=20,
        )


class TestBoundsEnforced:
    async def test_depth_exceeded(self) -> None:
        svc = _svc(max_depth=3)
        leaf = _pred("title", FilterOperator.EQ, "x")
        tree = leaf
        for _ in range(4):
            tree = And(operands=[tree, leaf])

        with pytest.raises(ValidationError, match="depth"):
            await svc.search_records(
                filter_expr=tree,
                schema_id=SCHEMA,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_predicates_exceeded(self) -> None:
        svc = _svc(max_preds=2)
        tree = And(
            operands=[
                _pred("title", FilterOperator.EQ, "x"),
                _pred("title", FilterOperator.EQ, "y"),
                _pred("resolution", FilterOperator.GTE, 3.0),
            ]
        )
        with pytest.raises(ValidationError, match="predicate leaves"):
            await svc.search_records(
                filter_expr=tree,
                schema_id=SCHEMA,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )

    async def test_joins_exceeded(self) -> None:
        svc = _svc(max_joins=1)
        # Simulate catalog advertising multiple hooks with a column named score
        svc.read_store.get_feature_catalog.return_value = [  # type: ignore[attr-defined]
            type(
                "E",
                (),
                {
                    "hook_name": "hook_a",
                    "columns": [
                        type("C", (), {"name": "score", "type": "number", "required": True})
                    ],
                },
            ),
            type(
                "E",
                (),
                {
                    "hook_name": "hook_b",
                    "columns": [
                        type("C", (), {"name": "score", "type": "number", "required": True})
                    ],
                },
            ),
        ]
        tree = And(
            operands=[
                Predicate(
                    field=FeatureFieldRef(hook="hook_a", column="score"),
                    op=FilterOperator.GT,
                    value=0.0,
                ),
                Predicate(
                    field=FeatureFieldRef(hook="hook_b", column="score"),
                    op=FilterOperator.GT,
                    value=0.0,
                ),
            ]
        )
        with pytest.raises(ValidationError, match="distinct feature hooks"):
            await svc.search_records(
                filter_expr=tree,
                schema_id=SCHEMA,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )


class TestUnknownField:
    async def test_unknown_metadata_field_rejected(self) -> None:
        svc = _svc()
        with pytest.raises(ValidationError, match="Unknown metadata field"):
            await svc.search_records(
                filter_expr=_pred("bogus", FilterOperator.EQ, "x"),
                schema_id=SCHEMA,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=20,
            )
