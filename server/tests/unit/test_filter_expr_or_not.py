"""US2 tests: FilterExpr accepts OR/NOT trees and validation walks them correctly."""

from unittest.mock import AsyncMock

import pytest

from osa.config import Config
from osa.domain.discovery.model.refs import MetadataFieldRef
from osa.domain.discovery.model.value import (
    And,
    FilterOperator,
    Not,
    Or,
    Predicate,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaSRN


SCHEMA = SchemaSRN.parse("urn:osa:localhost:schema:bio-sample@1.0.0")


def _config() -> Config:
    import os

    os.environ.setdefault("OSA_AUTH__JWT__SECRET", "a" * 64)
    os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
    return Config()  # type: ignore[call-arg]


def _svc() -> DiscoveryService:
    read_store = AsyncMock()
    read_store.search_records.return_value = []
    reader = AsyncMock()
    fm = {
        "title": FieldType.TEXT,
        "resolution": FieldType.NUMBER,
    }
    reader.get_all_field_types.return_value = fm
    reader.get_fields_for_schema.return_value = fm
    return DiscoveryService(read_store=read_store, field_reader=reader, config=_config())


def _pred(field: str, op: FilterOperator, value: object) -> Predicate:
    return Predicate(field=MetadataFieldRef(field=field), op=op, value=value)


class TestOrNot:
    async def test_or_tree_accepted(self):
        svc = _svc()
        tree = Or(
            operands=[
                _pred("title", FilterOperator.EQ, "A"),
                _pred("title", FilterOperator.EQ, "B"),
            ]
        )
        await svc.search_records(
            filter_expr=tree,
            schema_srn=SCHEMA,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
        )

    async def test_not_tree_accepted(self):
        svc = _svc()
        tree = Not(operand=_pred("title", FilterOperator.EQ, "X"))
        await svc.search_records(
            filter_expr=tree,
            schema_srn=SCHEMA,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
        )

    async def test_nested_mixed_tree(self):
        svc = _svc()
        tree = And(
            operands=[
                _pred("title", FilterOperator.EQ, "X"),
                Or(
                    operands=[
                        _pred("resolution", FilterOperator.GTE, 3.0),
                        _pred("resolution", FilterOperator.LT, 1.0),
                    ]
                ),
                Not(operand=_pred("title", FilterOperator.EQ, "Bad")),
            ]
        )
        await svc.search_records(
            filter_expr=tree,
            schema_srn=SCHEMA,
            convention_srn=None,
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
        )


class TestCompoundDisabledFlag:
    async def test_or_rejected_when_compound_disabled(self):
        svc = _svc()
        tree = Or(
            operands=[
                _pred("title", FilterOperator.EQ, "A"),
                _pred("title", FilterOperator.EQ, "B"),
            ]
        )
        with pytest.raises(ValidationError, match="compound_disabled|Compound"):
            await svc.search_records(
                filter_expr=tree,
                schema_srn=SCHEMA,
                convention_srn=None,
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=10,
                allow_compound=False,
            )
