"""Tests for PostgresFieldDefinitionReader — field type map construction."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.infrastructure.persistence.adapter.discovery import PostgresFieldDefinitionReader


def _make_schema_row(srn: str, fields: list[dict]) -> dict:
    return {"srn": srn, "fields": fields}


@pytest.fixture
def mock_session() -> AsyncMock:
    return AsyncMock()


def _setup_session_result(session: AsyncMock, rows: list[dict]) -> None:
    """Configure mock session to return rows from a SELECT on schemas_table."""
    result_mock = MagicMock()
    result_mock.mappings.return_value.all.return_value = rows
    session.execute.return_value = result_mock


class TestGetAllFieldTypes:
    async def test_builds_field_map_from_multiple_schemas(self, mock_session: AsyncMock) -> None:
        rows = [
            _make_schema_row(
                "urn:osa:localhost:schema:a@1",
                [
                    {
                        "name": "title",
                        "type": "text",
                        "required": True,
                        "cardinality": "exactly_one",
                    },
                    {
                        "name": "resolution",
                        "type": "number",
                        "required": False,
                        "cardinality": "exactly_one",
                    },
                ],
            ),
            _make_schema_row(
                "urn:osa:localhost:schema:b@1",
                [
                    {
                        "name": "method",
                        "type": "term",
                        "required": True,
                        "cardinality": "exactly_one",
                        "constraints": {
                            "type": "term",
                            "ontology_srn": "urn:osa:localhost:onto:methods@1",
                        },
                    },
                ],
            ),
        ]
        _setup_session_result(mock_session, rows)

        reader = PostgresFieldDefinitionReader(session=mock_session)
        result = await reader.get_all_field_types()

        assert result == {
            "title": FieldType.TEXT,
            "resolution": FieldType.NUMBER,
            "method": FieldType.TERM,
        }

    async def test_raises_on_conflicting_types(self, mock_session: AsyncMock) -> None:
        rows = [
            _make_schema_row(
                "urn:osa:localhost:schema:a@1",
                [{"name": "value", "type": "text", "required": True, "cardinality": "exactly_one"}],
            ),
            _make_schema_row(
                "urn:osa:localhost:schema:b@1",
                [
                    {
                        "name": "value",
                        "type": "number",
                        "required": True,
                        "cardinality": "exactly_one",
                    }
                ],
            ),
        ]
        _setup_session_result(mock_session, rows)

        reader = PostgresFieldDefinitionReader(session=mock_session)
        with pytest.raises(ValidationError, match="value"):
            await reader.get_all_field_types()

    async def test_returns_empty_map_when_no_schemas(self, mock_session: AsyncMock) -> None:
        _setup_session_result(mock_session, [])

        reader = PostgresFieldDefinitionReader(session=mock_session)
        result = await reader.get_all_field_types()

        assert result == {}

    async def test_same_field_same_type_across_schemas_ok(self, mock_session: AsyncMock) -> None:
        rows = [
            _make_schema_row(
                "urn:osa:localhost:schema:a@1",
                [{"name": "title", "type": "text", "required": True, "cardinality": "exactly_one"}],
            ),
            _make_schema_row(
                "urn:osa:localhost:schema:b@1",
                [
                    {
                        "name": "title",
                        "type": "text",
                        "required": False,
                        "cardinality": "exactly_one",
                    }
                ],
            ),
        ]
        _setup_session_result(mock_session, rows)

        reader = PostgresFieldDefinitionReader(session=mock_session)
        result = await reader.get_all_field_types()

        assert result == {"title": FieldType.TEXT}
