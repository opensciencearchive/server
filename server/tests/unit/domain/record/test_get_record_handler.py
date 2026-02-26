"""TDD Red: Tests for GetRecord query handler."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.record.model.aggregate import Record
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_record() -> Record:
    return Record(
        srn=_make_record_srn(),
        deposition_srn=_make_dep_srn(),
        metadata={"title": "Test Protein"},
        published_at=datetime.now(UTC),
    )


class TestGetRecordHandler:
    @pytest.mark.asyncio
    async def test_returns_record_detail(self):
        from osa.domain.record.query.get_record import GetRecord, GetRecordHandler

        record = _make_record()
        service = AsyncMock()
        service.get.return_value = record

        handler = GetRecordHandler(record_service=service)
        result = await handler.run(GetRecord(srn=record.srn))

        assert result.srn == record.srn
        assert result.deposition_srn == record.deposition_srn
        assert result.metadata == record.metadata
        service.get.assert_called_once_with(record.srn)

    @pytest.mark.asyncio
    async def test_raises_not_found(self):
        from osa.domain.record.query.get_record import GetRecord, GetRecordHandler

        service = AsyncMock()
        service.get.side_effect = NotFoundError("not found")

        handler = GetRecordHandler(record_service=service)

        with pytest.raises(NotFoundError):
            await handler.run(GetRecord(srn=_make_record_srn()))

    @pytest.mark.asyncio
    async def test_handler_is_public(self):
        """GetRecord should be accessible without authentication."""
        from osa.domain.record.query.get_record import GetRecordHandler
        from osa.domain.shared.authorization.gate import Public

        assert isinstance(GetRecordHandler.__auth__, Public)
