"""GetStats query handler — the stats route must not touch RecordRepository.

Layering regression guard: Router → QueryHandler → Service → Repository. The
/stats route previously injected RecordRepository directly.
"""

from unittest.mock import AsyncMock

import pytest

from osa.domain.record.query.get_stats import GetStats, GetStatsHandler


class TestGetStatsHandler:
    @pytest.mark.asyncio
    async def test_returns_record_count_from_service(self):
        service = AsyncMock()
        service.count.return_value = 42

        handler = GetStatsHandler(record_service=service)
        result = await handler.run(GetStats())

        assert result.records == 42
        service.count.assert_awaited_once()
