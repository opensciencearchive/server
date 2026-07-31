"""GetStats query handler — the stats route must not touch RecordRepository.

Layering regression guard: Router → QueryHandler → Service → Repository. The
/stats route previously injected RecordRepository directly.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.record.model.statistics import InstanceStats
from osa.domain.record.query.get_stats import GetStats, GetStatsHandler


class TestGetStatsHandler:
    @pytest.mark.asyncio
    async def test_returns_record_count_from_service(self):
        service = AsyncMock()
        service.count.return_value = 42

        stats_store = AsyncMock()
        stats_store.count_this_month.return_value = 5
        stats_store.read_snapshot.return_value = InstanceStats(
            storage_bytes=1024,
            feature_rows=84,
            computed_at=datetime(2026, 7, 27, tzinfo=UTC),
        )

        handler = GetStatsHandler(record_service=service, stats_store=stats_store)
        result = await handler.run(GetStats())

        assert result.records == 42
        assert result.records_this_month == 5
        assert result.storage_bytes == 1024
        assert result.features_per_record == 2.0  # 84 feature rows / 42 records
        assert result.computed_at == datetime(2026, 7, 27, tzinfo=UTC)
        service.count.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_falls_back_to_live_compute_before_first_refresh(self):
        service = AsyncMock()
        service.count.return_value = 10

        stats_store = AsyncMock()
        stats_store.count_this_month.return_value = 0
        stats_store.read_snapshot.return_value = None  # no snapshot yet
        stats_store.compute_snapshot.return_value = InstanceStats(
            storage_bytes=2048,
            feature_rows=0,
            computed_at=datetime(2026, 7, 27, tzinfo=UTC),
        )

        handler = GetStatsHandler(record_service=service, stats_store=stats_store)
        result = await handler.run(GetStats())

        assert result.storage_bytes == 2048
        assert result.features_per_record == 0.0
        stats_store.compute_snapshot.assert_awaited_once()
