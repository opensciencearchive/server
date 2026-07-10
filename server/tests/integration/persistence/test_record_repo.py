"""Integration tests for PostgresRecordRepository batch-mapping recovery (#160).

``srns_for_ingest_batch`` recovers a batch's upstream_source → SRN mapping from
DB-authoritative state on workflow retry, when bulk_publish's ON CONFLICT would
otherwise return only newly inserted rows. Records from other batches, other
runs, and deposition sources must be excluded.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.repository.record import PostgresRecordRepository
from tests.integration.conftest import seed_record


def _ingest_source(*, run_id: str, upstream: str, batch_index: int | None) -> dict:
    src: dict = {
        "type": "ingest",
        "id": f"{run_id}:{upstream}",
        "ingest_run_id": run_id,
        "upstream_source": upstream,
    }
    if batch_index is not None:
        src["batch_index"] = batch_index
    return src


@pytest.mark.asyncio
class TestSrnsForIngestBatch:
    async def test_returns_only_this_run_and_batch(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        # This run+batch: two records.
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r1@1",
            source=_ingest_source(run_id="run-a", upstream="pdb:1", batch_index=2),
        )
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r2@1",
            source=_ingest_source(run_id="run-a", upstream="pdb:2", batch_index=2),
        )
        # Same run, different batch — excluded.
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r3@1",
            source=_ingest_source(run_id="run-a", upstream="pdb:3", batch_index=1),
        )
        # Different run, same batch index — excluded.
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r4@1",
            source=_ingest_source(run_id="run-b", upstream="pdb:4", batch_index=2),
        )
        # Deposition-source record — excluded.
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r5@1",
            source={"type": "deposition", "id": "dep-1"},
        )

        repo = PostgresRecordRepository(pg_session)
        mapping = await repo.srns_for_ingest_batch("run-a", 2)

        assert mapping == {
            "pdb:1": RecordSRN.parse("urn:osa:localhost:rec:r1@1"),
            "pdb:2": RecordSRN.parse("urn:osa:localhost:rec:r2@1"),
        }

    async def test_empty_dict_when_no_matches(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r1@1",
            source=_ingest_source(run_id="run-a", upstream="pdb:1", batch_index=0),
        )
        repo = PostgresRecordRepository(pg_session)

        assert await repo.srns_for_ingest_batch("run-a", 9) == {}
        assert await repo.srns_for_ingest_batch("nope", 0) == {}

    async def test_legacy_rows_without_batch_index_excluded(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """A record predating the field (batch_index absent) never matches."""
        await seed_record(
            pg_engine,
            srn="urn:osa:localhost:rec:r1@1",
            source=_ingest_source(run_id="run-a", upstream="pdb:1", batch_index=None),
        )
        repo = PostgresRecordRepository(pg_session)

        assert await repo.srns_for_ingest_batch("run-a", 0) == {}
