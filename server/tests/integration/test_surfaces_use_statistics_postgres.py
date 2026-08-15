"""Read surfaces consume ``table_statistics`` (#219 phase 6).

The manifest (and through it the catalog, SKILL.md, and the MCP views), the
dashboard stats query, and the instance snapshot all previously recounted
tables per render. They now read the lockstep-maintained counts: zero
``count(`` statements on any request path — the only sanctioned counting is
the one-time backfill migration and the admin verifier.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.auth.model.principal import Principal, ProviderIdentity
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.data.command.verify_statistics import (
    VerifyTableStatistics,
    VerifyTableStatisticsHandler,
)
from osa.domain.data.query.get_stats import GetStats, GetStatsHandler
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.record.model.aggregate import Record
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.source import IngestSource
from osa.domain.shared.model.srn import ConventionSlug, Domain, RecordSRN, SchemaId
from osa.infrastructure.data.postgres_catalog_read_store import PostgresCatalogReadStore
from osa.infrastructure.data.postgres_statistics_store import PostgresStatisticsStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.record import PostgresRecordRepository
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table, table_statistics_table

from tests.factories import make_convention_docs_dict
from tests.integration.conftest import seed_hook_run

SCHEMA = SchemaId.parse("compound@1.0.0")
HOOK = "surface_features"


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _record(i: int) -> Record:
    return Record(
        srn=RecordSRN.parse(f"urn:osa:localhost:rec:surf{i}@1"),
        source=IngestSource(
            id=f"surf-{i}", ingest_run_id="run-1", upstream_source=f"up-{i}", batch_index=0
        ),
        convention_id=ConventionSlug.parse("surf-conv"),
        schema_id=SCHEMA,
        metadata={"species": f"sp{i}"},
        published_at=datetime(2026, 1, 1, 12, i, tzinfo=UTC),
    )


async def _seed_all(engine: AsyncEngine, session: AsyncSession) -> None:
    """Seed through the LOCKSTEP writers so table_statistics is maintained."""
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    await session.execute(
        conventions_table.insert().values(
            id=f"{SCHEMA.id.root}-{HOOK}",
            title="conv",
            description="convention",
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            file_requirements={},
            hooks=[HOOK],
            source=None,
            docs=make_convention_docs_dict(),
            created_at=datetime.now(UTC),
        )
    )
    await session.commit()
    columns = [ColumnDef(name="score", json_type="number", required=True)]
    run_id = await seed_hook_run(engine, feature_name=HOOK, columns=columns)
    fstore = PostgresFeatureStore(engine, session)
    await fstore.create_table(HOOK, columns)

    repo = PostgresRecordRepository(session)
    await repo.save_many([_record(1), _record(2), _record(3)])
    for r in [_record(1), _record(2), _record(3)]:
        await store.insert(SCHEMA, r.srn, r.metadata)
    # Feature rows for two of the three records: 2 + 3 = 5 rows, coverage 2.
    await fstore.insert_features(
        HOOK, str(_record(1).srn), [{"score": 1.0}, {"score": 2.0}], run_id
    )
    await fstore.insert_features(
        HOOK, str(_record(2).srn), [{"score": 3.0}, {"score": 4.0}, {"score": 5.0}], run_id
    )
    await session.commit()


def _catalog_service(session: AsyncSession) -> DataCatalogService:
    return DataCatalogService(read_store=PostgresCatalogReadStore(session, Domain("localhost")))


def _counting(captured: list[str]) -> list[str]:
    return [s for s in captured if "count(" in s.lower()]


@pytest.mark.asyncio
class TestManifestConsumesStatistics:
    async def test_manifest_counts_match_truth_with_zero_count_statements(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        await _seed_all(pg_engine, pg_session)

        captured_sql.clear()
        manifest = await _catalog_service(pg_session).get_schema_manifest(SCHEMA)

        assert _counting(captured_sql) == [], (
            f"manifest render issued aggregate queries: {_counting(captured_sql)}"
        )
        by_name = {tr.name: tr for tr in manifest.table_resources}
        assert by_name["records"].row_count == 3
        assert by_name[HOOK].row_count == 5
        assert by_name[HOOK].records_covered == 2

    async def test_fresh_schema_manifest_renders_zeros(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Absent stats row means zero — never an error, never a recount."""
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA, _fields())
        await PostgresSemanticsSchemaRepository(pg_session).save(
            Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
        )
        await pg_session.commit()

        manifest = await _catalog_service(pg_session).get_schema_manifest(SCHEMA)
        by_name = {tr.name: tr for tr in manifest.table_resources}
        assert by_name["records"].row_count == 0


@pytest.mark.asyncio
class TestStatsQueryConsumesStatistics:
    async def test_records_total_comes_from_statistics_not_a_full_count(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        await _seed_all(pg_engine, pg_session)
        handler = GetStatsHandler(stats_store=PostgresStatisticsStore(pg_session))

        captured_sql.clear()
        result = await handler.run(GetStats())

        assert result.records == 3
        # count_this_month is the one sanctioned counting statement left on
        # this path: an index-served month window, never a full-table count.
        for stmt in _counting(captured_sql):
            assert "published_at" in stmt, f"full-table count on the stats path: {stmt}"

    async def test_instance_snapshot_sums_statistics_instead_of_sweeping(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        await _seed_all(pg_engine, pg_session)
        store = PostgresStatisticsStore(pg_session)

        captured_sql.clear()
        snapshot = await store.compute_snapshot()

        assert snapshot.feature_rows == 5
        assert _counting(captured_sql) == [], (
            f"snapshot swept tables with count(): {_counting(captured_sql)}"
        )


def _admin() -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="admin-1"),
        roles=frozenset({Role.ADMIN}),
    )


@pytest.mark.asyncio
class TestVerifier:
    async def test_clean_state_reports_zero_drift_and_writes_nothing(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed_all(pg_engine, pg_session)
        handler = VerifyTableStatisticsHandler(
            principal=_admin(),
            stats_store=PostgresStatisticsStore(pg_session),
        )
        report = await handler.run(VerifyTableStatistics(repair=False))
        assert report.drift == []
        assert report.repaired is False

    async def test_corrupted_count_is_reported_and_repaired_only_on_request(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed_all(pg_engine, pg_session)
        # Corrupt the records count behind the system's back.
        await pg_session.execute(
            table_statistics_table.update()
            .where(table_statistics_table.c.table_name == "records")
            .values(row_count=999)
        )
        await pg_session.commit()

        handler = VerifyTableStatisticsHandler(
            principal=_admin(),
            stats_store=PostgresStatisticsStore(pg_session),
        )
        report = await handler.run(VerifyTableStatistics(repair=False))
        assert len(report.drift) == 1
        entry = report.drift[0]
        assert entry.table_name == "records"
        assert entry.stored is not None and entry.stored.row_count == 999
        assert entry.actual is not None and entry.actual.row_count == 3
        assert report.repaired is False

        # Still drifted (report-only made no writes) — now repair.
        report2 = await handler.run(VerifyTableStatistics(repair=True))
        assert len(report2.drift) == 1 and report2.repaired is True
        await pg_session.commit()

        report3 = await handler.run(VerifyTableStatistics(repair=False))
        assert report3.drift == []
