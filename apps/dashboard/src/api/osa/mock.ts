/**
 * MockOSAService — the platform-build OSAService. Returns deterministic sample
 * data (the Alpine-climate-network story from the design mockup); panels surface a
 * `<SampleDataChip/>` beside it. Self-host uses `RealOSAService` instead, so this
 * sample data never appears when connected to a real archive.
 */
import type {
  FeatureTable,
  IngestionRun,
  ObservabilitySnapshot,
  RecordStats,
  RecordTypeCount,
  TenantAuthView,
  TenantHook,
  TenantIngester,
  TenantRecord,
} from "@/domain/tenant";

import type { OSAService } from "./service";

const SAMPLE_SCHEMAS = [
  "station-timeseries@2.1.0",
  "radiosonde-profile@1.3.0",
  "station-metadata@1.0.0",
];

export class MockOSAService implements OSAService {
  getRecordStats(archiveId: string): Promise<RecordStats> {
    void archiveId;
    return Promise.resolve({
      publishedRecords: 12_481,
      recordsThisMonth: 318,
      derivedFeaturesPerRecord: 3,
      storageBytes: 1.4e12,
    });
  }

  getRecordTypeBreakdown(archiveId: string): Promise<RecordTypeCount[]> {
    void archiveId;
    return Promise.resolve([
      { type: "Surface station timeseries", count: 7204 },
      { type: "Radiosonde profiles", count: 3118 },
      { type: "Snowpack sensors", count: 1502 },
      { type: "Sky imagery", count: 657 },
    ]);
  }

  listSchemas(archiveId: string): Promise<string[]> {
    void archiveId;
    return Promise.resolve(SAMPLE_SCHEMAS);
  }

  listRecords(archiveId: string, schema: string): Promise<TenantRecord[]> {
    void archiveId;
    return Promise.resolve([
      {
        id: "9f2c4a1e",
        schema,
        createdAt: new Date("2026-07-24T10:31:00Z"),
        fields: { title: "Col du Lac surface timeseries", station: "ST-1042", readings: 8420 },
      },
      {
        id: "1a2b3c4d",
        schema,
        createdAt: new Date("2026-07-23T15:02:00Z"),
        fields: { title: "Radiosonde ascent series", site: "SD-9", flight: 3 },
      },
      {
        id: "5e6f7a8b",
        schema,
        createdAt: new Date("2026-07-22T09:47:00Z"),
        fields: { title: "Snowpack sensor batch 12", protocol: "SR50A v3" },
      },
    ]);
  }

  listFeatureTables(archiveId: string): Promise<FeatureTable[]> {
    void archiveId;
    return Promise.resolve([
      { name: "qc-metrics", schema: "station-timeseries@2.1.0", columns: 6, rows: 7204 },
      { name: "hourly-aggregates", schema: "station-timeseries@2.1.0", columns: 4, rows: 214_880 },
      { name: "ascent-profile", schema: "radiosonde-profile@1.3.0", columns: 3, rows: 3118 },
    ]);
  }

  listHooks(archiveId: string): Promise<TenantHook[]> {
    void archiveId;
    return Promise.resolve([
      {
        name: "validate-metadata",
        liveVersion: "build_8f3a91c2e5",
        description: "Rejects depositions with incomplete station metadata.",
        lastRunAt: new Date("2026-07-25T16:20:00Z"),
      },
      {
        name: "resolve-ontology",
        liveVersion: "build_8f3a91c2e5",
        description: "Resolves instrument and variable terms against CF standard names.",
        lastRunAt: new Date("2026-07-25T16:21:30Z"),
      },
    ]);
  }

  listIngesters(archiveId: string): Promise<TenantIngester[]> {
    void archiveId;
    return Promise.resolve([
      {
        name: "GHCN ingester",
        schema: "station-timeseries@2.1.0",
        description: "Imports GHCN station series and normalises them into the convention.",
        image: "ghcr.io/summit-lab/ghcn-ingester",
        digest: "sha256:8f3a91c2e5b4",
        schedule: "0 3 * * *",
      },
    ]);
  }

  listIngestionRuns(archiveId: string): Promise<IngestionRun[]> {
    void archiveId;
    return Promise.resolve([
      {
        id: "ing_7f3a91c2e5",
        convention: "station-timeseries@2.1.0",
        status: "running",
        ingestionFinished: false,
        batchesIngested: 12,
        batchesCompleted: 9,
        batchesFailed: 0,
        publishedCount: 8_940,
        startedAt: new Date("2026-07-27T09:15:00Z"),
        completedAt: null,
        failureReason: null,
      },
      {
        id: "ing_4dde94d9aa",
        convention: "station-timeseries@2.1.0",
        status: "completed",
        ingestionFinished: true,
        batchesIngested: 20,
        batchesCompleted: 20,
        batchesFailed: 0,
        publishedCount: 19_204,
        startedAt: new Date("2026-07-25T22:40:00Z"),
        completedAt: new Date("2026-07-25T23:12:00Z"),
        failureReason: null,
      },
      {
        id: "ing_1a2b3c4d5e",
        convention: "station-timeseries@2.1.0",
        status: "failed",
        ingestionFinished: false,
        batchesIngested: 3,
        batchesCompleted: 2,
        batchesFailed: 1,
        publishedCount: 1_920,
        startedAt: new Date("2026-07-24T11:02:00Z"),
        completedAt: new Date("2026-07-24T11:09:00Z"),
        failureReason: "hook runner out of memory on batch 3 after 2 retries",
      },
    ]);
  }

  getObservability(archiveId: string): Promise<ObservabilitySnapshot> {
    void archiveId;
    return Promise.resolve({
      status: "ready",
      components: [
        { name: "db", status: "healthy", detail: "connections 12/100" },
        { name: "workers", status: "healthy", detail: "queue depth 0" },
        { name: "runner", status: "healthy", detail: "not checked" },
      ],
    });
  }

  getAuthConfig(archiveId: string): Promise<TenantAuthView> {
    void archiveId;
    return Promise.resolve({
      provider: "orcid",
      clientId: "APP-K91F2LQ8XZ40MNRT",
      adminOrcidIds: ["0000-0002-1825-0097", "0000-0001-5109-3700"],
    });
  }
}
