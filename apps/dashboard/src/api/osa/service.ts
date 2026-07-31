/**
 * OSAService — data living INSIDE a tenant OSA instance (records, features,
 * hooks, ingesters, observability, sign-in).
 *
 * Two implementations: `RealOSAService` (self-host — reads the archive's real
 * endpoints through the BFF proxy) and `MockOSAService` (platform — sample data,
 * surfaced with a `<SampleDataChip/>` until the control-plane read path ships).
 * Both return the same plain domain types; a panel shows the sample-data
 * affordance based on `useServices().isPlatform`, not on the data.
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

export interface OSAService {
  /** GET /stats — record counts + the materialized storage/feature snapshot. */
  getRecordStats(archiveId: string): Promise<RecordStats>;
  /** GET /data catalog + per-schema manifests — records by schema. */
  getRecordTypeBreakdown(archiveId: string): Promise<RecordTypeCount[]>;
  /** GET /data catalog — the published schema ids (records browser selector). */
  listSchemas(archiveId: string): Promise<string[]>;
  /** GET /data/{schema}/records — published records for one schema. */
  listRecords(archiveId: string, schema: string): Promise<TenantRecord[]>;
  /** GET /data + manifests — the registered derived-feature tables. */
  listFeatureTables(archiveId: string): Promise<FeatureTable[]>;
  /** GET /hooks — the hook catalog with live versions. */
  listHooks(archiveId: string): Promise<TenantHook[]>;
  /** GET /ingesters — the ingester catalog (one per convention with a source). */
  listIngesters(archiveId: string): Promise<TenantIngester[]>;
  /** GET /ingestions — recent ingest runs, including in-progress. ADMIN only. */
  listIngestionRuns(archiveId: string): Promise<IngestionRun[]>;
  /** GET /ready — component health snapshot. */
  getObservability(archiveId: string): Promise<ObservabilitySnapshot>;
  /** GET /auth/config (BFF) — provider, ORCID client id, admin list. */
  getAuthConfig(archiveId: string): Promise<TenantAuthView>;
}
