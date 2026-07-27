/**
 * OSAService — data living INSIDE a tenant OSA instance (records,
 * validation, search, hooks, usage).
 *
 * The open-source OSA server exposes endpoints for much of this
 * (`/api/v1/stats`, schema manifests, `/hooks`, validation runs), but the
 * dashboard has NO authenticated path to tenant instances today — so every
 * method returns `Mocked<T>` and the only implementation is
 * `MockOSAService`. When a dashboard→tenant auth path ships, add
 * `RealOSAService`, delete the `Mocked<…>` wrappers method by method, and
 * fix the resulting compile errors — that is the whole migration.
 */
import type { Mocked } from "@/domain/mocked";
import type {
  DepositionPoint,
  ObservabilitySnapshot,
  RecordStats,
  RecordTypeCount,
  SearchOverview,
  TenantAuthView,
  TenantHook,
  TenantIngester,
  TenantRecord,
  UsageStats,
  ValidationSummary,
} from "@/domain/tenant";

export interface OSAService {
  /** @mock OSA has GET /stats + schema manifests; no tenant auth path yet. */
  getRecordStats(archiveId: string): Promise<Mocked<RecordStats>>;
  /** @mock Derivable from OSA depositions/events; no aggregate endpoint. */
  getDepositionSeries(archiveId: string): Promise<Mocked<DepositionPoint[]>>;
  /** @mock Derivable from OSA schema manifests. */
  getRecordTypeBreakdown(
    archiveId: string,
  ): Promise<Mocked<RecordTypeCount[]>>;
  /** @mock OSA validation runs are per-run; no check×pass/fail aggregate. */
  getValidationSummary(archiveId: string): Promise<Mocked<ValidationSummary>>;
  /** @mock No analytics endpoints on OSA (only Prometheus /metrics). */
  getUsageStats(archiveId: string): Promise<Mocked<UsageStats>>;
  /** @mock OSA has GET /data/{schema}/records; no tenant auth path yet. */
  listRecords(archiveId: string): Promise<Mocked<TenantRecord[]>>;
  /** REAL in self-host via GET /hooks (through the BFF proxy). */
  listHooks(archiveId: string): Promise<TenantHook[]>;
  /** @mock OSA models ingesters as components; no tenant auth path yet. */
  listIngesters(archiveId: string): Promise<Mocked<TenantIngester[]>>;
  /** @mock No search-index endpoint. */
  getSearchOverview(archiveId: string): Promise<Mocked<SearchOverview>>;
  /** @mock OSA /ready + /metrics exist; no tenant auth path yet. */
  getObservability(archiveId: string): Promise<Mocked<ObservabilitySnapshot>>;
  /** @mock The cloud API exposes only the admins list on the archive. */
  getAuthConfig(archiveId: string): Promise<Mocked<TenantAuthView>>;
}
