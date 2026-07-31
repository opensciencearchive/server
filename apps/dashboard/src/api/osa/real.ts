/**
 * RealOSAService — reads the local archive through the same-origin BFF proxy
 * (`/api/osa/*`), which attaches the server-held SUPERADMIN token (issue #173).
 * Every method hits a real archive endpoint; there is no sample data here.
 * Runs client-side, so a relative `baseUrl` resolves to the dashboard origin and
 * rides the httpOnly session cookie.
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
import {
  decodeAuthConfig,
  decodeCatalogSchemaIds,
  decodeFeatureTables,
  decodeHookCatalog,
  decodeIngesterCatalog,
  decodeIngestionRuns,
  decodeObservability,
  decodeRecordStats,
  decodeRecordTypeCount,
  decodeRecordsPage,
} from "./wire/decode";

export class RealOSAService implements OSAService {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  /** Fetch JSON through the BFF read proxy (`/api/osa/<root>/...`). */
  private async getJson(path: string): Promise<unknown> {
    return this.fetchJson(`${this.baseUrl}${path}`);
  }

  private async fetchJson(url: string): Promise<unknown> {
    const res = await fetch(url, { headers: { accept: "application/json" } });
    if (!res.ok) {
      throw new Error(`OSA request failed (${res.status}) for ${url}`);
    }
    return res.json();
  }

  async getRecordStats(archiveId: string): Promise<RecordStats> {
    void archiveId;
    return decodeRecordStats(await this.getJson("/stats"));
  }

  /** Catalog → per-schema manifest row counts (one row per published schema). */
  async getRecordTypeBreakdown(archiveId: string): Promise<RecordTypeCount[]> {
    void archiveId;
    const ids = decodeCatalogSchemaIds(await this.getJson("/data"));
    const counts = await Promise.all(
      ids.map((id) =>
        this.getJson(`/data/${encodeURIComponent(id)}`)
          .then(decodeRecordTypeCount)
          .catch(() => null),
      ),
    );
    return counts.filter((c): c is RecordTypeCount => c !== null);
  }

  async listSchemas(archiveId: string): Promise<string[]> {
    void archiveId;
    return decodeCatalogSchemaIds(await this.getJson("/data"));
  }

  async listRecords(archiveId: string, schema: string): Promise<TenantRecord[]> {
    void archiveId;
    const json = await this.getJson(`/data/${encodeURIComponent(schema)}/records`);
    return decodeRecordsPage(json, schema);
  }

  /** Catalog → per-schema manifests → the feature tables across all schemas. */
  async listFeatureTables(archiveId: string): Promise<FeatureTable[]> {
    void archiveId;
    const ids = decodeCatalogSchemaIds(await this.getJson("/data"));
    const perSchema = await Promise.all(
      ids.map((id) =>
        this.getJson(`/data/${encodeURIComponent(id)}`)
          .then((m) => decodeFeatureTables(m, id))
          .catch(() => [] as FeatureTable[]),
      ),
    );
    return perSchema.flat();
  }

  async listHooks(archiveId: string): Promise<TenantHook[]> {
    void archiveId;
    return decodeHookCatalog(await this.getJson("/hooks"));
  }

  async listIngesters(archiveId: string): Promise<TenantIngester[]> {
    void archiveId;
    return decodeIngesterCatalog(await this.getJson("/ingesters"));
  }

  async listIngestionRuns(archiveId: string): Promise<IngestionRun[]> {
    void archiveId;
    return decodeIngestionRuns(await this.getJson("/ingestions"));
  }

  async getObservability(archiveId: string): Promise<ObservabilitySnapshot> {
    void archiveId;
    return decodeObservability(await this.getJson("/ready"));
  }

  async getAuthConfig(archiveId: string): Promise<TenantAuthView> {
    void archiveId;
    // Bespoke BFF route, not the generic proxy (keeps /auth/* off the proxy).
    return decodeAuthConfig(await this.fetchJson("/api/auth-config"));
  }
}
