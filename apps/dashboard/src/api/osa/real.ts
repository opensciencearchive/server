/**
 * RealOSAService — reads a real archive's OSA API through a same-origin BFF
 * proxy. The base is resolved per archive so one service serves both shapes:
 *
 * - **self-host** (#173): the single local archive via `/api/osa/*` (the
 *   `archiveId` is ignored — there is only one archive).
 * - **platform** (#185): a tenant archive via the control-plane read-proxy at
 *   `/api/amacrin/api/v1/archives/{id}/osa/*`, which mints a scoped, per-node
 *   token server-side (the browser never holds a tenant credential).
 *
 * Every method hits a real endpoint; there is no sample data here. Runs
 * client-side, so a relative base resolves to the dashboard origin and rides the
 * httpOnly session cookie.
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

/** Resolves the read-proxy base for a given archive. */
export type ResolveBase = (archiveId: string) => string;

export interface RealOSAServiceOptions {
  /**
   * Auth-config source. The tenant read-proxy has no `auth` surface, so the
   * platform build supplies this (sample data) rather than a live read; self-host
   * omits it and reads the local archive via the bespoke `/api/auth-config` route.
   */
  getAuthConfig?: (archiveId: string) => Promise<TenantAuthView>;
}

export class RealOSAService implements OSAService {
  private readonly resolveBase: ResolveBase;
  private readonly authConfigOverride:
    | ((archiveId: string) => Promise<TenantAuthView>)
    | undefined;

  constructor(resolveBase: ResolveBase, options?: RealOSAServiceOptions) {
    this.resolveBase = resolveBase;
    this.authConfigOverride = options?.getAuthConfig;
  }

  /** Fetch JSON from the archive's read-proxy base (`<base>/<path>`). */
  private async getJson(archiveId: string, path: string): Promise<unknown> {
    const base = this.resolveBase(archiveId).replace(/\/$/, "");
    return this.fetchJson(`${base}${path}`);
  }

  private async fetchJson(url: string): Promise<unknown> {
    const res = await fetch(url, { headers: { accept: "application/json" } });
    if (!res.ok) {
      throw new Error(`OSA request failed (${res.status}) for ${url}`);
    }
    return res.json();
  }

  async getRecordStats(archiveId: string): Promise<RecordStats> {
    return decodeRecordStats(await this.getJson(archiveId, "/stats"));
  }

  /** Catalog → per-schema manifest row counts (one row per published schema). */
  async getRecordTypeBreakdown(archiveId: string): Promise<RecordTypeCount[]> {
    const ids = decodeCatalogSchemaIds(await this.getJson(archiveId, "/data"));
    const counts = await Promise.all(
      ids.map((id) =>
        this.getJson(archiveId, `/data/${encodeURIComponent(id)}`)
          .then(decodeRecordTypeCount)
          .catch(() => null),
      ),
    );
    return counts.filter((c): c is RecordTypeCount => c !== null);
  }

  async listSchemas(archiveId: string): Promise<string[]> {
    return decodeCatalogSchemaIds(await this.getJson(archiveId, "/data"));
  }

  async listRecords(archiveId: string, schema: string): Promise<TenantRecord[]> {
    const json = await this.getJson(
      archiveId,
      `/data/${encodeURIComponent(schema)}/records`,
    );
    return decodeRecordsPage(json, schema);
  }

  /** Catalog → per-schema manifests → the feature tables across all schemas. */
  async listFeatureTables(archiveId: string): Promise<FeatureTable[]> {
    const ids = decodeCatalogSchemaIds(await this.getJson(archiveId, "/data"));
    const perSchema = await Promise.all(
      ids.map((id) =>
        this.getJson(archiveId, `/data/${encodeURIComponent(id)}`)
          .then((m) => decodeFeatureTables(m, id))
          .catch(() => [] as FeatureTable[]),
      ),
    );
    return perSchema.flat();
  }

  async listHooks(archiveId: string): Promise<TenantHook[]> {
    return decodeHookCatalog(await this.getJson(archiveId, "/hooks"));
  }

  async listIngesters(archiveId: string): Promise<TenantIngester[]> {
    return decodeIngesterCatalog(await this.getJson(archiveId, "/ingesters"));
  }

  async listIngestionRuns(archiveId: string): Promise<IngestionRun[]> {
    return decodeIngestionRuns(await this.getJson(archiveId, "/ingestions"));
  }

  async getObservability(archiveId: string): Promise<ObservabilitySnapshot> {
    return decodeObservability(await this.getJson(archiveId, "/ready"));
  }

  async getAuthConfig(archiveId: string): Promise<TenantAuthView> {
    if (this.authConfigOverride !== undefined) {
      return this.authConfigOverride(archiveId);
    }
    // Self-host bespoke BFF route, not the generic proxy (keeps /auth/* off it).
    return decodeAuthConfig(await this.fetchJson("/api/auth-config"));
  }
}
