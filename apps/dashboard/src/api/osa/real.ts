/**
 * RealOSAService — reads a real archive's OSA API through a same-origin BFF
 * proxy. Self-host and platform are the SAME service, differing only in the
 * per-archive base URL:
 *
 * - **self-host** (#173): the single local archive via `/api/osa/*` (the
 *   `archiveId` is ignored — there is only one archive).
 * - **platform** (#185): a tenant archive via the control-plane read-proxy at
 *   `/api/amacrin/api/v1/archives/{id}/osa/*`, which mints a scoped, per-node
 *   token server-side (the browser never holds a tenant credential).
 *
 * Every read — including the non-secret sign-in config at `/auth/config` — goes
 * through the same proxy path; the only difference between the two builds is the
 * base prefix. There is no sample data here. Runs client-side, so a relative
 * base resolves to the dashboard origin and rides the httpOnly session cookie.
 */
import type { AgentSurface } from "@/domain/agent";
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
  decodeAgentSurface,
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

export class RealOSAService implements OSAService {
  private readonly resolveBase: ResolveBase;

  constructor(resolveBase: ResolveBase) {
    this.resolveBase = resolveBase;
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

  /** Same contract as `fetchJson`, for the markdown grounding doc. */
  private async fetchText(url: string): Promise<string> {
    const res = await fetch(url, { headers: { accept: "text/markdown" } });
    if (!res.ok) {
      throw new Error(`OSA request failed (${res.status}) for ${url}`);
    }
    return res.text();
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
    // Non-secret sign-in config through the same read proxy as every other
    // surface — self-host and platform both allowlist exactly `auth/config`
    // (#184/#185).
    return decodeAuthConfig(await this.getJson(archiveId, "/auth/config"));
  }

  async getAgentSurface(archiveId: string): Promise<AgentSurface> {
    // `SKILL.md` and root discovery are public and unversioned, so they sit
    // outside `/api/v1` — both proxies expose them as the `agent/*` aliases.
    // The two reads are independent; fetch them together.
    const base = this.resolveBase(archiveId).replace(/\/$/, "");
    const [skill, discovery] = await Promise.all([
      this.fetchText(`${base}/agent/skill`),
      this.fetchJson(`${base}/agent/discovery`),
    ]);
    return decodeAgentSurface({ skill, discovery });
  }
}
