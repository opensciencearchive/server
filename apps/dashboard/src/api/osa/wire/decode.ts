/**
 * Anti-corruption layer: OSA archive wire JSON → tenant domain types.
 * Parse-don't-validate; nothing outside `api/` ever sees a wire shape.
 */
import type { z } from "zod";

import type { AgentSurface } from "@/domain/agent";
import type { NodeOverview } from "@/domain/node";
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

import {
  wireAgentSurface,
  wireAuthConfig,
  wireHookCatalog,
  type wireHookCatalogItem,
  wireIngesterCatalog,
  wireIngestionRunList,
  wireNodeCatalog,
  wireNodeOverview,
  wireReady,
  wireRecordsPage,
  wireSchemaManifest,
  wireStats,
} from "./schemas";

/** Synthesize a human description from a hook's fixed feature-table contract. */
function describeFeature(
  hookName: string,
  feature: z.infer<typeof wireHookCatalogItem>["feature"],
): string {
  const columns = feature.columns?.length ?? 0;
  const cols = columns === 0 ? "" : ` (${columns} column${columns === 1 ? "" : "s"})`;
  return `Produces the "${hookName}" feature table${cols}.`;
}

/** GET /hooks → the hook catalog with each hook's current live release. */
export function decodeHookCatalog(json: unknown): TenantHook[] {
  const { items } = wireHookCatalog.parse(json);
  return items.map((h) => ({
    name: h.name,
    liveVersion: h.live_release === null ? "—" : `v${h.live_release.version}`,
    description: describeFeature(h.name, h.feature),
    // The public catalog exposes the live release's build time, not per-run
    // times (those need the ADMIN hook-runs endpoint) — closest available.
    lastRunAt: h.live_release === null ? null : new Date(h.live_release.built_at),
  }));
}

/** The `/mcp` connector URL, at the node's public origin (from skill_url). */
function mcpUrlFrom(skillUrl: string): string {
  try {
    return `${new URL(skillUrl).origin}/mcp`;
  } catch {
    return "/mcp";
  }
}

/** `agent/skill` + `agent/discovery` → the archive's agent-grounding surface. */
export function decodeAgentSurface(json: unknown): AgentSurface {
  const { skill, discovery } = wireAgentSurface.parse(json);
  return {
    skillMarkdown: skill,
    node: {
      name: discovery.node.name,
      domain: discovery.node.domain,
      description: discovery.node.description,
      osaVersion: discovery.node.osa_version,
    },
    mcpUrl: mcpUrlFrom(discovery.skill_url),
    skillUrl: discovery.skill_url,
    dataUrl: discovery.data_url,
  };
}

/** GET /api/node (BFF) → the self-host node overview. */
export function decodeNodeOverview(json: unknown): NodeOverview {
  return wireNodeOverview.parse(json);
}

/** GET /api/v1/data → the published schemas' short ids (for manifest lookups). */
export function decodeCatalogSchemaIds(json: unknown): string[] {
  return wireNodeCatalog.parse(json).schemas.map((s) => s.id);
}

/**
 * GET /api/v1/data/{schema} → one "by record type" row: the schema's title and
 * its records-table row count (0 if the records table is absent).
 */
export function decodeRecordTypeCount(json: unknown): RecordTypeCount {
  const manifest = wireSchemaManifest.parse(json);
  const records = manifest.table_resources.find((t) => t.name === "records");
  return { type: manifest.title, count: records?.row_count ?? 0 };
}

/** GET /api/v1/data/{schema} → the schema's feature tables (kind=feature). */
export function decodeFeatureTables(json: unknown, schema: string): FeatureTable[] {
  const manifest = wireSchemaManifest.parse(json);
  return manifest.table_resources
    .filter((t) => t.kind === "feature")
    .map((t) => ({
      name: t.name,
      schema,
      columns: t.columns?.length ?? 0,
      rows: t.row_count,
    }));
}

/** GET /api/v1/stats → the record-stats tiles. */
export function decodeRecordStats(json: unknown): RecordStats {
  const s = wireStats.parse(json);
  return {
    publishedRecords: s.records,
    recordsThisMonth: s.records_this_month,
    derivedFeaturesPerRecord: s.features_per_record,
    storageBytes: s.storage_bytes,
  };
}

// Implicit record-table columns present on every row — excluded from `fields`.
const IMPLICIT_RECORD_COLUMNS = new Set([
  "id",
  "srn",
  "schema_id",
  "version",
  "created_at",
]);

/** GET /api/v1/data/{schema}/records → published record rows for one schema. */
export function decodeRecordsPage(json: unknown, schema: string): TenantRecord[] {
  const { rows } = wireRecordsPage.parse(json);
  return rows.map((row) => {
    const fields: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (!IMPLICIT_RECORD_COLUMNS.has(key)) fields[key] = value;
    }
    return {
      id: String(row.id ?? row.srn ?? ""),
      schema,
      createdAt: new Date(String(row.created_at ?? "")),
      fields,
    };
  });
}

/** GET /api/v1/ingesters → the ingester catalog. */
export function decodeIngesterCatalog(json: unknown): TenantIngester[] {
  const { items } = wireIngesterCatalog.parse(json);
  return items.map((i) => ({
    name: i.title || i.name,
    schema: i.schema_id,
    description: i.description,
    image: i.image,
    digest: i.digest,
    schedule: i.schedule,
  }));
}

/** GET /api/v1/ingestions → the ingest-run list (pending/running are live). */
export function decodeIngestionRuns(json: unknown): IngestionRun[] {
  const { items } = wireIngestionRunList.parse(json);
  return items.map((r) => ({
    id: r.id,
    convention: r.convention_id,
    status: r.status,
    ingestionFinished: r.ingestion_finished,
    batchesIngested: r.batches_ingested,
    batchesCompleted: r.batches_completed,
    batchesFailed: r.batches_failed,
    publishedCount: r.published_count,
    startedAt: new Date(r.started_at),
    completedAt: r.completed_at ? new Date(r.completed_at) : null,
    failureReason: r.failure_reason,
  }));
}

/** GET /api/v1/ready → the observability snapshot (ok/unchecked → healthy). */
export function decodeObservability(json: unknown): ObservabilitySnapshot {
  const ready = wireReady.parse(json);
  return {
    status: ready.status,
    components: Object.entries(ready.components).map(([name, c]) => ({
      name,
      status: c.status === "error" ? "degraded" : "healthy",
      detail: c.detail ?? (c.status === "unchecked" ? "not checked" : ""),
    })),
  };
}

/** GET /api/auth-config (BFF) → the archive sign-in configuration. */
export function decodeAuthConfig(json: unknown): TenantAuthView {
  const a = wireAuthConfig.parse(json);
  return {
    provider: "orcid",
    clientId: a.client_id,
    adminOrcidIds: a.admin_orcids,
  };
}
