/**
 * Wire DTO schemas — verbatim mirrors of the OSA server's Pydantic models.
 * PRIVATE to `api/`: decoders (decode.ts) are the anti-corruption layer that
 * turns these into domain types; nothing outside `api/` sees a wire shape.
 */
import { z } from "zod";

// GET /api/v1/hooks — HookCatalog (domain/validation/query/list_hooks.py).
export const wireLiveRelease = z.object({
  version: z.number(),
  digest: z.string(),
  source_ref: z.string(),
  built_at: z.string(),
});

export const wireHookCatalogItem = z.object({
  name: z.string(),
  // TableFeatureSpec: {kind, cardinality, columns[]} — it has NO `name` (the
  // feature table is named after the hook). Only `columns` is read.
  feature: z.object({
    columns: z.array(z.object({ name: z.string() })).nullish(),
  }),
  live_release: wireLiveRelease.nullable(),
});

export const wireHookCatalog = z.object({
  items: z.array(wireHookCatalogItem),
});

// The agent surface: SKILL.md text + root discovery (skill.py:RootDiscovery),
// read through the proxy's `agent/skill` + `agent/discovery` aliases.
export const wireRootDiscovery = z.object({
  node: z.object({
    name: z.string(),
    domain: z.string(),
    description: z.string(),
    osa_version: z.string(),
  }),
  skill_url: z.string(),
  reference_base: z.string(),
  data_url: z.string(),
  openapi_url: z.string(),
});

export const wireAgentSurface = z.object({
  skill: z.string(),
  discovery: wireRootDiscovery,
});

// GET /api/v1/data — NodeCatalog (domain/data/model/catalog.py).
export const wireNodeCatalog = z.object({
  node_domain: z.string(),
  schemas: z.array(
    z.object({
      id: z.string(),
      version: z.string(),
    }),
  ),
});

// GET /api/v1/data/{schema} — SchemaManifest (domain/data/model/manifest.py).
// Only the fields the dashboard reads; unknown keys are ignored by default.
export const wireSchemaManifest = z.object({
  id: z.string(),
  version: z.string(),
  title: z.string(),
  table_resources: z.array(
    z.object({
      name: z.string(),
      kind: z.string(), // TableKind: "records" | "feature"
      row_count: z.number(),
      columns: z.array(z.object({ name: z.string() })).nullish(),
    }),
  ),
});

// GET /api/v1/stats — StatsResponse (routes/stats.py).
export const wireStats = z.object({
  records: z.number(),
  records_this_month: z.number(),
  storage_bytes: z.number(),
  features_per_record: z.number(),
});

// GET /api/v1/data/{schema}/records — JSON page (serializers/json.py).
// Rows are projected column→value objects; only `rows` is read.
export const wireRecordsPage = z.object({
  rows: z.array(z.record(z.string(), z.unknown())),
  next_cursor: z.string().nullable(),
  has_more: z.boolean(),
});

// GET /api/v1/ingesters — IngesterCatalog (query/list_ingesters.py).
export const wireIngesterCatalog = z.object({
  items: z.array(
    z.object({
      name: z.string(),
      title: z.string(),
      description: z.string(),
      schema_id: z.string(),
      image: z.string(),
      digest: z.string(),
      source_ref: z.string().nullable(),
      schedule: z.string().nullable(),
    }),
  ),
});

// GET /api/v1/ingestions — IngestRunList (query/list_ingestions.py). ADMIN only.
export const wireIngestionRunList = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      convention_id: z.string(),
      status: z.enum(["pending", "running", "completed", "failed"]),
      ingestion_finished: z.boolean(),
      batches_ingested: z.number(),
      batches_completed: z.number(),
      batches_failed: z.number(),
      published_count: z.number(),
      started_at: z.string(),
      completed_at: z.string().nullable(),
      failure_reason: z.string().nullable(),
    }),
  ),
});

// GET /api/v1/ready — ReadyResponse (routes/health.py).
export const wireReady = z.object({
  status: z.enum(["ready", "degraded"]),
  version: z.string(),
  components: z.record(
    z.string(),
    z.object({
      status: z.enum(["ok", "error", "unchecked"]),
      detail: z.string().nullish(),
    }),
  ),
});

// GET /api/auth-config (BFF → /api/v1/auth/config) — AuthConfigResult.
export const wireAuthConfig = z.object({
  provider: z.string(),
  client_id: z.string(),
  admin_orcids: z.array(z.string()),
});

// GET /api/node (BFF) — the assembled node overview for the self-host hero.
export const wireNodeOverview = z.object({
  name: z.string(),
  domain: z.string(),
  description: z.string(),
  osaVersion: z.string(),
  status: z.enum(["ready", "degraded", "unknown"]),
  records: z.number().nullable(),
  schemas: z.number(),
});
