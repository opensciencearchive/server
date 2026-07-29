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
  feature: z.object({
    name: z.string(),
    columns: z.array(z.object({ name: z.string() })).nullish(),
  }),
  live_release: wireLiveRelease.nullable(),
});

export const wireHookCatalog = z.object({
  items: z.array(wireHookCatalogItem),
});

// GET /api/agent (BFF) — SKILL.md text + root discovery (skill.py:RootDiscovery).
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
