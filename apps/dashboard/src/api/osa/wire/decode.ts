/**
 * Anti-corruption layer: OSA archive wire JSON → tenant domain types.
 * Parse-don't-validate; nothing outside `api/` ever sees a wire shape.
 */
import type { z } from "zod";

import type { TenantHook } from "@/domain/tenant";

import { wireHookCatalog, type wireHookCatalogItem } from "./schemas";

/** Synthesize a human description from a hook's fixed feature-table contract. */
function describeFeature(
  feature: z.infer<typeof wireHookCatalogItem>["feature"],
): string {
  const columns = feature.columns?.length ?? 0;
  const cols = columns === 0 ? "" : ` (${columns} column${columns === 1 ? "" : "s"})`;
  return `Produces the "${feature.name}" feature table${cols}.`;
}

/** GET /hooks → the hook catalog with each hook's current live release. */
export function decodeHookCatalog(json: unknown): TenantHook[] {
  const { items } = wireHookCatalog.parse(json);
  return items.map((h) => ({
    name: h.name,
    liveVersion: h.live_release === null ? "—" : `v${h.live_release.version}`,
    description: describeFeature(h.feature),
    // The public catalog exposes the live release's build time, not per-run
    // times (those need the ADMIN hook-runs endpoint) — closest available.
    lastRunAt: h.live_release === null ? null : new Date(h.live_release.built_at),
  }));
}
