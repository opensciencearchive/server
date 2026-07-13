/** Builders for server tool-call arguments (app-side only). */

import type { SortSpec, TableQuery } from "./types";

/**
 * Arguments for the `fetch_page` tool. Optional keys are omitted (not null)
 * to mirror the server's exclude_none serialization convention.
 */
export function fetchPageArgs(
  query: TableQuery,
  cursor?: string,
  sortOverride?: SortSpec[],
): Record<string, unknown> {
  const args: Record<string, unknown> = {
    schema: query.schema,
    table: query.table,
    sort: sortOverride ?? query.sort,
    limit: query.limit,
  };
  if (query.filter !== undefined) args["filter"] = query.filter;
  if (cursor !== undefined) args["cursor"] = cursor;
  return args;
}

/** Arguments for the `sample_values` tool (bare column name, not a dotted path). */
export function sampleValuesArgs(
  schema: string,
  table: string,
  column: string,
): Record<string, unknown> {
  return { schema, table, column };
}
