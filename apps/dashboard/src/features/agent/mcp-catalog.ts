/**
 * The MCP Apps surface every OSA node exposes at `/mcp` (#162). A fixed property
 * of the server version, so it's described statically here rather than fetched
 * by live MCP introspection. Keep in sync with the server's `data` view tools.
 */

export interface McpEntry {
  name: string;
  description: string;
}

/** Model-visible tools that render data (delegate to the `data` view queries). */
export const MCP_TOOLS: readonly McpEntry[] = [
  { name: "list_datasets", description: "List the node's published datasets." },
  { name: "describe_dataset", description: "Fields, tables and row counts for a dataset." },
  { name: "show_table", description: "A paginated table view of records or features." },
  { name: "show_chart", description: "A chart over a dataset's columns." },
  { name: "show_record", description: "A single record with its metadata and features." },
  { name: "show_filter_panel", description: "An interactive filter builder for a dataset." },
];

/** Interactive UI widgets the tools render (`ui://osa/*`). */
export const MCP_APPS: readonly McpEntry[] = [
  { name: "table", description: "Sortable, paginated record/feature table." },
  { name: "chart", description: "Interactive chart of dataset columns." },
  { name: "record", description: "Single-record detail view." },
  { name: "filter-panel", description: "Build a filter, then fetch matching rows." },
  { name: "dataset-overview", description: "Dataset summary with tables and counts." },
];
