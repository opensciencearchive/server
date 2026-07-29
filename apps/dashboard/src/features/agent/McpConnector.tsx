"use client";

import { CopyButton } from "@/ui";

import { MCP_APPS, MCP_TOOLS, type McpEntry } from "./mcp-catalog";
import styles from "./agent.module.css";

export function McpConnector({ mcpUrl }: { mcpUrl: string }) {
  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <h3>MCP connector</h3>
      </div>
      <p className={styles.sectionSub}>
        Add this archive to an MCP host (e.g. Claude) to query it with the tools
        and interactive apps below.
      </p>

      <div className={styles.connectorRow}>
        <span className={styles.url}>{mcpUrl}</span>
        <CopyButton value={mcpUrl} size="sm" />
      </div>
      <p className={styles.hint}>Streamable HTTP · public · read-only</p>

      <div className={styles.catalogGrid}>
        <Catalog label="Tools" entries={MCP_TOOLS} />
        <Catalog label="Interactive apps" entries={MCP_APPS} />
      </div>
    </section>
  );
}

function Catalog({
  label,
  entries,
}: {
  label: string;
  entries: readonly McpEntry[];
}) {
  return (
    <div>
      <p className={styles.catalogLabel}>{label}</p>
      {entries.map((entry) => (
        <div key={entry.name} className={styles.entry}>
          <span className={styles.entryName}>{entry.name}</span>
          <span className={styles.entryDesc}>{entry.description}</span>
        </div>
      ))}
    </div>
  );
}
