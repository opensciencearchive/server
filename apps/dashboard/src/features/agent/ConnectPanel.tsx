"use client";

import { CopyButton } from "@/ui";

import { MCP_TOOLS } from "./mcp-catalog";
import styles from "./agent.module.css";

/** A ready-to-paste MCP client config (`mcpServers` block) for this archive. */
function mcpClientConfig(mcpUrl: string): string {
  let key = "osa";
  try {
    key = new URL(mcpUrl).hostname.split(".")[0] || "osa";
  } catch {
    // Non-absolute URL — fall back to the generic key.
  }
  return JSON.stringify(
    { mcpServers: { [key]: { type: "http", url: mcpUrl } } },
    null,
    2,
  );
}

/**
 * The right-hand "Connect" column: a copy-paste MCP client config, an auth note,
 * and the fixed catalogue of tools the archive's `/mcp` server exposes.
 */
export function ConnectPanel({ mcpUrl }: { mcpUrl: string }) {
  const config = mcpClientConfig(mcpUrl);

  return (
    <div className={styles.connectCol}>
      <section>
        <div className={styles.sectionHead}>
          <h3>Connect</h3>
        </div>
        <div className={styles.configCard}>
          <div className={styles.configTop}>
            <span>MCP client config</span>
            <CopyButton value={config} label="Copy" size="sm" />
          </div>
          <pre className={styles.configCode}>
            <code>{config}</code>
          </pre>
          <p className={styles.configNote}>
            <span className={styles.noteLabel}>Note</span>
            <span>
              Read tools are public. Add <code>Authorization: Bearer</code> with
              an ORCID token to deposit.
            </span>
          </p>
        </div>
      </section>

      <section>
        <div className={styles.toolsHead}>
          <h3>Tools</h3>
          <span className={styles.toolsCount}>{MCP_TOOLS.length} exposed</span>
        </div>
        <div className={styles.toolsList}>
          {MCP_TOOLS.map((tool) => (
            <div key={tool.name} className={styles.tool}>
              <span className={styles.toolName}>{tool.name}</span>
              <span className={styles.toolDesc}>{tool.description}</span>
            </div>
          ))}
        </div>
        <p className={styles.generatedNote}>
          <span className={styles.noteLabel}>Generated</span>
          <span>
            Tool names and descriptions are a fixed property of the archive&apos;s
            MCP surface.
          </span>
        </p>
      </section>
    </div>
  );
}
