"use client";

import { CopyButton, EmptyState, PageHeader, Skeleton } from "@/ui";
import { icons } from "@/features/shell/icons";

import { ConnectPanel } from "./ConnectPanel";
import { SkillSheet } from "./SkillSheet";
import { useAgentSurface } from "./useAgentSurface";
import styles from "./agent.module.css";

/** Strip the scheme so a URL reads as a bare host + path (e.g. in a card). */
function bareUrl(url: string): string {
  return url.replace(/^https?:\/\//, "");
}

export function AgentPanel() {
  const surface = useAgentSurface();

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.agents}
        title="Agents"
        description="This archive publishes itself to language models — a skill file describing what's in here and when to use it, and an MCP server exposing the query tools. Both are generated from the archive's published data."
      />

      {surface.isPending ? (
        <Skeleton height="24rem" width="100%" />
      ) : surface.isError || surface.data === undefined ? (
        <EmptyState
          icon={icons.agents}
          title="Agent surface unavailable"
          description="The archive's SKILL.md and MCP connector appear here once the dashboard can reach the archive."
        />
      ) : (
        <>
          <div className={styles.summaryGrid}>
            <SummaryCard
              label="Skill file"
              status="Published"
              url={surface.data.skillUrl}
              description="Drop-in for Claude Skills and agent frameworks. Generated from the archive's schemas and their descriptions."
            />
            <SummaryCard
              label="MCP server"
              status="Live"
              url={surface.data.mcpUrl}
              description="Streamable HTTP transport. Read tools need no auth; deposition tools require an ORCID token."
            />
          </div>

          <div className={styles.mainGrid}>
            <SkillSheet markdown={surface.data.skillMarkdown} />
            <ConnectPanel mcpUrl={surface.data.mcpUrl} />
          </div>
        </>
      )}
    </div>
  );
}

function SummaryCard({
  label,
  status,
  url,
  description,
}: {
  label: string;
  status: string;
  url: string;
  description: string;
}) {
  return (
    <div className={styles.summaryCard}>
      <div className={styles.summaryTop}>
        <span className={styles.summaryLabel}>{label}</span>
        <span className={styles.summaryStatus}>
          <span className={styles.statusDot} />
          {status}
        </span>
      </div>
      <div className={styles.summaryUrlRow}>
        <span className={styles.summaryUrl}>{bareUrl(url)}</span>
        <CopyButton value={url} size="sm" />
      </div>
      <p className={styles.summaryDesc}>{description}</p>
    </div>
  );
}
