"use client";

import { EmptyState, PageHeader, Skeleton } from "@/ui";

import { McpConnector } from "./McpConnector";
import { SkillSheet } from "./SkillSheet";
import { useAgentSurface } from "./useAgentSurface";
import styles from "./agent.module.css";

export function AgentPanel() {
  const surface = useAgentSurface();

  return (
    <div className={styles.page}>
      <PageHeader
        title="Agents"
        description="How AI agents discover and query this archive — its skill sheet and Model Context Protocol connector."
      />

      {surface.isPending ? (
        <Skeleton height="20rem" width="100%" />
      ) : surface.isError || surface.data === undefined ? (
        <EmptyState
          title="Agent surface unavailable"
          description="The archive's SKILL.md and MCP connector appear here once the dashboard can reach the archive."
        />
      ) : (
        <>
          <section className={styles.section}>
            <div className={styles.sectionHead}>
              <h3>Skill sheet</h3>
            </div>
            <p className={styles.sectionSub}>
              Served at <code>/SKILL.md</code> — the grounding doc agents read to
              learn what this archive holds.
            </p>
            <SkillSheet markdown={surface.data.skillMarkdown} />
          </section>

          <McpConnector mcpUrl={surface.data.mcpUrl} />
        </>
      )}
    </div>
  );
}
