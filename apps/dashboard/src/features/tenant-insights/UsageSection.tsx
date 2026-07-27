"use client";

import { SampleDataChip, Skeleton, Stat } from "@/ui";

import { useUsageStats } from "./hooks";
import styles from "./tenant-insights.module.css";

export function UsageSection({ archiveId }: { archiveId: string }) {
  const usage = useUsageStats(archiveId);
  const stats = usage.data?.data;

  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <div className={styles.headLeft}>
          <h3>Who&apos;s using it</h3>
          <SampleDataChip />
        </div>
        <span className={styles.headMeta}>last 30 days</span>
      </div>

      {usage.isPending ? (
        <Skeleton height="7rem" width="100%" />
      ) : stats ? (
        <div className={styles.usageGrid}>
          <Stat
            label="Record downloads"
            value={stats.recordDownloads.toLocaleString("en-GB")}
            note={`${stats.uniqueClients.toLocaleString("en-GB")} unique clients`}
          />
          <Stat
            label="API and agent queries"
            value={stats.apiQueries.toLocaleString("en-GB")}
            note={`${Math.round(stats.agentQueryShare * 100)}% from agents`}
          />
          <Stat
            label="Bulk exports"
            value={stats.bulkExports.toLocaleString("en-GB")}
            note="each pinned to a digest"
          />
          <Stat
            label="Mirroring nodes"
            value={stats.mirroringNodes.toLocaleString("en-GB")}
            note="federated copies"
          />
        </div>
      ) : null}
    </section>
  );
}
