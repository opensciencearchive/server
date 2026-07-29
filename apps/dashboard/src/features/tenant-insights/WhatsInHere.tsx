"use client";

import Link from "next/link";

import { SampleDataChip, Skeleton, Stat } from "@/ui";

import { useRecordStats, useRecordTypeBreakdown } from "./hooks";
import styles from "./tenant-insights.module.css";

function formatBytes(bytes: number): string {
  const tb = bytes / 1e12;
  if (tb >= 1) return `${tb.toFixed(1)} TB`;
  const gb = bytes / 1e9;
  if (gb >= 1) return `${Math.round(gb)} GB`;
  return `${Math.round(bytes / 1e6)} MB`;
}

export function WhatsInHere({ archiveId }: { archiveId: string }) {
  const stats = useRecordStats(archiveId);
  const types = useRecordTypeBreakdown(archiveId);

  const typeMax = Math.max(1, ...(types.data?.data.map((t) => t.count) ?? []));

  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <div className={styles.headLeft}>
          <h3>What&apos;s in here</h3>
          <SampleDataChip />
        </div>
        <Link href="./records" className={styles.headLink}>
          Browse records →
        </Link>
      </div>

      <div className={styles.whatsGrid}>
        <div className={styles.panelCard}>
          {stats.isPending ? (
            <Skeleton height="6rem" width="100%" />
          ) : stats.data ? (
            <div className={styles.statRow}>
              <Stat
                label="Published records"
                value={stats.data.data.publishedRecords.toLocaleString("en-GB")}
              />
              <Stat
                label="Derived features"
                value={stats.data.data.derivedFeaturesPerRecord}
                note="per record"
              />
              <Stat
                label="Stored"
                value={formatBytes(stats.data.data.storageBytes)}
                note="objects + index"
              />
            </div>
          ) : null}
        </div>

        <div className={styles.panelCard}>
          <span className={styles.panelLabel}>By record type</span>
          {types.isPending ? (
            <Skeleton height="8rem" width="100%" />
          ) : (
            <div className={styles.typeList}>
              {types.data?.data.map((t) => (
                <div key={t.type} className={styles.typeRow}>
                  <div className={styles.typeHead}>
                    <span>{t.type}</span>
                    <span className={styles.typeCount}>
                      {t.count.toLocaleString("en-GB")}
                    </span>
                  </div>
                  <span className={styles.typeTrack}>
                    <span
                      className={styles.typeFill}
                      style={{ width: `${Math.round((t.count / typeMax) * 100)}%` }}
                    />
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
