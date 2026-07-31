"use client";

import Link from "next/link";

import { EmptyState, SampleDataChip, Skeleton, Stat } from "@/ui";
import { useServices } from "@/api/services";
import { icons } from "@/features/shell/icons";

import { useRecordStats, useRecordTypeBreakdown } from "./hooks";
import styles from "./tenant-insights.module.css";

function formatBytes(bytes: number): string {
  const tb = bytes / 1e12;
  if (tb >= 1) return `${tb.toFixed(1)} TB`;
  const gb = bytes / 1e9;
  if (gb >= 1) return `${Math.round(gb)} GB`;
  return `${Math.round(bytes / 1e6)} MB`;
}

function formatRatio(n: number): string {
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

export function WhatsInHere({ archiveId }: { archiveId: string }) {
  // Self-host reads real stats; platform's are sample data (chip).
  const isSample = useServices().tenantDataIsSample;
  const stats = useRecordStats(archiveId);
  const types = useRecordTypeBreakdown(archiveId);

  const breakdown = types.data ?? [];
  const typeMax = Math.max(1, ...breakdown.map((t) => t.count));

  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <div className={styles.headLeft}>
          <span className={styles.sectionIcon}>{icons.records}</span>
          <h3>What&apos;s in here</h3>
        </div>
        <Link href={`/archives/${archiveId}/records`} className={styles.headLink}>
          Browse records →
        </Link>
      </div>

      <div className={styles.whatsGrid}>
        <div className={styles.panelCard}>
          <div className={styles.headLeft}>
            <span className={styles.panelLabel}>Overview</span>
            {isSample && <SampleDataChip />}
          </div>
          {stats.isPending ? (
            <Skeleton height="6rem" width="100%" />
          ) : stats.data ? (
            <div className={styles.statRow}>
              <Stat
                label="Published records"
                value={stats.data.publishedRecords.toLocaleString("en-GB")}
              />
              <Stat
                label="Derived features"
                value={formatRatio(stats.data.derivedFeaturesPerRecord)}
                note="per record"
              />
              <Stat
                label="Stored"
                value={formatBytes(stats.data.storageBytes)}
                note="objects + index"
              />
            </div>
          ) : null}
        </div>

        <div className={styles.panelCard}>
          <span className={styles.panelLabel}>By record type</span>
          {types.isPending ? (
            <Skeleton height="8rem" width="100%" />
          ) : breakdown.length === 0 ? (
            <EmptyState
              icon={icons.records}
              title="No datasets yet"
              description="Published schemas and their record counts appear here once the archive holds data."
            />
          ) : (
            <div className={styles.typeList}>
              {breakdown.map((t) => (
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
