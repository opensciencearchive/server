"use client";

import { PageHeader, SampleDataChip, Skeleton, Stat } from "@/ui";

import { useSearchOverview } from "../hooks";
import styles from "./pages.module.css";

export function SearchPanel({ archiveId }: { archiveId: string }) {
  const overview = useSearchOverview(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Search"
        description="Full-text and structured search runs inside the archive, over every indexed record."
        actions={<SampleDataChip />}
      />

      {overview.isPending ? (
        <Skeleton height="5rem" width="100%" />
      ) : overview.data ? (
        <>
          <div className={[styles.stats, styles.statsWide].join(" ")}>
            <Stat
              label="Indexed records"
              value={overview.data.data.indexedRecords.toLocaleString("en-GB")}
            />
            <Stat
              label="Last indexed"
              value={
                overview.data.data.lastIndexedAt
                  ? overview.data.data.lastIndexedAt.toLocaleString("en-GB", {
                      day: "numeric",
                      month: "short",
                      hour: "2-digit",
                      minute: "2-digit",
                    })
                  : "—"
              }
            />
          </div>

          <div className={styles.section}>
            <h2 className={styles.sectionTitle}>Example queries</h2>
            <div className={styles.chips}>
              {overview.data.data.exampleQueries.map((q) => (
                <span key={q} className={styles.chip}>
                  {q}
                </span>
              ))}
            </div>
          </div>
        </>
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          Search runs inside the archive&apos;s OSA instance. These figures and
          example queries are sample data until the tenant API connection ships.
        </p>
      </div>
    </div>
  );
}
