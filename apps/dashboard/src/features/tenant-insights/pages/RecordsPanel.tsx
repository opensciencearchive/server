"use client";

import { DataTable, PageHeader, SampleDataChip, Skeleton, Stat } from "@/ui";
import type { TenantRecord } from "@/domain/tenant";

import { useRecordStats, useTenantRecords } from "../hooks";
import styles from "./pages.module.css";

function formatStorage(bytes: number): string {
  const tb = bytes / 1e12;
  if (tb >= 1) return `${tb.toFixed(1)} TB`;
  return `${(bytes / 1e9).toFixed(0)} GB`;
}

export function RecordsPanel({ archiveId }: { archiveId: string }) {
  const stats = useRecordStats(archiveId);
  const records = useTenantRecords(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Records"
        description="The published records held in this archive."
        actions={<SampleDataChip />}
      />

      {stats.isPending ? (
        <Skeleton height="5rem" width="100%" />
      ) : stats.data ? (
        <div className={styles.stats}>
          <Stat
            label="Published records"
            value={stats.data.data.publishedRecords.toLocaleString("en-GB")}
          />
          <Stat
            label="This month"
            value={`+${stats.data.data.recordsThisMonth.toLocaleString("en-GB")}`}
            note="new depositions"
            noteTone="success"
          />
          <Stat
            label="Storage"
            value={formatStorage(stats.data.data.storageBytes)}
          />
        </div>
      ) : null}

      {records.isPending ? (
        <Skeleton height="12rem" width="100%" />
      ) : records.data ? (
        <DataTable<TenantRecord>
          columns={[
            {
              key: "record",
              header: "Record",
              render: (r) => (
                <>
                  <span className={styles.srn}>{r.id}</span>
                  <span className={styles.recordTitle}>{r.title}</span>
                </>
              ),
            },
            {
              key: "type",
              header: "Type",
              render: (r) => r.type,
            },
            {
              key: "depositor",
              header: "Depositor",
              render: (r) => <span className="mono">{r.depositor}</span>,
            },
            {
              key: "deposited",
              header: "Deposited",
              align: "right",
              render: (r) =>
                r.depositedAt.toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "short",
                  year: "numeric",
                }),
            },
          ]}
          rows={records.data.data}
          rowKey={(r) => r.id}
        />
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          These records are sample data. Live tenant records arrive when the
          dashboard&apos;s connection to the archive&apos;s OSA instance ships.
        </p>
      </div>
    </div>
  );
}
