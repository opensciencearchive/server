"use client";

import { DataTable, PageHeader, SampleDataChip, Skeleton, Stat } from "@/ui";
import type { ValidationCheck } from "@/domain/tenant";

import { useValidationSummary } from "../hooks";
import styles from "./pages.module.css";

export function ValidationPanel({ archiveId }: { archiveId: string }) {
  const summary = useValidationSummary(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Validation"
        description="Every deposition is checked against the archive's conventions and policies."
        actions={<SampleDataChip />}
      />

      {summary.isPending ? (
        <Skeleton height="5rem" width="100%" />
      ) : summary.data ? (
        <>
          <div className={[styles.stats, styles.statsWide].join(" ")}>
            <Stat
              label="Pass rate"
              value={`${summary.data.data.passRatePercent}%`}
              note="records passing every check"
              noteTone="success"
            />
            <Stat
              label="Last full pass"
              value={
                summary.data.data.lastFullPassAt
                  ? summary.data.data.lastFullPassAt.toLocaleString("en-GB", {
                      day: "numeric",
                      month: "short",
                      hour: "2-digit",
                      minute: "2-digit",
                    })
                  : "—"
              }
            />
          </div>

          <DataTable<ValidationCheck>
            columns={[
              { key: "name", header: "Check", render: (c) => c.name },
              {
                key: "definedBy",
                header: "Defined by",
                render: (c) => <span className="mono">{c.definedBy}</span>,
              },
              {
                key: "passing",
                header: "Passing",
                align: "right",
                render: (c) => (
                  <span className="mono">
                    {c.passing.toLocaleString("en-GB")}
                  </span>
                ),
              },
              {
                key: "failing",
                header: "Failing",
                align: "right",
                render: (c) => (
                  <span className="mono">
                    {c.failing.toLocaleString("en-GB")}
                  </span>
                ),
              },
            ]}
            rows={summary.data.data.checks}
            rowKey={(c) => c.name}
            rowTone={(c) => (c.failing > 0 ? "warning" : undefined)}
          />
        </>
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          These validation results are sample data. Live check results are read
          from the archive&apos;s OSA instance when the tenant API connection
          ships.
        </p>
      </div>
    </div>
  );
}
