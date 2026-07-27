"use client";

import { DataTable, SampleDataChip, Skeleton } from "@/ui";
import type { ValidationCheck } from "@/domain/tenant";

import { useValidationSummary } from "./hooks";
import styles from "./tenant-insights.module.css";

function formatTimestamp(date: Date): string {
  return date.toLocaleString("en-GB", {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export function ValidationSection({ archiveId }: { archiveId: string }) {
  const validation = useValidationSummary(archiveId);
  const summary = validation.data?.data;

  return (
    <section className={styles.section}>
      <div className={styles.sectionHead}>
        <div className={styles.headLeft}>
          <h3>Validation</h3>
          <SampleDataChip />
          {summary && (
            <span className={styles.validationBlurb}>
              <strong>{summary.passRatePercent}%</strong> of records pass every
              check
            </span>
          )}
        </div>
        {summary?.lastFullPassAt && (
          <span className={styles.headMeta}>
            last full pass {formatTimestamp(summary.lastFullPassAt)}
          </span>
        )}
      </div>

      {validation.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : summary ? (
        <div className={styles.tableCard}>
          <DataTable<ValidationCheck>
            columns={[
              {
                key: "check",
                header: "Check",
                render: (c) => c.name,
              },
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
                  <span className={styles.mono}>
                    {c.passing.toLocaleString("en-GB")}
                  </span>
                ),
              },
              {
                key: "failing",
                header: "Failing",
                align: "right",
                render: (c) => (
                  <span
                    className={[
                      styles.mono,
                      c.failing > 0 ? styles.failingCount : "",
                    ].join(" ")}
                  >
                    {c.failing.toLocaleString("en-GB")}
                  </span>
                ),
              },
            ]}
            rows={summary.checks}
            rowKey={(c) => c.name}
            rowTone={(c) => (c.failing > 0 ? "warning" : undefined)}
          />
        </div>
      ) : null}
    </section>
  );
}
