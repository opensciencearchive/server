"use client";

import {
  Badge,
  DataTable,
  EmptyState,
  PageHeader,
  SampleDataChip,
  Skeleton,
} from "@/ui";
import type { Tone } from "@/ui";
import { useServices } from "@/api/services";
import type { IngestionRun, IngestionStatus } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { useIngestionRuns } from "../hooks";
import styles from "./pages.module.css";

const STATUS: Record<IngestionStatus, { label: string; tone: Tone }> = {
  pending: { label: "Pending", tone: "neutral" },
  running: { label: "Running", tone: "info" },
  completed: { label: "Completed", tone: "success" },
  failed: { label: "Failed", tone: "danger" },
};

function formatStarted(date: Date): string {
  return date.toLocaleString("en-GB", {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function IngestionsPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real runs; platform's are sample data (chip + note).
  const isSample = useServices().isPlatform;
  const runs = useIngestionRuns(archiveId);
  const list = runs.data ?? [];

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.ingestions}
        title="Ingestions"
        description="Bulk ingest runs — in-progress and completed. Live runs update as batches land."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {runs.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : list.length > 0 ? (
        <DataTable<IngestionRun>
          columns={[
            {
              key: "run",
              header: "Run",
              render: (r) => (
                <>
                  <span className={styles.srn}>{r.id}</span>
                  <span className={styles.recordTitle}>{r.convention}</span>
                  {r.status === "failed" && r.failureReason && (
                    <span className={styles.failReason}>{r.failureReason}</span>
                  )}
                </>
              ),
            },
            {
              key: "status",
              header: "Status",
              render: (r) => (
                <Badge tone={STATUS[r.status].tone} withDot>
                  {STATUS[r.status].label}
                </Badge>
              ),
            },
            {
              key: "batches",
              header: "Batches",
              align: "right",
              render: (r) => (
                <span className="mono">
                  {r.batchesCompleted}/{r.batchesIngested}
                  {r.batchesFailed > 0 ? ` · ${r.batchesFailed} failed` : ""}
                </span>
              ),
            },
            {
              key: "published",
              header: "Published",
              align: "right",
              render: (r) => r.publishedCount.toLocaleString("en-GB"),
            },
            {
              key: "started",
              header: "Started",
              align: "right",
              render: (r) => formatStarted(r.startedAt),
            },
          ]}
          rows={list}
          rowKey={(r) => r.id}
          rowTone={(r) => (r.status === "failed" ? "danger" : undefined)}
        />
      ) : (
        <EmptyState
          icon={icons.ingestions}
          title="No ingest runs yet"
          description="Bulk ingest runs appear here once you start one for a convention with a source."
        />
      )}

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            These ingest runs are sample data. Self-hosted archives show their
            real runs, updating live while in progress.
          </p>
        </div>
      )}
    </div>
  );
}
