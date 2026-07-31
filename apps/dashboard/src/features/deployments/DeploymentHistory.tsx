"use client";

import type { Deployment, DeploymentStatus } from "@/domain/deployment";
import { deploymentDurationMs } from "@/domain/deployment";
import { Badge, DataTable, EmptyState, Skeleton, type Tone } from "@/ui";
import { icons } from "@/features/shell/icons";

import { useDeployments } from "./useDeployments";
import styles from "./DeploymentHistory.module.css";

const STATUS_TONE: Record<DeploymentStatus["kind"], Tone> = {
  pending: "info",
  in_progress: "info",
  succeeded: "success",
  failed: "danger",
};

const STATUS_LABEL: Record<DeploymentStatus["kind"], string> = {
  pending: "queued",
  in_progress: "in_progress",
  succeeded: "succeeded",
  failed: "failed",
};

function formatDateTime(date: Date): string {
  return date.toLocaleString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function formatDuration(ms: number | null): string {
  if (ms === null) return "—";
  const totalSeconds = Math.max(0, Math.round(ms / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes === 0 ? `${seconds} s` : `${minutes} m ${seconds} s`;
}

/** Every provisioning attempt for an archive, newest first. */
export function DeploymentHistory({ archiveId }: { archiveId: string }) {
  const deployments = useDeployments(archiveId);
  const rows = deployments.data ?? [];

  return (
    <section className={styles.section}>
      <div className={styles.head}>
        <span className={styles.icon}>{icons.archives}</span>
        <h3>Deployment history</h3>
        <p className={styles.blurb}>
          every provisioning attempt, and the OSA version it put in place
        </p>
      </div>

      {deployments.isPending ? (
        <Skeleton height="8rem" width="100%" />
      ) : rows.length > 0 ? (
        <DataTable<Deployment>
          columns={[
            {
              key: "started",
              header: "Started",
              render: (d) => (
                <>
                  <span className="mono">{formatDateTime(d.startedAt)}</span>
                  {d.status.kind === "failed" && (
                    <span className={styles.failReason}>
                      {d.status.errorMessage}
                    </span>
                  )}
                </>
              ),
            },
            {
              key: "status",
              header: "Status",
              render: (d) => (
                <Badge tone={STATUS_TONE[d.status.kind]} withDot>
                  {STATUS_LABEL[d.status.kind]}
                </Badge>
              ),
            },
            {
              key: "version",
              header: "Version",
              render: (d) => (
                <span className="mono">{d.osaVersion ?? "—"}</span>
              ),
            },
            {
              key: "took",
              header: "Took",
              align: "right",
              render: (d) => (
                <span className="mono">
                  {formatDuration(deploymentDurationMs(d))}
                </span>
              ),
            },
          ]}
          rows={rows}
          rowKey={(d) => d.id}
          rowTone={(d) => (d.status.kind === "failed" ? "danger" : undefined)}
        />
      ) : deployments.isError ? (
        <EmptyState
          icon={icons.archives}
          title="History unavailable"
          description="The control plane did not return this archive's deployments. Refresh to try again."
        />
      ) : (
        <EmptyState
          icon={icons.archives}
          title="No deployments recorded"
          description="Provisioning attempts appear here once this archive is deployed."
        />
      )}
    </section>
  );
}
