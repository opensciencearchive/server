"use client";

import type { Deployment, DeploymentStatus } from "@/domain/deployment";
import { deploymentDurationMs } from "@/domain/deployment";
import { Button, CopyButton, Skeleton } from "@/ui";

import { useArchive } from "../archives/useArchives";
import { useDeploymentStatus } from "./useDeploymentStatus";
import { useRedeploy } from "./useRedeploy";
import styles from "./DeploymentPanel.module.css";

/** "now" for uptime/elapsed maths — fixed so the mock story reads clean. */
const NOW = new Date("2026-07-25T14:08:00Z");

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

function formatClock(date: Date): string {
  return date.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function formatDuration(ms: number): string {
  const totalSeconds = Math.max(0, Math.round(ms / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes === 0) return `${seconds} s`;
  return `${minutes} m ${seconds} s`;
}

function uptimeLabel(completedAt: Date): string {
  const days = Math.max(0, Math.floor((NOW.getTime() - completedAt.getTime()) / 86_400_000));
  return `${days} d uptime`;
}

/**
 * The "CURRENT DEPLOYMENT" column: Running summary, the in-progress
 * timeline, or the failure panel — driven by the polled deployment status.
 */
export function DeploymentPanel({ archiveId }: { archiveId: string }) {
  const archive = useArchive(archiveId);
  const status = useDeploymentStatus(archiveId);
  const redeploy = useRedeploy(archiveId);

  if (status.isPending || archive.isPending) {
    return <Skeleton height="12rem" width="100%" />;
  }

  const deployment = status.data;
  if (!deployment) return null;

  const onRedeploy = () => redeploy.mutate(undefined);
  const kind = deployment.status.kind;

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <span className={styles.eyebrow}>Current deployment</span>
        {(kind === "pending" || kind === "in_progress") && (
          <span className={styles.refreshing}>
            <span className={styles.refreshDot} aria-hidden />
            refreshing every 3 s
          </span>
        )}
      </div>

      {kind === "succeeded" ? (
        <RunningView
          deployment={deployment}
          onRedeploy={onRedeploy}
          redeploying={redeploy.isPending}
        />
      ) : kind === "failed" ? (
        <FailedView
          message={deployment.status.errorMessage}
          startedAt={deployment.startedAt}
          completedAt={deployment.status.completedAt}
          onRedeploy={onRedeploy}
          redeploying={redeploy.isPending}
        />
      ) : (
        <DeployingView status={deployment.status} startedAt={deployment.startedAt} domain={archive.data?.domain ?? ""} />
      )}
    </div>
  );
}

function RunningView({
  deployment,
  onRedeploy,
  redeploying,
}: {
  deployment: Deployment;
  onRedeploy: () => void;
  redeploying: boolean;
}) {
  const status = deployment.status;
  const completedAt = status.kind === "succeeded" ? status.completedAt : null;
  const durationMs = deploymentDurationMs(deployment);

  return (
    <div className={styles.runningBody}>
      <div className={styles.runningHead}>
        <span className={styles.statusDot} aria-hidden />
        <span className={styles.statusName}>Running</span>
        {completedAt && <span className={styles.uptime}>{uptimeLabel(completedAt)}</span>}
      </div>

      <dl className={styles.rows}>
        <div className={styles.row}>
          <dt>Deployed</dt>
          <dd className={styles.mono}>{completedAt ? formatDateTime(completedAt) : "—"}</dd>
        </div>
        <div className={styles.row}>
          <dt>Version</dt>
          <dd className={styles.mono}>{deployment.osaVersion ?? "—"}</dd>
        </div>
        <div className={styles.row}>
          <dt>Took</dt>
          <dd className={styles.mono}>{durationMs != null ? formatDuration(durationMs) : "—"}</dd>
        </div>
      </dl>

      <Button
        className={styles.fullWidth}
        onClick={onRedeploy}
        disabled={redeploying}
      >
        {redeploying ? "Redeploying…" : "Redeploy"}
      </Button>
    </div>
  );
}

interface Stage {
  label: string;
  detail: React.ReactNode;
  state: "complete" | "active" | "pending";
}

function DeployingView({
  status,
  startedAt,
  domain,
}: {
  status: DeploymentStatus;
  startedAt: Date;
  domain: string;
}) {
  const isProvisioning = status.kind === "in_progress";
  const elapsed = formatDuration(NOW.getTime() - startedAt.getTime());

  const stages: Stage[] = [
    {
      label: "Accepted",
      detail: `${formatClock(startedAt)} · request received`,
      state: "complete",
    },
    {
      label: "Provisioning infrastructure",
      detail: (
        <>
          {isProvisioning ? `in_progress · ${elapsed} elapsed` : "queued"}
          <span className={styles.stageBlurb}>
            Compute, Postgres and the image registry for this archive.
          </span>
        </>
      ),
      state: isProvisioning ? "active" : "pending",
    },
    {
      label: `Archive answering at ${domain || "its URL"}`,
      detail: "pending",
      state: "pending",
    },
  ];

  return (
    <div className={styles.timeline}>
      {stages.map((stage, i) => (
        <div key={stage.label} className={styles.stage}>
          <div className={styles.rail}>
            <span
              className={[
                styles.node,
                stage.state === "complete" ? styles.nodeComplete : "",
                stage.state === "active" ? styles.nodeActive : "",
              ].join(" ")}
              aria-hidden
            >
              {stage.state === "complete" && (
                <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M3.5 8.5l3 3 6-6.5" />
                </svg>
              )}
              {stage.state === "active" && <span className={styles.nodePulse} />}
            </span>
            {i < stages.length - 1 && (
              <span
                className={[
                  styles.connector,
                  stage.state === "complete" ? styles.connectorComplete : "",
                ].join(" ")}
              />
            )}
          </div>
          <div className={styles.stageBody}>
            <span
              className={[
                styles.stageLabel,
                stage.state === "pending" ? styles.stagePending : "",
              ].join(" ")}
            >
              {stage.label}
            </span>
            <span className={styles.stageDetail}>{stage.detail}</span>
          </div>
        </div>
      ))}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <span>
          You can close this tab — deployment continues server-side, and the
          archive appears as running when it finishes.
        </span>
      </div>
    </div>
  );
}

function FailedView({
  message,
  startedAt,
  completedAt,
  onRedeploy,
  redeploying,
}: {
  message: string;
  startedAt: Date;
  completedAt: Date | null;
  onRedeploy: () => void;
  redeploying: boolean;
}) {
  return (
    <div className={styles.failed}>
      <div className={styles.failedHead}>
        <span className={styles.failedIcon} aria-hidden>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
            <path d="M4 4l8 8M12 4l-8 8" />
          </svg>
        </span>
        <div>
          <h4 className={styles.failedTitle}>Deployment failed</h4>
          <span className={styles.failedMeta}>
            failed · started {formatClock(startedAt)}
            {completedAt ? ` · ended ${formatClock(completedAt)}` : ""}
          </span>
        </div>
      </div>

      <pre className={styles.errorBlock}>{message}</pre>

      <div className={styles.failedActions}>
        <Button variant="primary" onClick={onRedeploy} disabled={redeploying}>
          {redeploying ? "Redeploying…" : "Redeploy"}
        </Button>
        <CopyButton value={message} label="Copy error" size="sm" />
      </div>
      <p className={styles.failedFoot}>
        Redeploy reuses your stored administrators and sealed credentials.
        Destroying this archive lives in Settings and releases the subdomain.
      </p>
    </div>
  );
}
