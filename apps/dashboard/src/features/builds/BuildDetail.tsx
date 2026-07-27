"use client";

import { Badge, Skeleton } from "@/ui";
import type { Build } from "@/domain/build";

import { BuildPipeline } from "./BuildPipeline";
import { ComponentTable } from "./ComponentTable";
import { CopyBuildSummary } from "./CopyBuildSummary";
import { buildStatusInFlight, buildStatusTone } from "./status";
import { useBuild } from "./useBuild";
import styles from "./BuildDetail.module.css";

function formatTime(date: Date): string {
  return date.toLocaleString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function BuildView({ build }: { build: Build }) {
  const { status } = build;
  const inFlight = buildStatusInFlight(status.kind);
  const failed =
    status.kind === "build_failed" || status.kind === "publish_failed";

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.headerMain}>
          <span className="eyebrow">Build</span>
          <div className={styles.title}>
            <h1 className={styles.id}>{build.id}</h1>
            <Badge tone={buildStatusTone(status.kind)} withDot>
              {inFlight ? (
                <span className={styles.pulsing}>{status.kind}</span>
              ) : (
                status.kind
              )}
            </Badge>
          </div>
          <div className={styles.meta}>
            <span>{build.conventionSlug}</span>
            {build.conventionRef && (
              <>
                <span className={styles.sep}>·</span>
                <span>ref {build.conventionRef}</span>
              </>
            )}
            {status.kind === "published" && (
              <>
                <span className={styles.sep}>·</span>
                <span>published {formatTime(status.publishedAt)}</span>
              </>
            )}
            <span className={styles.sep}>·</span>
            <span>created {formatTime(build.createdAt)}</span>
          </div>
        </div>
        <CopyBuildSummary build={build} />
      </header>

      {status.kind === "cancelled" ? (
        <div className={styles.cancelBanner} data-testid="cancel-banner">
          <span className={styles.cancelLabel}>Cancelled</span>
          <div>
            <p className={styles.cancelHead}>Cancelled by {status.cancelledBy}</p>
            {status.cancelReason && (
              <p className={styles.cancelReason}>{status.cancelReason}</p>
            )}
          </div>
        </div>
      ) : (
        <BuildPipeline status={status} componentCount={build.components.length} />
      )}

      {failed && (
        <div className={styles.errorPanel}>
          <span className={styles.errorLabel}>Error</span>
          <pre className={styles.errorText}>{status.errorMessage}</pre>
        </div>
      )}

      <section className={styles.components}>
        <div className={styles.componentsHead}>
          <h4>Components</h4>
        </div>
        <ComponentTable components={build.components} />
      </section>

      <div className={styles.provenance}>
        <span className={styles.provenanceLabel}>Provenance</span>
        <p>
          Digests are immutable content addresses; a build ID is the deterministic
          image tag. A redeploy mints new digests — nothing is overwritten in
          place. This page answers exactly what is running.
        </p>
      </div>
    </div>
  );
}

export function BuildDetail({ buildId }: { buildId: string }) {
  const build = useBuild(buildId);

  if (build.isPending) {
    return (
      <div className={styles.page}>
        <Skeleton height="4rem" width="100%" />
        <Skeleton height="5rem" width="100%" />
        <Skeleton height="12rem" width="100%" />
      </div>
    );
  }

  if (build.isError || !build.data) {
    return (
      <div className={styles.errorPanel}>
        <span className={styles.errorLabel}>Error</span>
        <pre className={styles.errorText}>Build not found.</pre>
      </div>
    );
  }

  return <BuildView build={build.data} />;
}
