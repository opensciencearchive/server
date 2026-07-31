"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

import { ApiError } from "@/api/http/errors";
import { StatusDot } from "@/ui";

import { useArchive } from "../archives/useArchives";
import { CenteredNotice } from "../shell/CenteredNotice";
import { CenteredScreen } from "../shell/CenteredScreen";
import { useDeploymentStatus } from "./useDeploymentStatus";
import styles from "./DeployingScreen.module.css";

function is404(err: unknown): boolean {
  return err instanceof ApiError && err.status === 404;
}

/**
 * Post-create deployment progress (#185). A chrome-less centered screen shown
 * right after an archive is created: it polls the deployment (pre-seeded by
 * `useCreateArchive`) and, once it succeeds, redirects to the archive dashboard.
 * A failure or a bad/unknown id resolves to a centered notice instead.
 */
export function DeployingScreen({ archiveId }: { archiveId: string }) {
  const router = useRouter();
  const archive = useArchive(archiveId);
  const deployment = useDeploymentStatus(archiveId);
  const status = deployment.data?.status;

  useEffect(() => {
    if (status?.kind === "succeeded") {
      router.replace(`/archives/${archiveId}`);
    }
  }, [status?.kind, archiveId, router]);

  if (is404(archive.error) || is404(deployment.error)) {
    return (
      <CenteredNotice
        title="Archive not found"
        description="This archive doesn't exist, or you don't have access to it."
        actionHref="/"
        actionLabel="Back to archives"
      />
    );
  }

  if (status?.kind === "failed") {
    return (
      <CenteredNotice
        tone="danger"
        title="Deployment failed"
        description={status.errorMessage}
        actionHref={`/archives/${archiveId}`}
        actionLabel="View archive"
      />
    );
  }

  const name = archive.data?.name ?? "your archive";
  const domain = archive.data?.domain;

  return (
    <CenteredScreen>
      <div className={styles.card}>
        <h1 className={styles.title}>Deploying {name}</h1>
        {domain && <p className={styles.domain}>{domain}</p>}
        <div className={styles.status}>
          <StatusDot
            tone="info"
            pulse
            mono={false}
            label="Provisioning your archive…"
          />
        </div>
        <p className={styles.hint}>
          This usually takes a minute or two. This page updates automatically and
          opens your archive as soon as it&apos;s ready.
        </p>
        <Link className={styles.back} href="/">
          Back to archives
        </Link>
      </div>
    </CenteredScreen>
  );
}
