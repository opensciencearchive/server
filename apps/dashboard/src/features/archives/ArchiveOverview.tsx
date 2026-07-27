"use client";

import { CopyButton, Skeleton } from "@/ui";

import { useServices } from "@/api/services";
import type { Archive } from "@/domain/archive";
import { useOrganisation } from "../organisations/useOrganisations";
import { useDeploymentStatus } from "../deployments/useDeploymentStatus";
import { DeploymentPanel } from "../deployments/DeploymentPanel";
import { NextSteps } from "../tenant-insights/NextSteps";
import { UsageSection } from "../tenant-insights/UsageSection";
import { ValidationSection } from "../tenant-insights/ValidationSection";
import { WhatsInHere } from "../tenant-insights/WhatsInHere";
import { useArchive } from "./useArchives";
import styles from "./ArchiveOverview.module.css";

function formatDate(date: Date): string {
  return date.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

function signInSummary(adminCount: number): string {
  const noun = adminCount === 1 ? "administrator" : "administrators";
  return `ORCID · ${adminCount} ${noun}`;
}

export function ArchiveOverview({ archiveId }: { archiveId: string }) {
  const isPlatform = useServices().isPlatform;
  const archive = useArchive(archiveId);

  if (archive.isPending) {
    return (
      <div className={styles.page}>
        <Skeleton height="16rem" width="100%" />
        <Skeleton height="20rem" width="100%" />
      </div>
    );
  }

  if (!archive.data) return null;

  return (
    <div className={styles.page}>
      {isPlatform ? (
        <PlatformHero archiveId={archiveId} archive={archive.data} />
      ) : (
        <SelfHostHero archive={archive.data} />
      )}

      <WhatsInHere archiveId={archiveId} />
      <ValidationSection archiveId={archiveId} />
      <UsageSection archiveId={archiveId} />
      <NextSteps />
    </div>
  );
}

/** Cloud hero: live deployment status, the public domain, and fleet metadata. */
function PlatformHero({
  archiveId,
  archive: data,
}: {
  archiveId: string;
  archive: Archive;
}) {
  const deployment = useDeploymentStatus(archiveId);

  const isRunning =
    data.status.kind === "running" ||
    deployment.data?.status.kind === "succeeded";
  const visitUrl =
    deployment.data?.status.kind === "succeeded"
      ? (deployment.data.status.url ?? `https://${data.domain}`)
      : `https://${data.domain}`;

  return (
    <div className={styles.hero}>
      <div className={styles.heroGrid}>
        <div className={styles.heroLeft}>
          <div className={styles.heroTitle}>
            <span className={styles.eyebrow}>Archive</span>
            <h1>{data.name}</h1>
          </div>
          <div className={styles.domainRow}>
            <span className={styles.domain}>{data.domain}</span>
            <CopyButton value={data.domain} size="sm" />
            {isRunning && (
              <a
                className={styles.visit}
                href={visitUrl}
                target="_blank"
                rel="noreferrer"
              >
                ↗ Visit
              </a>
            )}
          </div>
        </div>

        <div className={styles.heroRight}>
          <DeploymentPanel archiveId={archiveId} />
        </div>
      </div>

      <PlatformMetaStrip archive={data} />
    </div>
  );
}

/**
 * Self-host hero: this server *is* the archive, so there is no cloud
 * deployment, public domain, org, or region to show. Content lives in the
 * tenant sections below.
 */
function SelfHostHero({ archive }: { archive: Archive }) {
  return (
    <div className={styles.hero}>
      <div className={styles.heroGrid}>
        <div className={styles.heroLeft}>
          <div className={styles.heroTitle}>
            <span className={styles.eyebrow}>Archive</span>
            <h1>{archive.name}</h1>
            <p className={styles.blurb}>
              Public to read; depositors sign in with ORCID.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function PlatformMetaStrip({ archive: data }: { archive: Archive }) {
  const organisation = useOrganisation(data.organisationId);

  return (
    <div className={styles.metaStrip}>
      <MetaCell label="Organisation" value={organisation.data?.name ?? "…"} />
      <MetaCell
        label="Region"
        value={data.deploymentConfig?.region ?? "—"}
        mono
      />
      <MetaCell label="Sign-in" value={signInSummary(data.orcidAdmins.length)} />
      <MetaCell label="Created" value={formatDate(data.createdAt)} mono last />
    </div>
  );
}

function MetaCell({
  label,
  value,
  mono,
  last,
}: {
  label: string;
  value: string;
  mono?: boolean;
  last?: boolean;
}) {
  return (
    <div className={[styles.metaCell, last ? styles.metaCellLast : ""].join(" ")}>
      <span className={styles.metaLabel}>{label}</span>
      <span className={mono ? styles.metaValueMono : styles.metaValue}>
        {value}
      </span>
    </div>
  );
}
