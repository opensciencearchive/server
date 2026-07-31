"use client";

import { CopyButton, Skeleton } from "@/ui";

import { useServices } from "@/api/services";
import type { Archive } from "@/domain/archive";
import type { NodeStatus } from "@/domain/node";
import { useOrganisation } from "../organisations/useOrganisations";
import { useDeploymentStatus } from "../deployments/useDeploymentStatus";
import { DeploymentPanel } from "../deployments/DeploymentPanel";
import { NextSteps } from "../tenant-insights/NextSteps";
import { WhatsInHere } from "../tenant-insights/WhatsInHere";
import { useArchive } from "./useArchives";
import { useNodeOverview } from "./useNodeOverview";
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

  return (
    <div className={styles.page}>
      {isPlatform ? <PlatformOverviewHero archiveId={archiveId} /> : <SelfHostHero />}

      <NextSteps archiveId={archiveId} />
      <WhatsInHere archiveId={archiveId} />
    </div>
  );
}

// ── Platform (cloud) ────────────────────────────────────────────────────────

function PlatformOverviewHero({ archiveId }: { archiveId: string }) {
  const archive = useArchive(archiveId);
  if (archive.isPending) return <Skeleton height="16rem" width="100%" />;
  if (!archive.data) return null;
  return <PlatformHero archiveId={archiveId} archive={archive.data} />;
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

// ── Self-host ───────────────────────────────────────────────────────────────

/**
 * Self-host hero, wired to the live node (`/api/node`): identity + description
 * + domain on the left, a status panel on the right, and a stats meta strip.
 * Loads with a skeleton and degrades to a clear fallback if the archive API
 * can't be reached.
 */
function SelfHostHero() {
  const node = useNodeOverview();

  if (node.isPending) return <HeroSkeleton />;
  if (node.isError || node.data === undefined) return <HeroUnavailable />;

  const n = node.data;
  const visitUrl = externalUrl(n.domain);

  return (
    <div className={styles.hero}>
      <div className={styles.heroGrid}>
        <div className={styles.heroLeft}>
          <div className={styles.heroTitle}>
            <span className={styles.eyebrow}>Archive</span>
            <h1>{n.name || "Local archive"}</h1>
            {n.description && <p className={styles.blurb}>{n.description}</p>}
          </div>
          {n.domain && (
            <div className={styles.domainRow}>
              <span className={styles.domain}>{n.domain}</span>
              <CopyButton value={n.domain} size="sm" />
              {visitUrl && (
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
          )}
        </div>

        <div className={styles.heroRight}>
          <NodeStatusPanel status={n.status} />
        </div>
      </div>

      <div className={styles.metaStrip}>
        <MetaCell label="Records" value={formatCount(n.records)} mono />
        <MetaCell label="Schemas" value={String(n.schemas)} mono />
        <MetaCell label="OSA version" value={n.osaVersion || "—"} mono />
        <MetaCell label="Domain" value={n.domain || "—"} mono last />
      </div>
    </div>
  );
}

const STATUS_DISPLAY: Record<NodeStatus, { label: string; note: string; dot: string }> = {
  ready: {
    label: "Running",
    note: "The public read API is live.",
    dot: styles.statusDotReady!,
  },
  degraded: {
    label: "Degraded",
    note: "Some components are unhealthy.",
    dot: styles.statusDotDegraded!,
  },
  unknown: {
    label: "Unknown",
    note: "Couldn't read the node's status.",
    dot: styles.statusDotUnknown!,
  },
};

function NodeStatusPanel({ status }: { status: NodeStatus }) {
  const s = STATUS_DISPLAY[status];
  return (
    <div className={styles.statusPanel}>
      <span className={styles.eyebrow}>Status</span>
      <div className={styles.statusHeadline}>
        <span className={[styles.statusDot, s.dot].join(" ")} />
        <span>{s.label}</span>
      </div>
      <p className={styles.statusNote}>{s.note}</p>
    </div>
  );
}

function HeroSkeleton() {
  return (
    <div className={styles.hero}>
      <div className={styles.heroGrid}>
        <div className={styles.heroLeft}>
          <Skeleton height="1rem" width="5rem" />
          <Skeleton height="2.5rem" width="60%" />
          <Skeleton height="3rem" width="90%" />
        </div>
        <div className={styles.heroRight}>
          <Skeleton height="1rem" width="4rem" />
          <Skeleton height="2rem" width="8rem" />
        </div>
      </div>
    </div>
  );
}

function HeroUnavailable() {
  return (
    <div className={styles.hero}>
      <div className={styles.heroGrid}>
        <div className={styles.heroLeft}>
          <div className={styles.heroTitle}>
            <span className={styles.eyebrow}>Archive</span>
            <h1>Local archive</h1>
            <p className={styles.blurb}>
              The archive API isn&apos;t reachable right now — check that the
              server is running, then refresh.
            </p>
          </div>
        </div>
        <div className={styles.heroRight}>
          <NodeStatusPanel status="unknown" />
        </div>
      </div>
    </div>
  );
}

/** A visitable URL for a node domain, or null for a local/unset domain. */
function externalUrl(domain: string): string | null {
  if (!domain || domain === "localhost" || domain.startsWith("localhost:")) {
    return null;
  }
  return domain.startsWith("http") ? domain : `https://${domain}`;
}

function formatCount(value: number | null): string {
  return value === null ? "—" : value.toLocaleString("en-GB");
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
