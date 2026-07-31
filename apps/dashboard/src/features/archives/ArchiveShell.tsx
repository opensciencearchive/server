"use client";

import { AppShell } from "../shell/AppShell";
import { Breadcrumbs } from "../shell/Breadcrumbs";
import { CenteredNotice } from "../shell/CenteredNotice";
import { type NavItem, type NavSection, SidebarNav } from "../shell/SidebarNav";
import { Badge } from "@/ui";

import { ApiError } from "@/api/http/errors";
import { useServices } from "@/api/services";
import type { Archive, ArchiveStatus } from "@/domain/archive";
import { useBuildList } from "../builds/useBuild";
import { useOrganisation } from "../organisations/useOrganisations";
import { archiveStatusInFlight, archiveStatusTone } from "./status";
import { useArchive } from "./useArchives";
import styles from "./ArchiveShell.module.css";

interface ShellProps {
  archiveId: string;
  children: React.ReactNode;
}

/**
 * The project-level chrome. Platform and self-host diverge in the fleet-shaped
 * bits (org breadcrumb, "Visit archive", the Builds nav item + count all read
 * cloud data), so each identity renders its own subcomponent and calls only the
 * hooks valid in that build — the branch is a mount boundary, not a conditional
 * hook.
 */
export function ArchiveShell({ archiveId, children }: ShellProps) {
  return useServices().isPlatform ? (
    <PlatformArchiveShell archiveId={archiveId}>{children}</PlatformArchiveShell>
  ) : (
    <SelfHostArchiveShell archiveId={archiveId}>{children}</SelfHostArchiveShell>
  );
}

/** Shared sidebar. `builds` present ⇒ show the (platform-only) Builds item. */
function navSections(base: string, builds?: { count?: number }): NavSection[] {
  const buildsItem: NavItem[] = builds
    ? [
        {
          label: "Builds",
          href: `${base}/builds`,
          icon: "builds",
          ...(builds.count !== undefined ? { count: builds.count } : {}),
        },
      ]
    : [];
  return [
    {
      items: [
        { label: "Overview", href: base, icon: "overview" },
        {
          label: "Records",
          href: `${base}/records`,
          icon: "records",
          children: [{ label: "Features", href: `${base}/features` }],
        },
        { label: "Agents", href: `${base}/agents`, icon: "agents" },
      ],
    },
    {
      items: [
        { label: "Hooks", href: `${base}/hooks`, icon: "hooks" },
        { label: "Ingesters", href: `${base}/ingesters`, icon: "ingesters" },
        { label: "Ingestions", href: `${base}/ingestions`, icon: "ingestions" },
        ...buildsItem,
        {
          label: "Observability",
          href: `${base}/observability`,
          icon: "observability",
        },
      ],
    },
    {
      items: [
        {
          label: "Authentication",
          href: `${base}/authentication`,
          icon: "authentication",
        },
        { label: "Settings", href: `${base}/settings`, icon: "settings" },
      ],
    },
  ];
}

/** The status pill shown next to the archive name in the breadcrumb. */
function statusBadge(status: ArchiveStatus | undefined): React.ReactNode {
  if (!status) return undefined;
  return (
    <Badge
      tone={archiveStatusTone(status.kind)}
      withDot
      title={status.kind === "error" ? status.message : undefined}
    >
      {archiveStatusInFlight(status) ? (
        <span className={styles.pulsing}>{status.kind}</span>
      ) : (
        status.kind
      )}
    </Badge>
  );
}

function PlatformArchiveShell({ archiveId, children }: ShellProps) {
  const archive = useArchive(archiveId);
  const organisation = useOrganisation(archive.data?.organisationId ?? "");
  const builds = useBuildList(archiveId); // @mock count — no list endpoint
  const base = `/archives/${archiveId}`;

  // A bad/stale archive URL shouldn't render the whole dashboard with every
  // panel 404-ing — short-circuit before the shell (and children) mount.
  if (archive.error instanceof ApiError && archive.error.status === 404) {
    return (
      <CenteredNotice
        title="Archive not found"
        description="This archive doesn't exist, or you don't have access to it."
        actionHref="/"
        actionLabel="Back to archives"
      />
    );
  }

  return (
    <AppShell
      breadcrumbs={
        <Breadcrumbs
          crumbs={[
            ...(archive.data && organisation.data
              ? [
                  {
                    label: organisation.data.name,
                    href: `/organisations/${organisation.data.id}`,
                  },
                ]
              : []),
            {
              label: archive.data?.name ?? "…",
              badge: statusBadge(archive.data?.status),
            },
          ]}
        />
      }
      headerActions={
        archive.data && archive.data.status.kind === "running" ? (
          <a
            className={styles.visit}
            href={`https://${archive.data.domain}`}
            target="_blank"
            rel="noreferrer"
          >
            ↗ Visit archive
          </a>
        ) : undefined
      }
      sidebar={
        <SidebarNav
          sections={navSections(
            base,
            builds.data ? { count: builds.data.length } : {},
          )}
        />
      }
    >
      {children}
    </AppShell>
  );
}

function SelfHostArchiveShell({ archiveId, children }: ShellProps) {
  const archive = useArchive(archiveId);
  const base = `/archives/${archiveId}`;
  const data: Archive | undefined = archive.data;

  return (
    <AppShell
      breadcrumbs={
        <Breadcrumbs
          crumbs={[
            {
              label: data?.name ?? "…",
              badge: statusBadge(data?.status),
            },
          ]}
        />
      }
      sidebar={<SidebarNav sections={navSections(base)} />}
    >
      {children}
    </AppShell>
  );
}
