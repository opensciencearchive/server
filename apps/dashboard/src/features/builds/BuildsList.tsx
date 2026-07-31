"use client";

import Link from "next/link";

import { Badge, DataTable, EmptyState, PageHeader, Skeleton } from "@/ui";
import type { BuildListItem } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { buildStatusTone } from "./status";
import { useBuildList } from "./useBuild";
import styles from "./BuildsList.module.css";

function formatDate(date: Date): string {
  return date.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

export function BuildsList({ archiveId }: { archiveId: string }) {
  const builds = useBuildList(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Builds"
        description="Every deploy attempt of a convention — its components, status and provenance."
      />

      {builds.isPending ? (
        <Skeleton height="12rem" width="100%" />
      ) : builds.data && builds.data.length > 0 ? (
        <DataTable<BuildListItem>
          columns={[
            {
              key: "id",
              header: "Build",
              render: (b) => (
                <Link href={`./builds/${b.id}`} className="mono">
                  {b.id}
                </Link>
              ),
            },
            {
              key: "convention",
              header: "Convention",
              render: (b) => b.conventionSlug,
            },
            {
              key: "ref",
              header: "Ref",
              render: (b) => (
                <span className="mono">{b.conventionRef ?? "—"}</span>
              ),
            },
            {
              key: "status",
              header: "Status",
              render: (b) => (
                <Badge tone={buildStatusTone(b.statusKind)} withDot>
                  {b.statusKind}
                </Badge>
              ),
            },
            {
              key: "created",
              header: "Created",
              align: "right",
              render: (b) => formatDate(b.createdAt),
            },
          ]}
          rows={builds.data}
          rowKey={(b) => b.id}
        />
      ) : builds.isError ? (
        <EmptyState
          icon={icons.builds}
          title="Build history unavailable"
          description="The control plane did not return this archive's builds. Refresh to try again."
        />
      ) : (
        <EmptyState
          icon={icons.builds}
          title="No builds yet"
          description={
            <>
              A build appears here each time <span className="mono">osa deploy</span>{" "}
              submits a convention to this archive.
            </>
          }
        />
      )}
    </div>
  );
}
