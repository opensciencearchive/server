"use client";

import Link from "next/link";

import { Badge, DataTable, PageHeader, SampleDataChip, Skeleton } from "@/ui";
import type { BuildListItem } from "@/domain/tenant";

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
        actions={<SampleDataChip />}
      />

      {builds.isPending ? (
        <Skeleton height="12rem" width="100%" />
      ) : builds.data ? (
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
          rows={builds.data.data}
          rowKey={(b) => b.id}
        />
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          Build history needs a list API — this table is sample data. Real builds
          are reached from the link that <span className="mono">osa deploy</span>{" "}
          prints when it submits a convention.
        </p>
      </div>
    </div>
  );
}
