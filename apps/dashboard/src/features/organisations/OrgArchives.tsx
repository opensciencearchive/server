"use client";

import Link from "next/link";

import { Button, EmptyState, PageHeader, Skeleton } from "@/ui";

import { ArchiveCard } from "../archives/ArchiveCard";
import { useOrgArchives, useOrganisation } from "./useOrganisations";
import styles from "./OrgArchives.module.css";

export function OrgArchives({ orgId }: { orgId: string }) {
  const organisation = useOrganisation(orgId);
  const archives = useOrgArchives(orgId);

  const newArchiveHref = `/archives/new?org=${orgId}`;

  return (
    <div className={styles.page}>
      <PageHeader
        title={organisation.data?.name ?? "…"}
        description="Archives owned by this organisation."
        actions={
          <Link href={newArchiveHref}>
            <Button variant="primary">+ New archive</Button>
          </Link>
        }
      />

      {archives.isPending ? (
        <div className={styles.grid}>
          <Skeleton height="9rem" width="100%" />
          <Skeleton height="9rem" width="100%" />
        </div>
      ) : archives.data?.length === 0 ? (
        <EmptyState
          title="No archives yet"
          description="Create an archive to get a hosted OSA server at your own subdomain — it deploys immediately."
          action={
            <Link href={newArchiveHref}>
              <Button variant="primary">+ New archive</Button>
            </Link>
          }
        />
      ) : (
        <div className={styles.grid}>
          {archives.data?.map((archive) => (
            <ArchiveCard key={archive.id} archive={archive} />
          ))}
        </div>
      )}
    </div>
  );
}
