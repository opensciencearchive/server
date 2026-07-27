"use client";

import Link from "next/link";
import { useState } from "react";

import type { Archive } from "@/domain/archive";
import type { Organisation } from "@/domain/organisation";
import {
  Badge,
  Button,
  Card,
  CardFooter,
  EmptyState,
  PageHeader,
  Skeleton,
  StatusDot,
} from "@/ui";

import { useArchives } from "../archives/useArchives";
import { archiveStatusTone } from "../archives/status";
import { CreateOrgDialog } from "./CreateOrgDialog";
import { useOrganisations } from "./useOrganisations";
import styles from "./OrganisationsHome.module.css";

function orgInitials(name: string): string {
  const words = name.split(/\s+/).filter(Boolean);
  return (
    words
      .slice(0, 2)
      .map((word) => word[0])
      .join("")
      .toUpperCase() || "?"
  );
}

function rollup(archives: Archive[]): Array<{ kind: Archive["status"]["kind"]; count: number }> {
  const counts = new Map<Archive["status"]["kind"], number>();
  for (const archive of archives) {
    counts.set(archive.status.kind, (counts.get(archive.status.kind) ?? 0) + 1);
  }
  return [...counts.entries()].map(([kind, count]) => ({ kind, count }));
}

function OrgCard({
  organisation,
  archives,
}: {
  organisation: Organisation;
  archives: Archive[];
}) {
  const deploying = archives.filter((a) => a.status.kind === "deploying").length;
  const subtitleParts = [
    `${archives.length} archive${archives.length === 1 ? "" : "s"}`,
    organisation.name === "Personal"
      ? "created on sign-in"
      : deploying > 0
        ? `${deploying} deploying`
        : null,
  ].filter(Boolean);

  return (
    <Link href={`/organisations/${organisation.id}`} className={styles.cardLink}>
      <Card className={styles.card}>
        <div className={styles.cardBody}>
          <div className={styles.cardTop}>
            <span className={styles.avatar}>{orgInitials(organisation.name)}</span>
            {organisation.role && <Badge outline>{organisation.role}</Badge>}
          </div>
          <h4 className={styles.cardName}>{organisation.name}</h4>
          <p className={styles.cardSubtitle}>{subtitleParts.join(" · ")}</p>
        </div>
        {archives.length > 0 && (
          <CardFooter>
            {rollup(archives).map(({ kind, count }) => (
              <StatusDot
                key={kind}
                tone={archiveStatusTone(kind)}
                label={`${count} ${kind}`}
                pulse={kind === "deploying"}
              />
            ))}
          </CardFooter>
        )}
      </Card>
    </Link>
  );
}

export function OrganisationsHome() {
  const organisations = useOrganisations();
  const archives = useArchives();
  const [creating, setCreating] = useState(false);

  const archivesByOrg = new Map<string, Archive[]>();
  for (const archive of archives.data ?? []) {
    const list = archivesByOrg.get(archive.organisationId) ?? [];
    list.push(archive);
    archivesByOrg.set(archive.organisationId, list);
  }

  return (
    <div className={styles.page}>
      <PageHeader
        title="Organisations"
        description="Archives live inside an organisation. Your role there decides what you can do."
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            + New organisation
          </Button>
        }
      />

      {organisations.isPending ? (
        <div className={styles.grid}>
          <Skeleton height="10rem" width="100%" />
          <Skeleton height="10rem" width="100%" />
          <Skeleton height="10rem" width="100%" />
        </div>
      ) : organisations.data?.length === 0 ? (
        <EmptyState
          title="No organisations yet"
          description="Your Personal organisation is created on first sign-in. Create one to share archives with your group."
          action={
            <Button variant="primary" onClick={() => setCreating(true)}>
              + New organisation
            </Button>
          }
        />
      ) : (
        <div className={styles.grid}>
          {organisations.data?.map((organisation) => (
            <OrgCard
              key={organisation.id}
              organisation={organisation}
              archives={archivesByOrg.get(organisation.id) ?? []}
            />
          ))}
        </div>
      )}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          Membership is read-only in v1 — invites and role changes are handled
          by Amacrin support.
        </p>
      </div>

      <CreateOrgDialog open={creating} onClose={() => setCreating(false)} />
    </div>
  );
}
