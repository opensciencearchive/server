"use client";

import { Badge, Card, PageHeader, Skeleton } from "@/ui";

import { useOrganisation } from "./useOrganisations";
import styles from "./OrgSettingsPanel.module.css";

export function OrgSettingsPanel({ orgId }: { orgId: string }) {
  const organisation = useOrganisation(orgId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Organisation settings"
        description="Identity and membership for this organisation."
      />

      <Card className={styles.card}>
        {organisation.isPending ? (
          <Skeleton height="6rem" width="100%" />
        ) : organisation.data ? (
          <dl className={styles.rows}>
            <div className={styles.row}>
              <dt>Name</dt>
              <dd>{organisation.data.name}</dd>
            </div>
            <div className={styles.row}>
              <dt>Organisation ID</dt>
              <dd className="mono">{organisation.data.id}</dd>
            </div>
            <div className={styles.row}>
              <dt>Your role</dt>
              <dd>
                {organisation.data.role && (
                  <Badge outline>{organisation.data.role}</Badge>
                )}
              </dd>
            </div>
            <div className={styles.row}>
              <dt>Created</dt>
              <dd>
                {organisation.data.createdAt.toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "long",
                  year: "numeric",
                })}
              </dd>
            </div>
          </dl>
        ) : null}
      </Card>

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          Renaming or deleting an organisation isn&apos;t available yet — the
          API defers both. Contact Amacrin support if you need either.
        </p>
      </div>
    </div>
  );
}
