"use client";

import { Badge, Card, CopyButton, PageHeader, SampleDataChip, Skeleton } from "@/ui";
import { useServices } from "@/api/services";
import { icons } from "@/features/shell/icons";

import { useTenantAuthConfig } from "../hooks";
import styles from "./pages.module.css";

export function AuthenticationPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real auth config; platform's is sample data (chip + note).
  const isSample = useServices().isPlatform;
  const authConfig = useTenantAuthConfig(archiveId);
  const admins = authConfig.data?.adminOrcidIds ?? [];

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.authentication}
        title="Archive sign-in"
        description="How researchers sign in to this archive to deposit and manage records."
      />

      <Card className={styles.card}>
        <dl className={styles.rows}>
          <div className={styles.row}>
            <dt>Provider</dt>
            <dd>
              <Badge outline>ORCID</Badge>
            </dd>
          </div>
          <div className={styles.row}>
            <dt>Client ID {isSample && <SampleDataChip />}</dt>
            <dd>
              {authConfig.isPending ? (
                <Skeleton height="1.5rem" width="16rem" />
              ) : authConfig.data ? (
                <span className="mono">
                  {authConfig.data.clientId || "Not configured"}
                </span>
              ) : null}
            </dd>
          </div>
          <div className={styles.row}>
            <dt>Client secret</dt>
            <dd>
              <span className={styles.cardMeta}>Sealed — never displayed</span>
            </dd>
          </div>
        </dl>
      </Card>

      <h2 className={styles.sectionTitle}>Administrators</h2>
      <Card className={styles.card}>
        {authConfig.isPending ? (
          <Skeleton height="4rem" width="100%" />
        ) : admins.length > 0 ? (
          <ul className={styles.adminList}>
            {admins.map((orcid) => (
              <li key={orcid} className={styles.adminRow}>
                <span className="mono">{orcid}</span>
                <CopyButton value={orcid} label="Copy" size="sm" />
              </li>
            ))}
          </ul>
        ) : (
          <p className={styles.cardDesc}>
            No administrators are configured for this archive.
          </p>
        )}
      </Card>

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            This sign-in configuration is sample data. Self-hosted archives show
            their real ORCID client id and configured administrators.
          </p>
        </div>
      )}
    </div>
  );
}
