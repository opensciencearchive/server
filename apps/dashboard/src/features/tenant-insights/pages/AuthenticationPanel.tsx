"use client";

import Link from "next/link";

import { useArchive } from "@/features/archives/useArchives";
import { Badge, Card, CopyButton, PageHeader, SampleDataChip, Skeleton } from "@/ui";

import { useTenantAuthConfig } from "../hooks";
import styles from "./pages.module.css";

export function AuthenticationPanel({ archiveId }: { archiveId: string }) {
  const archive = useArchive(archiveId);
  const authConfig = useTenantAuthConfig(archiveId);
  const base = `/archives/${archiveId}`;

  return (
    <div className={styles.page}>
      <PageHeader
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
            <dt>
              Client ID <SampleDataChip />
            </dt>
            <dd>
              {authConfig.isPending ? (
                <Skeleton height="1.5rem" width="16rem" />
              ) : authConfig.data ? (
                <span className="mono">{authConfig.data.data.clientId}</span>
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
        {archive.isPending ? (
          <Skeleton height="4rem" width="100%" />
        ) : archive.data ? (
          archive.data.orcidAdmins.length > 0 ? (
            <ul className={styles.adminList}>
              {archive.data.orcidAdmins.map((orcid) => (
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
          )
        ) : null}
      </Card>

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          The administrator ORCID iDs come from this archive&apos;s configuration
          and are live. The client secret is sealed and never shown — rotate
          credentials from{" "}
          <Link href={`${base}/settings`}>Settings</Link>.
        </p>
      </div>
    </div>
  );
}
