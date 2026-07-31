"use client";

import { Card, EmptyState, PageHeader, SampleDataChip, Skeleton } from "@/ui";
import { useServices } from "@/api/services";
import { icons } from "@/features/shell/icons";

import { useTenantIngesters } from "../hooks";
import styles from "./pages.module.css";

function shortDigest(digest: string): string {
  const bare = digest.replace(/^sha256:/, "");
  return bare.length > 12 ? bare.slice(0, 12) : bare;
}

export function IngestersPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real ingesters; platform's are sample data (chip + note).
  const isSample = useServices().isPlatform;
  const ingesters = useTenantIngesters(archiveId);
  const list = ingesters.data ?? [];

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.ingesters}
        title="Ingesters"
        description="Ingesters import external datasets and normalise them into the archive's convention."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {ingesters.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : list.length > 0 ? (
        <div className={styles.grid}>
          {list.map((ingester) => (
            <Card key={ingester.name} className={styles.gridCard}>
              <span className={styles.cardName}>{ingester.name}</span>
              <p className={styles.cardDesc}>{ingester.description}</p>
              <div className={styles.chips}>
                <span className={styles.chip}>{ingester.schema}</span>
                {ingester.schedule && (
                  <span className={styles.chip}>cron {ingester.schedule}</span>
                )}
              </div>
              <span className={styles.cardMeta}>
                <span className="mono">
                  {ingester.image}@{shortDigest(ingester.digest)}
                </span>
              </span>
            </Card>
          ))}
        </div>
      ) : (
        <EmptyState
          icon={icons.ingesters}
          title="No ingesters configured"
          description="Ingesters appear here when a convention declares a source to import from."
        />
      )}

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            This ingester list is sample data. Self-hosted archives show their
            real configured ingesters here.
          </p>
        </div>
      )}
    </div>
  );
}
