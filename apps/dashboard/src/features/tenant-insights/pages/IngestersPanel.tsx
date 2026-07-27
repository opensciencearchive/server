"use client";

import Link from "next/link";

import { Card, PageHeader, SampleDataChip, Skeleton } from "@/ui";

import { useTenantIngesters } from "../hooks";
import styles from "./pages.module.css";

export function IngestersPanel({ archiveId }: { archiveId: string }) {
  const ingesters = useTenantIngesters(archiveId);
  const base = `/archives/${archiveId}`;

  return (
    <div className={styles.page}>
      <PageHeader
        title="Ingesters"
        description="Ingesters import external datasets and normalise them into the archive's convention."
        actions={<SampleDataChip />}
      />

      {ingesters.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : ingesters.data ? (
        <div className={styles.grid}>
          {ingesters.data.data.map((ingester) => (
            <Card key={ingester.name} className={styles.gridCard}>
              <span className={styles.cardName}>{ingester.name}</span>
              <p className={styles.cardDesc}>{ingester.description}</p>
              <div className={styles.chips}>
                {ingester.acceptedFormats.map((format) => (
                  <span key={format} className={styles.chip}>
                    {format}
                  </span>
                ))}
              </div>
              <span className={styles.cardMeta}>
                live{" "}
                <Link
                  className={styles.buildLink}
                  href={`${base}/builds/${ingester.liveVersion}`}
                >
                  {ingester.liveVersion}
                </Link>
              </span>
            </Card>
          ))}
        </div>
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          This ingester list is sample data. Live ingesters are read from the
          archive&apos;s OSA instance when the tenant API connection ships.
        </p>
      </div>
    </div>
  );
}
