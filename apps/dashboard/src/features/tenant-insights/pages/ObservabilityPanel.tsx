"use client";

import { Card, PageHeader, SampleDataChip, Skeleton, StatusDot } from "@/ui";
import type { Tone } from "@/ui";

import { useObservability } from "../hooks";
import styles from "./pages.module.css";

function componentTone(status: "healthy" | "degraded"): Tone {
  return status === "healthy" ? "success" : "warning";
}

export function ObservabilityPanel({ archiveId }: { archiveId: string }) {
  const snapshot = useObservability(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Observability"
        description="Live health of the archive's runtime — its API, database, workers and hook runner."
        actions={<SampleDataChip />}
      />

      {snapshot.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : snapshot.data ? (
        <>
          <div className={styles.overallStatus}>
            <StatusDot
              tone={snapshot.data.data.status === "ready" ? "success" : "warning"}
              label={snapshot.data.data.status}
              mono={false}
            />
          </div>

          <div className={styles.grid}>
            {snapshot.data.data.components.map((component) => (
              <Card key={component.name} className={styles.gridCard}>
                <StatusDot
                  tone={componentTone(component.status)}
                  label={component.name}
                />
                <span className={styles.cardMeta}>{component.detail}</span>
              </Card>
            ))}
          </div>
        </>
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          These health signals are sample data. Live metrics stream from the
          archive&apos;s OSA instance when the tenant API connection ships.
        </p>
      </div>
    </div>
  );
}
