"use client";

import { Card, PageHeader, SampleDataChip, Skeleton, StatusDot } from "@/ui";
import type { Tone } from "@/ui";
import { useServices } from "@/api/services";
import { icons } from "@/features/shell/icons";

import { useObservability } from "../hooks";
import styles from "./pages.module.css";

function componentTone(status: "healthy" | "degraded"): Tone {
  return status === "healthy" ? "success" : "warning";
}

export function ObservabilityPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real readiness; platform's is sample data (chip + note).
  const isSample = useServices().isPlatform;
  const snapshot = useObservability(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.observability}
        title="Observability"
        description="Live health of the archive's runtime — its database, workers and hook runner."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {snapshot.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : snapshot.data ? (
        <>
          <div className={styles.overallStatus}>
            <StatusDot
              tone={snapshot.data.status === "ready" ? "success" : "warning"}
              label={snapshot.data.status}
              mono={false}
            />
          </div>

          <div className={styles.grid}>
            {snapshot.data.components.map((component) => (
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

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            These health signals are sample data. Self-hosted archives show their
            live component health here.
          </p>
        </div>
      )}
    </div>
  );
}
