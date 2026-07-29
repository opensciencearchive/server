"use client";

import { DataTable, PageHeader, SampleDataChip, Skeleton, Stat } from "@/ui";

import { useRecordStats } from "../hooks";
import styles from "./pages.module.css";

/**
 * Features are computed per record by the archive's hooks. The names below
 * are sample data, chosen to be consistent with the seeded hooks
 * (validate-metadata, resolve-ontology) — not a live feature registry.
 */
interface DerivedFeature {
  name: string;
  producedBy: string;
  description: string;
}

const SAMPLE_FEATURES: DerivedFeature[] = [
  {
    name: "qc-metrics",
    producedBy: "validate-metadata",
    description: "Per-record quality scores from the metadata completeness check.",
  },
  {
    name: "ontology-mappings",
    producedBy: "resolve-ontology",
    description: "Tissue and cell-type terms resolved against UBERON/CL.",
  },
  {
    name: "assay-summary",
    producedBy: "validate-metadata",
    description: "Normalised assay descriptors extracted at deposition time.",
  },
];

export function FeaturesPanel({ archiveId }: { archiveId: string }) {
  const stats = useRecordStats(archiveId);

  return (
    <div className={styles.page}>
      <PageHeader
        title="Features"
        description="Derived features are computed per record by the archive's hooks as depositions land."
        actions={<SampleDataChip />}
      />

      {stats.isPending ? (
        <Skeleton height="5rem" width="100%" />
      ) : stats.data ? (
        <div className={[styles.stats, styles.statsWide].join(" ")}>
          <Stat
            label="Derived features per record"
            value={stats.data.data.derivedFeaturesPerRecord}
          />
          <Stat
            label="Published records"
            value={stats.data.data.publishedRecords.toLocaleString("en-GB")}
            note="each carrying the features below"
          />
        </div>
      ) : null}

      <DataTable<DerivedFeature>
        columns={[
          {
            key: "name",
            header: "Feature",
            render: (f) => <span className="mono">{f.name}</span>,
          },
          {
            key: "producedBy",
            header: "Produced by",
            render: (f) => <span className="mono">{f.producedBy}</span>,
          },
          {
            key: "description",
            header: "Description",
            render: (f) => f.description,
          },
        ]}
        rows={SAMPLE_FEATURES}
        rowKey={(f) => f.name}
      />

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          These feature names are sample data consistent with the archive&apos;s
          seeded hooks. The live feature set is read from the OSA instance once
          the tenant API connection ships.
        </p>
      </div>
    </div>
  );
}
