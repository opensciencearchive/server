"use client";

import {
  DataTable,
  EmptyState,
  PageHeader,
  SampleDataChip,
  Skeleton,
  Stat,
} from "@/ui";
import { useServices } from "@/api/services";
import type { FeatureTable } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { useTenantFeatureTables } from "../hooks";
import styles from "./pages.module.css";

export function FeaturesPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real feature tables; platform's are sample data (chip).
  const isSample = useServices().tenantDataIsSample;
  const features = useTenantFeatureTables(archiveId);
  const tables = features.data ?? [];
  const totalRows = tables.reduce((sum, t) => sum + t.rows, 0);

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.features}
        title="Features"
        description="Derived features are computed per record by the archive's hooks, one table per hook."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {!features.isPending && tables.length > 0 && (
        <div className={[styles.stats, styles.statsWide].join(" ")}>
          <Stat label="Feature tables" value={tables.length} />
          <Stat
            label="Total feature rows"
            value={totalRows.toLocaleString("en-GB")}
          />
        </div>
      )}

      {features.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : tables.length > 0 ? (
        <DataTable<FeatureTable>
          columns={[
            {
              key: "name",
              header: "Feature",
              render: (f) => <span className="mono">{f.name}</span>,
            },
            {
              key: "schema",
              header: "Schema",
              render: (f) => <span className="mono">{f.schema}</span>,
            },
            {
              key: "columns",
              header: "Columns",
              align: "right",
              render: (f) => f.columns,
            },
            {
              key: "rows",
              header: "Rows",
              align: "right",
              render: (f) => f.rows.toLocaleString("en-GB"),
            },
          ]}
          rows={tables}
          rowKey={(f) => `${f.schema}/${f.name}`}
        />
      ) : (
        <EmptyState
          icon={icons.features}
          title="No feature tables yet"
          description="Feature tables appear here once a convention registers a hook that produces them."
        />
      )}

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            These feature tables are sample data. Self-hosted archives show their
            real registered feature tables here.
          </p>
        </div>
      )}
    </div>
  );
}
