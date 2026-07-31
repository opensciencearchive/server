"use client";

import { useState } from "react";

import {
  DataTable,
  EmptyState,
  PageHeader,
  SampleDataChip,
  Select,
  Skeleton,
  Stat,
} from "@/ui";
import { useServices } from "@/api/services";
import type { TenantRecord } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { useRecordStats, useTenantRecords, useTenantSchemas } from "../hooks";
import styles from "./pages.module.css";

function formatStorage(bytes: number): string {
  const tb = bytes / 1e12;
  if (tb >= 1) return `${tb.toFixed(1)} TB`;
  const gb = bytes / 1e9;
  if (gb >= 1) return `${Math.round(gb)} GB`;
  return `${Math.round(bytes / 1e6)} MB`;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/** A compact "key: value · key: value" preview of a record's first few fields. */
function fieldsPreview(fields: Record<string, unknown>): string {
  const parts = Object.entries(fields)
    .slice(0, 3)
    .map(([key, value]) => `${key}: ${formatValue(value)}`);
  return parts.length > 0 ? parts.join("  ·  ") : "—";
}

export function RecordsPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real records; platform's are sample data (chip + note).
  const isSample = useServices().isPlatform;
  const stats = useRecordStats(archiveId);
  const schemas = useTenantSchemas(archiveId);

  const [selected, setSelected] = useState<string | undefined>(undefined);
  const schemaList = schemas.data ?? [];
  const activeSchema = selected ?? schemaList[0];
  const records = useTenantRecords(archiveId, activeSchema);

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.records}
        title="Records"
        description="The published records held in this archive."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {stats.isPending ? (
        <Skeleton height="5rem" width="100%" />
      ) : stats.data ? (
        <div className={styles.stats}>
          <Stat
            label="Published records"
            value={stats.data.publishedRecords.toLocaleString("en-GB")}
          />
          <Stat
            label="This month"
            value={`+${stats.data.recordsThisMonth.toLocaleString("en-GB")}`}
            note="new depositions"
            noteTone="success"
          />
          <Stat label="Storage" value={formatStorage(stats.data.storageBytes)} />
        </div>
      ) : null}

      {schemaList.length > 1 && activeSchema && (
        <div className={styles.schemaPicker}>
          <label htmlFor="records-schema">Schema</label>
          <Select
            id="records-schema"
            value={activeSchema}
            onChange={(e) => setSelected(e.target.value)}
          >
            {schemaList.map((schema) => (
              <option key={schema} value={schema}>
                {schema}
              </option>
            ))}
          </Select>
        </div>
      )}

      {records.isPending || schemas.isPending ? (
        <Skeleton height="12rem" width="100%" />
      ) : records.data && records.data.length > 0 ? (
        <DataTable<TenantRecord>
          columns={[
            {
              key: "id",
              header: "Record",
              render: (r) => <span className={styles.srn}>{r.id}</span>,
            },
            {
              key: "fields",
              header: "Metadata",
              render: (r) => (
                <span className={styles.fieldsPreview}>{fieldsPreview(r.fields)}</span>
              ),
            },
            {
              key: "created",
              header: "Published",
              align: "right",
              render: (r) =>
                r.createdAt.toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "short",
                  year: "numeric",
                }),
            },
          ]}
          rows={records.data}
          rowKey={(r) => r.id}
        />
      ) : (
        <EmptyState
          icon={icons.records}
          title="No records yet"
          description="Published records for this schema appear here once the archive holds data."
        />
      )}

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            These records are sample data. Self-hosted archives show their real
            published records here.
          </p>
        </div>
      )}
    </div>
  );
}
