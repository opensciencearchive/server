"use client";

import Link from "next/link";

import { DataTable, EmptyState, PageHeader, SampleDataChip, Skeleton } from "@/ui";
import { useServices } from "@/api/services";
import type { TenantHook } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { useTenantHooks } from "../hooks";
import styles from "./pages.module.css";

export function HooksPanel({ archiveId }: { archiveId: string }) {
  // Self-host reads real hooks from the archive; platform has no tenant path,
  // so its list is sample data (chip + note).
  const isSample = useServices().tenantDataIsSample;
  const hooks = useTenantHooks(archiveId);
  const list = hooks.data ?? [];
  const base = `/archives/${archiveId}`;

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.hooks}
        title="Hooks"
        description="Hooks run against every deposition to validate, enrich and transform records."
        actions={isSample ? <SampleDataChip /> : undefined}
      />

      {hooks.isPending ? (
        <Skeleton height="8rem" width="100%" />
      ) : list.length > 0 ? (
        <DataTable<TenantHook>
          columns={[
            {
              key: "name",
              header: "Hook",
              render: (h) => <span className="mono">{h.name}</span>,
            },
            {
              key: "description",
              header: "Description",
              render: (h) => h.description,
            },
            {
              key: "liveVersion",
              header: "Live version",
              render: (h) =>
                isSample ? (
                  <Link
                    className={styles.buildLink}
                    href={`${base}/builds/${h.liveVersion}`}
                  >
                    {h.liveVersion}
                  </Link>
                ) : (
                  <span className="mono">{h.liveVersion}</span>
                ),
            },
            {
              key: "lastRun",
              header: "Last run",
              align: "right",
              render: (h) =>
                h.lastRunAt
                  ? h.lastRunAt.toLocaleString("en-GB", {
                      day: "numeric",
                      month: "short",
                      hour: "2-digit",
                      minute: "2-digit",
                    })
                  : "—",
            },
          ]}
          rows={list}
          rowKey={(h) => h.name}
        />
      ) : (
        <EmptyState
          icon={icons.hooks}
          title="No hooks registered"
          description="Hooks appear here once a convention registers one to validate or enrich records."
        />
      )}

      {isSample && (
        <div className={styles.note}>
          <span className={styles.noteLabel}>Note</span>
          <p>
            This hook list is sample data. Live hooks and run times are read from
            the archive&apos;s OSA instance when the tenant API connection ships.
          </p>
        </div>
      )}
    </div>
  );
}
