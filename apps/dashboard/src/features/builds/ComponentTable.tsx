"use client";

import { Badge, CopyButton, DataTable } from "@/ui";
import type { ComponentBuild } from "@/domain/build";

import { componentStatusTone } from "./status";
import styles from "./ComponentTable.module.css";

/** Shorten a full digest to `sha256:9f2c4a1e…d8f1` for display. */
function truncateDigest(digest: string): string {
  const [algo, hex] = digest.split(":");
  if (!hex || hex.length <= 12) return digest;
  return `${algo}:${hex.slice(0, 8)}…${hex.slice(-4)}`;
}

function ComponentDetail({
  component,
  siblingFailed,
}: {
  component: ComponentBuild;
  siblingFailed: boolean;
}) {
  const { status } = component;
  switch (status.kind) {
    case "succeeded":
      return status.digest ? (
        <span className={styles.digest} title={status.imageRef}>
          <span className="mono">{truncateDigest(status.digest)}</span>
          <CopyButton value={status.digest} label="Copy" size="sm" />
        </span>
      ) : (
        <span className="mono" title={status.imageRef}>
          {status.imageRef}
        </span>
      );
    case "failed":
      // The verbatim error is shown beneath the name; keep this cell terse.
      return <span className={styles.error}>build step failed</span>;
    case "cancelled":
      return siblingFailed ? (
        <span className={styles.muted}>consequence, not a cause</span>
      ) : (
        <span className={styles.muted}>—</span>
      );
    default:
      return <span className={styles.muted}>—</span>;
  }
}

export function ComponentTable({
  components,
}: {
  components: ComponentBuild[];
}) {
  const siblingFailed = components.some((c) => c.status.kind === "failed");

  return (
    <DataTable<ComponentBuild>
      columns={[
        {
          key: "name",
          header: "Component",
          render: (c) => (
            <div className={styles.nameCell}>
              <span className={styles.name}>{c.name}</span>
              {c.status.kind === "failed" && (
                <span className={styles.errorInline}>
                  {c.status.errorMessage}
                </span>
              )}
              {c.status.kind === "cancelled" && siblingFailed && (
                <span className={styles.mutedInline}>
                  cancelled — a sibling component failed
                </span>
              )}
            </div>
          ),
        },
        {
          key: "kind",
          header: "Kind",
          render: (c) => <span className="mono">{c.kind}</span>,
        },
        {
          key: "status",
          header: "Status",
          render: (c) => (
            <Badge tone={componentStatusTone(c.status)} withDot>
              {c.status.kind}
            </Badge>
          ),
        },
        {
          key: "image",
          header: "Image · digest",
          render: (c) => (
            <ComponentDetail component={c} siblingFailed={siblingFailed} />
          ),
        },
      ]}
      rows={components}
      rowKey={(c) => `${c.kind}/${c.name}`}
      rowTone={(c) => (c.status.kind === "failed" ? "danger" : undefined)}
    />
  );
}
