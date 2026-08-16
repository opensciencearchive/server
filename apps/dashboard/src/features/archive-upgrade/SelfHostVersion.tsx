"use client";

import { compareOsaVersions } from "@/domain/osa-version";
import { useNodeOverview } from "@/features/archives/useNodeOverview";
import { Badge, CopyButton } from "@/ui";

import styles from "./SelfHostVersion.module.css";

/**
 * Self-host upgrade awareness (#222, user-decided scope): show the node's
 * version and, when a newer release exists, the exact CLI command — the
 * dashboard cannot perform the upgrade itself (server and dashboard share
 * one OSA_IMAGE_VERSION; upgrading the server restarts this dashboard too).
 */
export function SelfHostVersion() {
  const node = useNodeOverview();

  if (!node.data || !node.data.osaVersion) return null;

  const current = node.data.osaVersion;
  const latest = node.data.latestOsaVersion;
  const behind =
    latest !== null && compareOsaVersions(latest, current) > 0;

  return (
    <div className={styles.wrapper}>
      <div className={styles.row}>
        <span className={styles.label}>OSA version</span>
        <span className="mono">{current}</span>
        {behind ? (
          <Badge tone="info" withDot>
            Update available → {latest}
          </Badge>
        ) : latest !== null ? (
          <Badge tone="success">Up to date</Badge>
        ) : null}
      </div>
      {behind && (
        <div className={styles.hint}>
          <p className={styles.hintText}>
            Run this where the archive is hosted — it restarts the server and
            this dashboard:
          </p>
          <div className={styles.command}>
            <code className="mono">osa start --osa-version {latest}</code>
            <CopyButton value={`osa start --osa-version ${latest}`} />
          </div>
        </div>
      )}
    </div>
  );
}
