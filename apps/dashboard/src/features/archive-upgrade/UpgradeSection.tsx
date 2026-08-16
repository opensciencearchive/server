"use client";

import { useState } from "react";

import type { Archive } from "@/domain/archive";
import { isDeployBlocked } from "@/domain/archive";
import { eligibleUpgradeTargets } from "@/domain/osa-version";
import { useDeploymentStatus } from "@/features/deployments/useDeploymentStatus";
import { blockedReason } from "@/features/archive-settings/blocked";
import { Badge, Button, Card, Skeleton } from "@/ui";

import { useOsaVersions } from "./useOsaVersions";
import { UpgradeDialog } from "./UpgradeDialog";
import styles from "./UpgradeSection.module.css";

/**
 * The archive's OSA version: current pin, availability state, and the
 * upgrade entry point (#222). Platform mode only — self-host gets a
 * read-only version line in SelfHostSettings instead.
 */
export function UpgradeSection({ archive }: { archive: Archive }) {
  const versions = useOsaVersions();
  const latestDeployment = useDeploymentStatus(archive.id);
  const [dialogOpen, setDialogOpen] = useState(false);

  const blocked = isDeployBlocked(archive.status);
  const reason = blockedReason(archive.status);

  const pin = archive.osaVersionPin;
  const pinEntry = versions.data?.find((v) => v.version === pin);
  const targets = versions.data ? eligibleUpgradeTargets(versions.data, pin) : [];
  const newest = targets[0];

  // The pin moves when an upgrade starts; a failed deployment at the pin
  // means the upgrade did not land — and a retry deploys the PIN, never the
  // previous version. Say so instead of implying a rollback.
  const upgradeFailed =
    latestDeployment.data?.status.kind === "failed" &&
    latestDeployment.data.osaVersion !== pin &&
    !blocked;

  return (
    <Card className={styles.card}>
      <div className={styles.heading}>
        <h2 className={styles.title}>OSA version</h2>
        <div className={styles.chipRow}>
          <span className="mono">{pin}</span>
          {versions.isPending ? (
            <Skeleton height="1.25rem" width="8rem" />
          ) : newest ? (
            <Badge tone="info" withDot>
              Update available → {newest.version}
            </Badge>
          ) : (
            <Badge tone="success">Up to date</Badge>
          )}
        </div>
        {upgradeFailed && (
          <p className={styles.failedNote} role="alert">
            The upgrade to <span className="mono">{pin}</span> failed during
            deployment. Retrying deploys <span className="mono">{pin}</span> —
            upgrades don&apos;t roll back.
          </p>
        )}
        {pinEntry?.status === "deprecated" && (
          <p className={styles.deprecatedNote}>
            <span className="mono">{pin}</span> is deprecated — upgrading is
            recommended.
          </p>
        )}
        {pinEntry?.status === "withdrawn" && (
          <p className={styles.withdrawnNote} role="alert">
            <span className="mono">{pin}</span> has been withdrawn: redeploys
            of this version are refused. Upgrade required.
          </p>
        )}
      </div>

      <div className={styles.actions}>
        <Button
          variant="primary"
          disabled={blocked || !newest}
          title={blocked ? reason : !newest ? "Already on the newest supported version." : undefined}
          onClick={() => setDialogOpen(true)}
        >
          Upgrade…
        </Button>
        {blocked && <span className={styles.note}>{reason}</span>}
      </div>

      {newest && (
        <UpgradeDialog
          archive={archive}
          targets={targets}
          open={dialogOpen}
          onClose={() => setDialogOpen(false)}
        />
      )}
    </Card>
  );
}
