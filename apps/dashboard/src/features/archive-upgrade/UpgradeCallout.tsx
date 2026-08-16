"use client";

import { useState } from "react";

import type { Archive } from "@/domain/archive";
import { isDeployBlocked } from "@/domain/archive";
import { eligibleUpgradeTargets } from "@/domain/osa-version";
import { Badge, Button } from "@/ui";

import { useOsaVersions } from "./useOsaVersions";
import { UpgradeDialog } from "./UpgradeDialog";
import styles from "./UpgradeCallout.module.css";

/**
 * Overview entry point for upgrades (#222): a single row shown only when a
 * newer supported version exists and the archive can act on it. Up to date,
 * registry unreachable, or deployment in flight all render nothing — the
 * settings page owns those states.
 */
export function UpgradeCallout({ archive }: { archive: Archive }) {
  const versions = useOsaVersions();
  const [dialogOpen, setDialogOpen] = useState(false);

  const targets = versions.data
    ? eligibleUpgradeTargets(versions.data, archive.osaVersionPin)
    : [];
  const newest = targets[0];
  if (!newest || isDeployBlocked(archive.status)) return null;

  return (
    <div className={styles.callout}>
      <Badge tone="info" withDot>
        Update available
      </Badge>
      <span className={styles.text}>
        OSA <span className="mono">{newest.version}</span> is out — this
        archive runs <span className="mono">{archive.osaVersionPin}</span>.
      </span>
      <Button size="sm" variant="primary" onClick={() => setDialogOpen(true)}>
        Upgrade…
      </Button>
      <UpgradeDialog
        archive={archive}
        targets={targets}
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
      />
    </div>
  );
}
