"use client";

import { useCallback, useState } from "react";

import type { Archive } from "@/domain/archive";
import { isDeployBlocked } from "@/domain/archive";
import { Button, Card } from "@/ui";

import { blockedReason } from "./blocked";
import { DestroyConfirmDialog } from "./DestroyConfirmDialog";
import styles from "./DestroySection.module.css";

const OWNER_ONLY_REASON =
  "Only an organisation Owner can destroy this archive.";

export function DestroySection({
  archive,
  isOwner,
}: {
  archive: Archive;
  isOwner: boolean;
}) {
  const [open, setOpen] = useState(false);
  const onClose = useCallback(() => setOpen(false), []);

  const inFlight = isDeployBlocked(archive.status);
  const disabled = !isOwner || inFlight;
  const reason = !isOwner
    ? OWNER_ONLY_REASON
    : inFlight
      ? blockedReason(archive.status)
      : undefined;

  return (
    <Card className={styles.card}>
      <div className={styles.inner}>
        <div className={styles.heading}>
          <h2 className={styles.title}>Destroy archive</h2>
          <p className={styles.description}>
            Destroy is the only way to remove an archive. Everything goes:
            compute, database, images, and sealed credentials are torn down;
            in-flight builds are cancelled; and the subdomain{" "}
            <span className="mono">{archive.slug}</span> is released for anyone
            to claim.
          </p>
        </div>
        <Button
          variant="danger"
          className={styles.trigger}
          disabled={disabled}
          title={reason}
          onClick={() => setOpen(true)}
        >
          Destroy archive…
        </Button>
      </div>

      {reason && (
        <div className={styles.reasonBar}>
          <span>{reason}</span>
        </div>
      )}

      <DestroyConfirmDialog archive={archive} open={open} onClose={onClose} />
    </Card>
  );
}
