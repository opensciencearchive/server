"use client";

import { useCallback, useState } from "react";

import { ApiError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import { Button, Dialog, Field, Input } from "@/ui";

import { useDestroyArchive } from "./useDestroyArchive";
import styles from "./DestroyConfirmDialog.module.css";

export function DestroyConfirmDialog({
  archive,
  open,
  onClose,
}: {
  archive: Archive;
  open: boolean;
  onClose: () => void;
}) {
  const [confirmation, setConfirmation] = useState("");
  const destroy = useDestroyArchive(archive.id, archive.organisationId);

  const destroyReset = destroy.reset;
  const close = useCallback(() => {
    setConfirmation("");
    destroyReset();
    onClose();
  }, [destroyReset, onClose]);

  const matches = confirmation === archive.slug;

  const submit = () => {
    if (!matches || destroy.isPending) return;
    destroy.mutate();
  };

  const errorMessage =
    destroy.error instanceof ApiError
      ? destroy.error.message
      : destroy.error
        ? "Something went wrong — try again."
        : undefined;

  return (
    <Dialog open={open} onClose={close} title={`Destroy ${archive.name}?`}>
      <div className={styles.body}>
        <p className={styles.lead}>This cannot be undone.</p>
        <ul className={styles.consequences}>
          <li>Compute, database and images are torn down.</li>
          <li>Sealed ORCID credentials are deleted.</li>
          <li>In-flight builds are cancelled.</li>
          <li>
            <span className="mono">{archive.slug}</span> is released — anyone can
            claim it.
          </li>
        </ul>

        <Field
          label={`Type ${archive.slug} to confirm`}
          error={errorMessage}
        >
          {({ id, describedBy, invalid }) => (
            <Input
              id={id}
              aria-describedby={describedBy}
              invalid={invalid}
              mono
              value={confirmation}
              onChange={(e) => setConfirmation(e.target.value)}
              autoComplete="off"
            />
          )}
        </Field>
      </div>

      <div className={styles.actions}>
        <Button variant="ghost" onClick={close}>
          Keep archive
        </Button>
        <Button
          variant="danger"
          onClick={submit}
          disabled={!matches || destroy.isPending}
        >
          {destroy.isPending ? "Destroying…" : "Destroy permanently"}
        </Button>
      </div>
    </Dialog>
  );
}
