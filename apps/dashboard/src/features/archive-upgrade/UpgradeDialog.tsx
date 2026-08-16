"use client";

import { useCallback, useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { ApiError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import type { OsaVersion } from "@/domain/osa-version";
import { releaseCompareUrl } from "@/domain/osa-version";
import { Button, Checkbox, Dialog, Field, Select, Skeleton } from "@/ui";

import { useReleaseNotes } from "./useReleaseNotes";
import { useUpgradeArchive } from "./useUpgradeArchive";
import styles from "./UpgradeDialog.module.css";

/**
 * The deliberate half of #222: from → to, what changed, an explicit
 * forward-only acknowledgment, then hand off to /deploying/{id}.
 */
export function UpgradeDialog({
  archive,
  targets,
  open,
  onClose,
}: {
  archive: Archive;
  /** Eligible targets, newest first — guaranteed non-empty by the caller. */
  targets: OsaVersion[];
  open: boolean;
  onClose: () => void;
}) {
  const newest = targets[0]!;
  const [toVersion, setToVersion] = useState(newest.version);
  const [acknowledged, setAcknowledged] = useState(false);
  const upgrade = useUpgradeArchive(archive.id);
  const target = targets.find((t) => t.version === toVersion) ?? newest;

  const notes = useReleaseNotes(archive.osaVersionPin, toVersion, open);

  const upgradeReset = upgrade.reset;
  const close = useCallback(() => {
    setAcknowledged(false);
    upgradeReset();
    onClose();
  }, [upgradeReset, onClose]);

  const submit = () => {
    if (!acknowledged || upgrade.isPending) return;
    upgrade.mutate(toVersion);
  };

  const errorMessage =
    upgrade.error instanceof ApiError
      ? upgrade.error.code === "invalid_state"
        ? "A deployment is already in progress."
        : upgrade.error.message
      : upgrade.error
        ? "Something went wrong — try again."
        : undefined;

  return (
    <Dialog
      open={open}
      onClose={close}
      title={`Upgrade ${archive.name} to ${toVersion}?`}
    >
      <div className={styles.body}>
        <div className={styles.versions}>
          <span className="mono">{archive.osaVersionPin}</span>
          <span aria-hidden>→</span>
          {targets.length > 1 ? (
            <Field label="Upgrade to">
              {({ id }) => (
                <Select
                  id={id}
                  value={toVersion}
                  onChange={(e) => setToVersion(e.target.value)}
                >
                  {targets.map((t) => (
                    <option key={t.version} value={t.version}>
                      {t.version}
                      {t.isDefault ? " (recommended)" : ""}
                    </option>
                  ))}
                </Select>
              )}
            </Field>
          ) : (
            <span className="mono">{toVersion}</span>
          )}
        </div>

        <ReleaseNotes
          notes={notes}
          fallbackUrl={target.notesUrl}
          compareUrl={releaseCompareUrl(archive.osaVersionPin, toVersion)}
        />

        <p className={styles.warning}>
          The archive restarts and runs schema migrations — expect a brief
          pause in availability while it comes back up.
        </p>

        <Checkbox
          label="I understand upgrades are forward-only and can't be rolled back."
          checked={acknowledged}
          onChange={(e) => setAcknowledged(e.target.checked)}
        />

        {errorMessage && (
          <p className={styles.error} role="alert">
            {errorMessage}
          </p>
        )}
      </div>

      <div className={styles.actions}>
        <Button variant="ghost" onClick={close}>
          Not now
        </Button>
        <Button
          variant="primary"
          onClick={submit}
          disabled={!acknowledged || upgrade.isPending}
        >
          {upgrade.isPending ? "Starting upgrade…" : `Upgrade to ${toVersion}`}
        </Button>
      </div>
    </Dialog>
  );
}

function ReleaseNotes({
  notes,
  fallbackUrl,
  compareUrl,
}: {
  notes: ReturnType<typeof useReleaseNotes>;
  fallbackUrl: string | null;
  compareUrl: string;
}) {
  if (notes.isPending) {
    return <Skeleton height="6rem" width="100%" />;
  }
  if (notes.isError || !notes.data) {
    // GitHub unreachable — degrade to the registry's link-out.
    return (
      <p className={styles.notesFallback}>
        {fallbackUrl ? (
          <a href={fallbackUrl} target="_blank" rel="noreferrer">
            Read the release notes on GitHub
          </a>
        ) : (
          "Release notes are unavailable right now."
        )}{" "}
        ·{" "}
        <a href={compareUrl} target="_blank" rel="noreferrer">
          Full comparison
        </a>
      </p>
    );
  }
  return (
    <div className={styles.notes}>
      {notes.data.map((release) => (
        <section key={release.version} className={styles.release}>
          <h3 className={styles.releaseTitle}>
            <span className="mono">{release.version}</span>
            {release.name && release.name !== release.version
              ? ` — ${release.name}`
              : null}
          </h3>
          <div className={styles.prose}>
            <Markdown remarkPlugins={[remarkGfm]}>{release.body}</Markdown>
          </div>
        </section>
      ))}
      <p className={styles.notesFallback}>
        <a href={compareUrl} target="_blank" rel="noreferrer">
          Full comparison on GitHub
        </a>
      </p>
    </div>
  );
}
