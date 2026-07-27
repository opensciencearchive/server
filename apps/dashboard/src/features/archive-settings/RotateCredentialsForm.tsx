"use client";

import Link from "next/link";
import { useState } from "react";

import type { ArchiveAuthInput } from "@/api/amacrin/service";
import { ApiError } from "@/api/http/errors";
import type { Archive } from "@/domain/archive";
import { isDeployBlocked } from "@/domain/archive";
import { useRedeploy } from "@/features/deployments/useRedeploy";
import { Button, Card, Field, Input, TagInput } from "@/ui";

import { blockedReason } from "./blocked";
import styles from "./RotateCredentialsForm.module.css";

export function RotateCredentialsForm({ archive }: { archive: Archive }) {
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");
  const [admins, setAdmins] = useState<string[]>([]);
  const [rotated, setRotated] = useState(false);

  const redeploy = useRedeploy(archive.id);
  const blocked = isDeployBlocked(archive.status);
  const reason = blockedReason(archive.status);

  const canSubmit =
    !blocked &&
    !redeploy.isPending &&
    clientId.trim() !== "" &&
    clientSecret.trim() !== "";

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (!canSubmit) return;
    const auth: ArchiveAuthInput = {
      orcid: {
        clientId: clientId.trim(),
        clientSecret,
      },
      adminOrcidIds: admins,
    };
    redeploy.mutate(
      { auth },
      {
        onSuccess: () => {
          setClientId("");
          setClientSecret("");
          setAdmins([]);
          setRotated(true);
        },
      },
    );
  };

  const errorMessage =
    redeploy.error instanceof ApiError
      ? redeploy.error.message
      : redeploy.error
        ? "Something went wrong — try again."
        : undefined;

  return (
    <Card className={styles.card}>
      <div className={styles.heading}>
        <h2 className={styles.title}>Rotate archive sign-in credentials</h2>
        <p className={styles.description}>
          Depositors sign in to this archive with ORCID. Supplying a new client
          and administrators <strong>replaces both</strong> and redeploys the
          archive. The current secret is sealed on write and never shown — enter
          the new one in full.
        </p>
      </div>

      <form onSubmit={submit} className={styles.form}>
        <div className={styles.grid}>
          <Field label="ORCID client ID">
            {({ id }) => (
              <Input
                id={id}
                mono
                value={clientId}
                onChange={(e) => {
                  setClientId(e.target.value);
                  setRotated(false);
                }}
                placeholder="APP-…"
                disabled={blocked}
              />
            )}
          </Field>
          <Field label="ORCID client secret">
            {({ id }) => (
              <Input
                id={id}
                type="password"
                mono
                value={clientSecret}
                onChange={(e) => {
                  setClientSecret(e.target.value);
                  setRotated(false);
                }}
                placeholder="Write-only"
                autoComplete="off"
                disabled={blocked}
              />
            )}
          </Field>
        </div>

        <Field label="Administrator ORCID iDs">
          {({ id, describedBy }) => (
            <TagInput
              id={id}
              describedBy={describedBy}
              label="Administrator ORCID iDs"
              values={admins}
              onChange={(next) => {
                setAdmins(next);
                setRotated(false);
              }}
              placeholder="Add an iD…"
            />
          )}
        </Field>

        {errorMessage && (
          <p className={styles.error} role="alert">
            {errorMessage}
          </p>
        )}

        <div className={styles.actions}>
          <Button
            variant="primary"
            type="submit"
            disabled={!canSubmit}
            title={blocked ? reason : undefined}
          >
            {redeploy.isPending ? "Rotating…" : "Rotate and redeploy"}
          </Button>
          {blocked ? (
            <span className={styles.note}>{reason}</span>
          ) : (
            <span className={styles.note}>
              Brief downtime while the archive restarts.
            </span>
          )}
        </div>

        {rotated && (
          <p className={styles.confirmation} role="status">
            A redeploy started with the new credentials.{" "}
            <Link href={`/archives/${archive.id}`}>
              Follow it on the overview
            </Link>
            .
          </p>
        )}
      </form>
    </Card>
  );
}
