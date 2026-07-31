"use client";

import Link from "next/link";
import { useState } from "react";

import type { Archive } from "@/domain/archive";
import { isDeployBlocked } from "@/domain/archive";
import { roleAtLeast } from "@/domain/organisation";
import { useServices } from "@/api/services";
import { useArchive } from "@/features/archives/useArchives";
import { useSession } from "@/features/auth/useSession";
import { useRedeploy } from "@/features/deployments/useRedeploy";
import { Button, Card, PageHeader, Skeleton } from "@/ui";

import { blockedReason } from "./blocked";
import { DestroySection } from "./DestroySection";
import { RotateCredentialsForm } from "./RotateCredentialsForm";
import styles from "./SettingsPanel.module.css";

export function SettingsPanel({ archiveId }: { archiveId: string }) {
  // Redeploy / rotate / destroy are cloud-lifecycle actions (they call
  // usePlatformServices, which throws in self-host). Self-host manages the node
  // out-of-band via the CLI, so it gets a read-only card instead.
  const isPlatform = useServices().isPlatform;
  const archive = useArchive(archiveId);
  const session = useSession();

  return (
    <div className={styles.page}>
      <PageHeader
        title="Settings"
        description={
          archive.data ? (
            <>
              {archive.data.name} ·{" "}
              <span className="mono">{archive.data.domain}</span>
            </>
          ) : undefined
        }
      />

      {archive.isPending || !archive.data ? (
        <Skeleton height="12rem" width="100%" />
      ) : isPlatform ? (
        <SettingsSections
          archive={archive.data}
          isOwner={isOwnerOf(archive.data, session.data?.organisations ?? [])}
        />
      ) : (
        <SelfHostSettings />
      )}
    </div>
  );
}

function SelfHostSettings() {
  return (
    <Card className={styles.card}>
      <div className={styles.heading}>
        <h2 className={styles.title}>Manage this node</h2>
        <p className={styles.description}>
          This archive runs from your own deployment. Manage it with the{" "}
          <span className="mono">osa</span> CLI and your compose / Kubernetes
          stack — redeploys, credential rotation and teardown happen there, not
          from the dashboard.
        </p>
      </div>
    </Card>
  );
}

function isOwnerOf(
  archive: Archive,
  organisations: { id: string; role: "member" | "admin" | "owner" | null }[],
): boolean {
  const org = organisations.find((o) => o.id === archive.organisationId);
  return org?.role != null && roleAtLeast(org.role, "owner");
}

function SettingsSections({
  archive,
  isOwner,
}: {
  archive: Archive;
  isOwner: boolean;
}) {
  return (
    <div className={styles.sections}>
      <RotateCredentialsForm archive={archive} />
      <RedeploySection archive={archive} />
      <DestroySection archive={archive} isOwner={isOwner} />
    </div>
  );
}

function RedeploySection({ archive }: { archive: Archive }) {
  const redeploy = useRedeploy(archive.id);
  const [done, setDone] = useState(false);

  const blocked = isDeployBlocked(archive.status);
  const reason = blockedReason(archive.status);

  const onClick = () => {
    redeploy.mutate(undefined, { onSuccess: () => setDone(true) });
  };

  return (
    <Card className={styles.card}>
      <div className={styles.heading}>
        <h2 className={styles.title}>Redeploy</h2>
        <p className={styles.description}>
          Reuses the stored administrators and sealed credentials. Nothing to
          enter.
        </p>
      </div>
      <div className={styles.actions}>
        <Button
          className={styles.selfStart}
          disabled={blocked || redeploy.isPending}
          title={blocked ? reason : undefined}
          onClick={onClick}
        >
          {redeploy.isPending ? "Redeploying…" : "Redeploy archive"}
        </Button>
        {blocked && <span className={styles.note}>{reason}</span>}
      </div>
      {done && (
        <p className={styles.confirmation} role="status">
          A redeploy started.{" "}
          <Link href={`/archives/${archive.id}`}>
            Follow it on the overview
          </Link>
          .
        </p>
      )}
    </Card>
  );
}
