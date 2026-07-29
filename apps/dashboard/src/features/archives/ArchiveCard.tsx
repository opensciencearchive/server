"use client";

import Link from "next/link";

import type { Archive } from "@/domain/archive";
import { Badge, Card } from "@/ui";

import { archiveStatusTone } from "./status";
import styles from "./ArchiveCard.module.css";

export function ArchiveCard({ archive }: { archive: Archive }) {
  return (
    <Link href={`/archives/${archive.id}`} className={styles.link}>
      <Card className={styles.card}>
        <div className={styles.top}>
          <h4 className={styles.name}>{archive.name}</h4>
          <Badge
            tone={archiveStatusTone(archive.status.kind)}
            withDot
          >
            {archive.status.kind}
          </Badge>
        </div>
        <p className={styles.domain}>{archive.domain}</p>
        <p className={styles.meta}>
          {archive.deploymentConfig?.region ?? "eu-west-1"}
          {" · created "}
          {archive.createdAt.toLocaleDateString("en-GB", {
            day: "numeric",
            month: "short",
            year: "numeric",
          })}
        </p>
        {archive.status.kind === "error" && (
          <p className={styles.error}>{archive.status.message}</p>
        )}
      </Card>
    </Link>
  );
}
