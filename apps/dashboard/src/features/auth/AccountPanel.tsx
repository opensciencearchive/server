"use client";

import { Badge, Card, PageHeader, Skeleton } from "@/ui";

import { useSession } from "./useSession";
import { useSignOut } from "./useSignOut";
import styles from "./AccountPanel.module.css";

export function AccountPanel() {
  const { data: session } = useSession();
  const signOut = useSignOut();

  return (
    <div className={styles.page}>
      <PageHeader
        title="Account"
        description="Your platform identity. Sign-in is via Google; your email is contact-only."
      />
      <Card className={styles.card}>
        {session ? (
          <dl className={styles.rows}>
            <div className={styles.row}>
              <dt>Email</dt>
              <dd className="mono">{session.user.email}</dd>
            </div>
            <div className={styles.row}>
              <dt>User ID</dt>
              <dd className="mono">{session.user.id}</dd>
            </div>
            <div className={styles.row}>
              <dt>Member since</dt>
              <dd>
                {session.user.createdAt.toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "long",
                  year: "numeric",
                })}
              </dd>
            </div>
            <div className={styles.row}>
              <dt>Organisations</dt>
              <dd className={styles.orgs}>
                {session.organisations.map((org) => (
                  <span key={org.id} className={styles.org}>
                    {org.name}
                    {org.role && <Badge outline>{org.role}</Badge>}
                  </span>
                ))}
              </dd>
            </div>
          </dl>
        ) : (
          <Skeleton height="8rem" width="100%" />
        )}
      </Card>
      <button
        type="button"
        className={styles.signOut}
        onClick={() => signOut.mutate()}
        disabled={signOut.isPending}
      >
        {signOut.isPending ? "Signing out…" : "Sign out"}
      </button>
    </div>
  );
}
