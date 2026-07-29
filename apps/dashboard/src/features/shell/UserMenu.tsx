"use client";

import { userInitials } from "@/domain/user";

import { useSession } from "../auth/useSession";
import { useSignOut } from "../auth/useSignOut";
import styles from "./UserMenu.module.css";

export function UserMenu() {
  const { data: session } = useSession();
  const signOut = useSignOut();

  const initials = session ? userInitials(session.user.email) : "…";

  return (
    <details className={styles.menu}>
      <summary className={styles.avatar} aria-label="Account menu">
        {initials}
      </summary>
      <div className={styles.dropdown}>
        {session && <p className={styles.email}>{session.user.email}</p>}
        <button
          type="button"
          className={styles.signOut}
          onClick={() => signOut.mutate()}
          disabled={signOut.isPending}
        >
          {signOut.isPending ? "Signing out…" : "Sign out"}
        </button>
      </div>
    </details>
  );
}
