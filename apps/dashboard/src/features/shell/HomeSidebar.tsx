"use client";

import { SELF_HOST_ARCHIVE_ID } from "@/api/config";
import { useServices } from "@/api/services";

import { useSignOut } from "../auth/useSignOut";
import { CLI_REFERENCE_URL } from "./links";
import { SidebarNav } from "./SidebarNav";
import styles from "./SidebarFooter.module.css";

export function HomeSidebar() {
  const signOut = useSignOut();

  // Self-host has no org plane; the home link points at the single archive.
  const homeItem = useServices().isPlatform
    ? { label: "Organisations", href: "/", icon: "organisations" as const }
    : {
        label: "Overview",
        href: `/archives/${SELF_HOST_ARCHIVE_ID}`,
        icon: "overview" as const,
      };

  return (
    <SidebarNav
      sections={[
        {
          items: [
            homeItem,
            { label: "Account", href: "/account", icon: "account" },
          ],
        },
      ]}
      footer={
        <>
          <a
            className={styles.footerLink}
            href={CLI_REFERENCE_URL}
            target="_blank"
            rel="noreferrer"
          >
            CLI reference
          </a>
          <button
            type="button"
            className={styles.footerButton}
            onClick={() => signOut.mutate()}
            disabled={signOut.isPending}
          >
            Sign out
          </button>
        </>
      }
    />
  );
}
