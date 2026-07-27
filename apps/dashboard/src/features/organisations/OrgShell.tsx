"use client";

import { AppShell } from "../shell/AppShell";
import { Breadcrumbs } from "../shell/Breadcrumbs";
import { SidebarNav } from "../shell/SidebarNav";
import { Badge } from "@/ui";

import { useOrganisation } from "./useOrganisations";

export function OrgShell({
  orgId,
  children,
}: {
  orgId: string;
  children: React.ReactNode;
}) {
  const organisation = useOrganisation(orgId);
  const base = `/organisations/${orgId}`;

  return (
    <AppShell
      breadcrumbs={
        <Breadcrumbs
          crumbs={[
            {
              label: organisation.data?.name ?? "…",
              href: base,
              badge: organisation.data?.role ? (
                <Badge outline>{organisation.data.role}</Badge>
              ) : undefined,
            },
          ]}
        />
      }
      sidebar={
        <SidebarNav
          sections={[
            {
              items: [
                { label: "Archives", href: base, icon: "archives" },
                { label: "Members", href: `${base}/members`, icon: "members" },
                {
                  label: "Organisation settings",
                  href: `${base}/settings`,
                  icon: "settings",
                },
              ],
            },
          ]}
        />
      }
    >
      {children}
    </AppShell>
  );
}
