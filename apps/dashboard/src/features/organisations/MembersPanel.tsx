"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import { Badge, DataTable, EmptyState, PageHeader, Skeleton } from "@/ui";
import type { OrgMember } from "@/domain/tenant";
import { icons } from "@/features/shell/icons";

import { orgKeys } from "./keys";
import styles from "./MembersPanel.module.css";

function formatJoined(date: Date): string {
  return date.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

export function MembersPanel({ orgId }: { orgId: string }) {
  const { amacrin } = usePlatformServices();
  const members = useQuery({
    queryKey: orgKeys.members(orgId),
    queryFn: () => amacrin.listOrgMembers(orgId),
  });

  return (
    <div className={styles.page}>
      <PageHeader
        icon={icons.members}
        title="Members"
        description="Who can see and manage this organisation's archives."
      />

      {members.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : members.data && members.data.length > 0 ? (
        <DataTable<OrgMember>
          columns={[
            {
              key: "email",
              header: "Member",
              render: (m) => <span className="mono">{m.email}</span>,
            },
            {
              key: "role",
              header: "Role",
              render: (m) => <Badge outline>{m.role}</Badge>,
            },
            {
              key: "joined",
              header: "Joined",
              align: "right",
              render: (m) => formatJoined(m.joinedAt),
            },
          ]}
          rows={members.data}
          rowKey={(m) => m.userId}
        />
      ) : (
        <EmptyState
          icon={icons.members}
          title="Roster unavailable"
          description="This organisation's members could not be read. Refresh to try again."
        />
      )}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          The roster is read-only for now — invites, role changes and removals
          are coming. Until then, Amacrin support can make those changes for
          you.
        </p>
      </div>
    </div>
  );
}
