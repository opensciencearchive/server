"use client";

import { useQuery } from "@tanstack/react-query";

import { usePlatformServices } from "@/api/services";
import {
  Badge,
  DataTable,
  PageHeader,
  SampleDataChip,
  Skeleton,
} from "@/ui";
import type { OrgMember } from "@/domain/tenant";

import { orgKeys } from "./keys";
import styles from "./MembersPanel.module.css";

export function MembersPanel({ orgId }: { orgId: string }) {
  const { amacrin } = usePlatformServices();
  const members = useQuery({
    queryKey: orgKeys.members(orgId),
    // @mock — no members endpoint exists; Mocked<T> sample data.
    queryFn: () => amacrin.listOrgMembers(orgId),
  });

  return (
    <div className={styles.page}>
      <PageHeader
        title="Members"
        description="Who can see and manage this organisation's archives."
        actions={<SampleDataChip />}
      />

      {members.isPending ? (
        <Skeleton height="10rem" width="100%" />
      ) : members.data ? (
        <DataTable<OrgMember>
          columns={[
            { key: "name", header: "Member", render: (m) => m.name },
            {
              key: "email",
              header: "Email",
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
              render: (m) =>
                m.joinedAt.toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "short",
                  year: "numeric",
                }),
            },
          ]}
          rows={members.data.data}
          rowKey={(m) => m.email}
        />
      ) : null}

      <div className={styles.note}>
        <span className={styles.noteLabel}>Note</span>
        <p>
          Membership is read-only in v1 — invites, removals and role changes
          are handled by Amacrin support.
        </p>
      </div>
    </div>
  );
}
