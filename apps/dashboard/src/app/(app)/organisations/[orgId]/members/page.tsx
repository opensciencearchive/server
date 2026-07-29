import type { Metadata } from "next";

import { MembersPanel } from "@/features/organisations/MembersPanel";

export const metadata: Metadata = { title: "Members" };

export default async function MembersPage({
  params,
}: {
  params: Promise<{ orgId: string }>;
}) {
  const { orgId } = await params;
  return <MembersPanel orgId={orgId} />;
}
