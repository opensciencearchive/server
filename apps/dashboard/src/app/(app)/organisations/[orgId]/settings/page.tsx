import type { Metadata } from "next";

import { OrgSettingsPanel } from "@/features/organisations/OrgSettingsPanel";

export const metadata: Metadata = { title: "Organisation settings" };

export default async function OrgSettingsPage({
  params,
}: {
  params: Promise<{ orgId: string }>;
}) {
  const { orgId } = await params;
  return <OrgSettingsPanel orgId={orgId} />;
}
