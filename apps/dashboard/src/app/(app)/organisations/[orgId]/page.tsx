import type { Metadata } from "next";

import { OrgArchives } from "@/features/organisations/OrgArchives";

export const metadata: Metadata = { title: "Archives" };

export default async function OrgArchivesPage({
  params,
}: {
  params: Promise<{ orgId: string }>;
}) {
  const { orgId } = await params;
  return <OrgArchives orgId={orgId} />;
}
