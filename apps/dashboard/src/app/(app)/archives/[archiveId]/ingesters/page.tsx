import type { Metadata } from "next";

import { IngestersPanel } from "@/features/tenant-insights/pages/IngestersPanel";

export const metadata: Metadata = { title: "Ingesters" };

export default async function IngestersPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <IngestersPanel archiveId={archiveId} />;
}
