import type { Metadata } from "next";

import { IngestionsPanel } from "@/features/tenant-insights/pages/IngestionsPanel";

export const metadata: Metadata = { title: "Ingestions" };

export default async function IngestionsPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <IngestionsPanel archiveId={archiveId} />;
}
