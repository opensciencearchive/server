import type { Metadata } from "next";

import { ObservabilityPanel } from "@/features/tenant-insights/pages/ObservabilityPanel";

export const metadata: Metadata = { title: "Observability" };

export default async function ObservabilityPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <ObservabilityPanel archiveId={archiveId} />;
}
