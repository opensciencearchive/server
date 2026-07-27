import type { Metadata } from "next";

import { RecordsPanel } from "@/features/tenant-insights/pages/RecordsPanel";

export const metadata: Metadata = { title: "Records" };

export default async function RecordsPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <RecordsPanel archiveId={archiveId} />;
}
