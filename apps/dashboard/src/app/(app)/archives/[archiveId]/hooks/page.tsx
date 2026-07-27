import type { Metadata } from "next";

import { HooksPanel } from "@/features/tenant-insights/pages/HooksPanel";

export const metadata: Metadata = { title: "Hooks" };

export default async function HooksPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <HooksPanel archiveId={archiveId} />;
}
