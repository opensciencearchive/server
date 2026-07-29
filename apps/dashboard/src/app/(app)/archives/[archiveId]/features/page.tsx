import type { Metadata } from "next";

import { FeaturesPanel } from "@/features/tenant-insights/pages/FeaturesPanel";

export const metadata: Metadata = { title: "Features" };

export default async function FeaturesPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <FeaturesPanel archiveId={archiveId} />;
}
