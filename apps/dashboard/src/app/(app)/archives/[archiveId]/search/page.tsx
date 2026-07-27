import type { Metadata } from "next";

import { SearchPanel } from "@/features/tenant-insights/pages/SearchPanel";

export const metadata: Metadata = { title: "Search" };

export default async function SearchPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <SearchPanel archiveId={archiveId} />;
}
