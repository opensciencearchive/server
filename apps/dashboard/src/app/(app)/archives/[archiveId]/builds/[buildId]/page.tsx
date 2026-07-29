import type { Metadata } from "next";

import { BuildDetail } from "@/features/builds/BuildDetail";

export const metadata: Metadata = { title: "Build" };

export default async function BuildDetailPage({
  params,
}: {
  params: Promise<{ archiveId: string; buildId: string }>;
}) {
  const { buildId } = await params;
  return <BuildDetail buildId={buildId} />;
}
