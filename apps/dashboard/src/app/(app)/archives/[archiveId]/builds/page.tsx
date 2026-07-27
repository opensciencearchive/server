import type { Metadata } from "next";

import { BuildsList } from "@/features/builds/BuildsList";

export const metadata: Metadata = { title: "Builds" };

export default async function BuildsPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <BuildsList archiveId={archiveId} />;
}
