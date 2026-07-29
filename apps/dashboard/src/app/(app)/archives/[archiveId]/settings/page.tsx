import type { Metadata } from "next";

import { SettingsPanel } from "@/features/archive-settings/SettingsPanel";

export const metadata: Metadata = { title: "Archive settings" };

export default async function ArchiveSettingsPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <SettingsPanel archiveId={archiveId} />;
}
