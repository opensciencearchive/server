import type { Metadata } from "next";

import { AuthenticationPanel } from "@/features/tenant-insights/pages/AuthenticationPanel";

export const metadata: Metadata = { title: "Authentication" };

export default async function AuthenticationPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <AuthenticationPanel archiveId={archiveId} />;
}
