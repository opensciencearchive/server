import type { Metadata } from "next";

import { ValidationPanel } from "@/features/tenant-insights/pages/ValidationPanel";

export const metadata: Metadata = { title: "Validation" };

export default async function ValidationPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <ValidationPanel archiveId={archiveId} />;
}
