import type { Metadata } from "next";

import { AgentPanel } from "@/features/agent/AgentPanel";

export const metadata: Metadata = { title: "Agents" };

export default async function AgentsPage({
  params,
}: {
  params: Promise<{ archiveId: string }>;
}) {
  const { archiveId } = await params;
  return <AgentPanel archiveId={archiveId} />;
}
