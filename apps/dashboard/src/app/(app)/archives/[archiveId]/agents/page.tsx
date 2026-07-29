import type { Metadata } from "next";

import { AgentPanel } from "@/features/agent/AgentPanel";

export const metadata: Metadata = { title: "Agents" };

export default function AgentsPage() {
  return <AgentPanel />;
}
