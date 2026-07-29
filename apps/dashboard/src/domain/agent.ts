/**
 * The archive's agent-grounding surface — its `SKILL.md` sheet and the pointers
 * from root discovery (`GET /`). Rendered on the Agents page; the MCP tool/app
 * catalogue is a fixed property of the server version (see features/agent).
 */
export interface AgentNode {
  name: string;
  domain: string;
  description: string;
  osaVersion: string;
}

export interface AgentSurface {
  /** Raw `SKILL.md` markdown, rendered prettily. */
  skillMarkdown: string;
  node: AgentNode;
  /** Absolute `/mcp` connector URL (derived from the node's public origin). */
  mcpUrl: string;
  skillUrl: string;
  dataUrl: string;
}
