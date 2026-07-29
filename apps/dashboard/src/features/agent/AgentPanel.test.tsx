import { screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import { server } from "@/mocks/server";
import { renderWithProviders } from "@/test/render";

import { AgentPanel } from "./AgentPanel";

const SURFACE = {
  skill: "# Cortex atlas\n\nAgent grounding doc.",
  discovery: {
    node: {
      name: "Cortex",
      domain: "cortex.example.org",
      description: "d",
      osa_version: "0.0.7",
    },
    skill_url: "https://cortex.example.org/SKILL.md",
    reference_base: "https://cortex.example.org/api/v1/data",
    data_url: "https://cortex.example.org/api/v1/data",
    openapi_url: "https://cortex.example.org/api/v1/openapi.json",
  },
};

describe("AgentPanel", () => {
  it("renders the skill sheet and the MCP connector when the surface loads", async () => {
    server.use(http.get("/api/agent", () => HttpResponse.json(SURFACE)));
    renderWithProviders(<AgentPanel />);

    // SKILL.md markdown rendered (the `# Cortex atlas` heading).
    expect(await screen.findByText("Cortex atlas")).toBeInTheDocument();
    // MCP connector: derived /mcp url + a known tool.
    expect(
      screen.getByText("https://cortex.example.org/mcp"),
    ).toBeInTheDocument();
    expect(screen.getByText("list_datasets")).toBeInTheDocument();
  });

  it("shows an empty state when the surface can't be reached", async () => {
    server.use(
      http.get("/api/agent", () => new HttpResponse(null, { status: 502 })),
    );
    renderWithProviders(<AgentPanel />);

    expect(
      await screen.findByText("Agent surface unavailable"),
    ).toBeInTheDocument();
  });
});
