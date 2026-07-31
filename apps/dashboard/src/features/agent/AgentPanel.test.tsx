import { screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import { server } from "@/mocks/server";
import { renderWithProviders } from "@/test/render";

import { AgentPanel } from "./AgentPanel";

const SURFACE = {
  skill: "# Alpine climate network\n\nAgent grounding doc.",
  discovery: {
    node: {
      name: "Alpine",
      domain: "alpine.example.org",
      description: "d",
      osa_version: "0.0.7",
    },
    skill_url: "https://alpine.example.org/SKILL.md",
    reference_base: "https://alpine.example.org/api/v1/data",
    data_url: "https://alpine.example.org/api/v1/data",
    openapi_url: "https://alpine.example.org/api/v1/openapi.json",
  },
};

describe("AgentPanel", () => {
  it("renders the skill sheet and the MCP connector when the surface loads", async () => {
    server.use(http.get("/api/agent", () => HttpResponse.json(SURFACE)));
    renderWithProviders(<AgentPanel />);

    // SKILL.md markdown rendered (the `# Alpine climate network` heading).
    expect(await screen.findByText("Alpine climate network")).toBeInTheDocument();
    // Summary cards show the bare skill + MCP URLs (scheme stripped).
    expect(screen.getByText("alpine.example.org/SKILL.md")).toBeInTheDocument();
    expect(screen.getByText("alpine.example.org/mcp")).toBeInTheDocument();
    // Connect column: a known tool from the fixed MCP catalogue.
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
