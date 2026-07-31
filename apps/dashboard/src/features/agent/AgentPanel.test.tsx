import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MockOSAService } from "@/api/osa/mock";
import { makeTestServices, renderWithProviders } from "@/test/render";

import { AgentPanel } from "./AgentPanel";

const ARCHIVE = "arch_a1p1n3c11m";

describe("AgentPanel", () => {
  it("renders the skill sheet and the MCP connector when the surface loads", async () => {
    renderWithProviders(<AgentPanel archiveId={ARCHIVE} />);

    // SKILL.md markdown is rendered (its top-level heading).
    expect(
      await screen.findByRole("heading", { name: /alpine climate network/i }),
    ).toBeInTheDocument();
    // Summary cards show the bare skill + MCP URLs (scheme stripped).
    expect(screen.getByText(/\/SKILL\.md$/)).toBeInTheDocument();
    expect(screen.getByText(/\/mcp$/)).toBeInTheDocument();
    // Connect column: a known tool from the fixed MCP catalogue.
    expect(screen.getByText("list_datasets")).toBeInTheDocument();
  });

  it("shows an empty state when the archive can't be reached", async () => {
    const osa = new MockOSAService();
    osa.getAgentSurface = () => Promise.reject(new Error("unreachable"));

    renderWithProviders(<AgentPanel archiveId={ARCHIVE} />, {
      services: makeTestServices({ osa }),
    });

    expect(
      await screen.findByText("Agent surface unavailable"),
    ).toBeInTheDocument();
  });
});
