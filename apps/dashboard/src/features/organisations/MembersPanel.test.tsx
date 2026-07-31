import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { MembersPanel } from "./MembersPanel";

describe("MembersPanel", () => {
  it("renders the organisation's real roster with roles", async () => {
    renderWithProviders(<MembersPanel orgId="org_7f3k2mq9x1" />);

    const table = await screen.findByRole("table");
    expect(
      within(table).getByText("p.marsh@example.ac.uk"),
    ).toBeInTheDocument();
    expect(within(table).getByText("owner")).toBeInTheDocument();
    expect(screen.queryByText(/sample data/i)).not.toBeInTheDocument();
  });

  it("says management is not here yet rather than showing dead controls", async () => {
    renderWithProviders(<MembersPanel orgId="org_7f3k2mq9x1" />);

    expect(
      await screen.findByText(/invites, role changes and removals/i),
    ).toBeInTheDocument();
  });
});
