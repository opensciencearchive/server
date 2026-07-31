import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { OrganisationsHome } from "./OrganisationsHome";

describe("OrganisationsHome", () => {
  it("renders an org card per organisation with role and status rollups", async () => {
    renderWithProviders(<OrganisationsHome />);

    expect(await screen.findByText("Summit Lab")).toBeInTheDocument();
    expect(screen.getByText("Personal")).toBeInTheDocument();
    expect(screen.getByText("Radar Array Consortium")).toBeInTheDocument();

    // Summit Lab (seeded): 1 running + 1 error
    const summitCard = screen.getByText("Summit Lab").closest("a")!;
    expect(summitCard).toHaveTextContent("2 archives");
    expect(summitCard).toHaveTextContent("1 running");
    expect(summitCard).toHaveTextContent("1 error");
    expect(summitCard).toHaveTextContent("admin");
  });

  it("states that membership is read-only in v1", async () => {
    renderWithProviders(<OrganisationsHome />);
    expect(
      await screen.findByText(/membership is read-only in v1/i),
    ).toBeInTheDocument();
  });
});
