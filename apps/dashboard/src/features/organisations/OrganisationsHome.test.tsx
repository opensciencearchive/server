import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { renderWithProviders } from "@/test/render";

import { OrganisationsHome } from "./OrganisationsHome";

describe("OrganisationsHome", () => {
  it("renders an org card per organisation with role and status rollups", async () => {
    renderWithProviders(<OrganisationsHome />);

    expect(await screen.findByText("Marsh Lab")).toBeInTheDocument();
    expect(screen.getByText("Personal")).toBeInTheDocument();
    expect(screen.getByText("Cryo-EM Consortium")).toBeInTheDocument();

    // Marsh Lab (seeded): 1 running + 1 error
    const marshCard = screen.getByText("Marsh Lab").closest("a")!;
    expect(marshCard).toHaveTextContent("2 archives");
    expect(marshCard).toHaveTextContent("1 running");
    expect(marshCard).toHaveTextContent("1 error");
    expect(marshCard).toHaveTextContent("admin");
  });

  it("states that membership is read-only in v1", async () => {
    renderWithProviders(<OrganisationsHome />);
    expect(
      await screen.findByText(/membership is read-only in v1/i),
    ).toBeInTheDocument();
  });
});
