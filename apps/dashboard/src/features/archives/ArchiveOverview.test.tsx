import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";

import { ArchiveOverview } from "./ArchiveOverview";

/** Services shaped like a self-hosted build (no cloud control plane). */
function selfHostServices() {
  return { ...makeTestServices(), isPlatform: false };
}

describe("ArchiveOverview", () => {
  it("renders the hero with domain, Running, and a Redeploy control for a running archive", async () => {
    renderWithProviders(<ArchiveOverview archiveId="arch_a1p1n3c11m" />);

    expect(await screen.findByText("Alpine climate network")).toBeInTheDocument();
    expect(screen.getByText("alpine-climate.amacr.in")).toBeInTheDocument();
    expect(await screen.findByText("Running")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /redeploy/i }),
    ).toBeInTheDocument();
  });

  it("renders the meta strip with organisation, region and sign-in", async () => {
    renderWithProviders(<ArchiveOverview archiveId="arch_a1p1n3c11m" />);

    expect(await screen.findByText("Summit Lab")).toBeInTheDocument();
    expect(screen.getByText("eu-west-1")).toBeInTheDocument();
    expect(screen.getByText(/ORCID · 2 administrators/)).toBeInTheDocument();
  });

  it("renders the provisioning timeline and close-tab note for a deploying archive", async () => {
    renderWithProviders(<ArchiveOverview archiveId="arch_d0pp13rswp" />);

    expect(await screen.findByText("Doppler radar sweeps")).toBeInTheDocument();
    expect(await screen.findByText("Accepted")).toBeInTheDocument();
    expect(
      screen.getByText("Provisioning infrastructure"),
    ).toBeInTheDocument();
    expect(await screen.findByText(/you can close this tab/i)).toBeInTheDocument();
  });

  it("renders the verbatim error message and a Redeploy button for an error archive", async () => {
    renderWithProviders(<ArchiveOverview archiveId="arch_so1lmo1s7r" />);

    expect(
      await screen.findByText(
        "provisioning failed: PersistentVolumeClaim pending after 600s — storage class 'gp3' quota exceeded in eu-west-1",
      ),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: /redeploy/i }),
    ).toBeInTheDocument();
  });

  it("renders the sample-data sections including the 12,481 record stat", async () => {
    renderWithProviders(<ArchiveOverview archiveId="arch_a1p1n3c11m" />);

    expect((await screen.findAllByText("12,481")).length).toBeGreaterThan(0);
    expect(screen.getByText("Next steps")).toBeInTheDocument();
    // The platform build's tenant sections carry a sample-data affordance.
    expect(screen.getAllByText("Sample data").length).toBeGreaterThan(0);
  });

  describe("self-host (IS_PLATFORM=false)", () => {
    it("renders the synthetic local archive without cloud deployment/meta", async () => {
      renderWithProviders(<ArchiveOverview archiveId="local" />, {
        services: selfHostServices(),
      });

      // The synthetic local archive, not a cloud archive record.
      expect(await screen.findByText("Local archive")).toBeInTheDocument();
      // No cloud deployment panel, no fleet meta strip.
      expect(
        screen.queryByRole("button", { name: /redeploy/i }),
      ).not.toBeInTheDocument();
      expect(screen.queryByText("Organisation")).not.toBeInTheDocument();
      expect(screen.queryByText("Region")).not.toBeInTheDocument();
    });

    it("still renders the tenant sections (real data lands in M2c)", async () => {
      renderWithProviders(<ArchiveOverview archiveId="local" />, {
        services: selfHostServices(),
      });

      expect(await screen.findByText("Next steps")).toBeInTheDocument();
      expect(screen.getByText("What's in here")).toBeInTheDocument();
    });
  });
});
