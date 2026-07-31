import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";

import { ArchiveShell } from "./ArchiveShell";

describe("ArchiveShell", () => {
  it("platform: shows the org breadcrumb and the Builds nav item", async () => {
    renderWithProviders(
      <ArchiveShell archiveId="arch_a1p1n3c11m">
        <div>body</div>
      </ArchiveShell>,
    );

    expect(await screen.findByText("Summit Lab")).toBeInTheDocument();
    // Accessible name may include a count chip once builds load ("Builds 14").
    expect(screen.getByRole("link", { name: /Builds/ })).toBeInTheDocument();
  });

  it("self-host: collapses to the single archive — no Builds, org, or Visit", async () => {
    renderWithProviders(
      <ArchiveShell archiveId="local">
        <div>body</div>
      </ArchiveShell>,
      { services: { ...makeTestServices(), isPlatform: false } },
    );

    // The single local archive, project nav present…
    expect(await screen.findByText("Local archive")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Overview" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Hooks" })).toBeInTheDocument();
    // …but no fleet/cloud affordances.
    expect(
      screen.queryByRole("link", { name: /Builds/ }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Summit Lab")).not.toBeInTheDocument();
    expect(screen.queryByText(/visit archive/i)).not.toBeInTheDocument();
  });

  it("platform: shows a centered not-found notice for an unknown archive", async () => {
    // The mock rejects an unknown id with a 404 ApiError, like production.
    renderWithProviders(
      <ArchiveShell archiveId="arch_does_not_exist">
        <div>panel content</div>
      </ArchiveShell>,
    );

    expect(await screen.findByText("Archive not found")).toBeInTheDocument();
    // Neither the sidebar chrome nor the child panels mount.
    expect(
      screen.queryByRole("link", { name: "Overview" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("panel content")).not.toBeInTheDocument();
  });
});
