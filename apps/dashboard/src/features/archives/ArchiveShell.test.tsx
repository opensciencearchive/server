import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";

import { ArchiveShell } from "./ArchiveShell";

describe("ArchiveShell", () => {
  it("platform: shows the org breadcrumb and the Builds nav item", async () => {
    renderWithProviders(
      <ArchiveShell archiveId="arch_c0r73xa71a">
        <div>body</div>
      </ArchiveShell>,
    );

    expect(await screen.findByText("Marsh Lab")).toBeInTheDocument();
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
    expect(screen.queryByText("Marsh Lab")).not.toBeInTheDocument();
    expect(screen.queryByText(/visit archive/i)).not.toBeInTheDocument();
  });
});
