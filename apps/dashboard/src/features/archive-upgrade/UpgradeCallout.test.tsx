import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";
import { buildArchive } from "@/test/factories";

import { UpgradeCallout } from "./UpgradeCallout";

describe("UpgradeCallout", () => {
  it("offers the upgrade and opens the shared dialog", async () => {
    const services = makeTestServices();
    const upgradeSpy = vi.spyOn(services.amacrin, "upgradeArchive");
    const user = userEvent.setup();
    renderWithProviders(
      <UpgradeCallout archive={buildArchive({ id: "arch_sky1mag3ry" })} />,
      { services },
    );

    expect(await screen.findByText("Update available")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Upgrade…" }));
    await user.click(
      await screen.findByLabelText(
        "I understand upgrades are forward-only and can't be rolled back.",
      ),
    );
    await user.click(screen.getByRole("button", { name: "Upgrade to v0.0.11" }));
    await waitFor(() =>
      expect(upgradeSpy).toHaveBeenCalledWith("arch_sky1mag3ry", "v0.0.11"),
    );
  });

  it("renders nothing when the archive is up to date", async () => {
    const services = makeTestServices();
    const { container } = renderWithProviders(
      <UpgradeCallout
        archive={buildArchive({
          id: "arch_sky1mag3ry",
          osaVersionPin: "v0.0.11",
        })}
      />,
      { services },
    );
    await waitFor(() => expect(container).toBeEmptyDOMElement());
  });

  it("renders nothing while a deployment is in flight", async () => {
    const services = makeTestServices();
    const { container } = renderWithProviders(
      <UpgradeCallout
        archive={buildArchive({
          id: "arch_d0pp13rswp",
          status: { kind: "deploying" },
        })}
      />,
      { services },
    );
    await waitFor(() => expect(container).toBeEmptyDOMElement());
  });
});
