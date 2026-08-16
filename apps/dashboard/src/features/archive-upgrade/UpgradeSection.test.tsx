import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { makeTestServices, renderWithProviders } from "@/test/render";
import { buildArchive } from "@/test/factories";
import { mockRouter } from "@/test/router-mock";

import { UpgradeSection } from "./UpgradeSection";

// Seeded mock registry: pin v0.0.9; v0.0.10 + v0.0.11 supported (default),
// v0.0.8 deprecated. Seeded archives pin v0.0.9.

describe("UpgradeSection", () => {
  it("shows the pin and an update-available badge when newer versions exist", async () => {
    const services = makeTestServices();
    renderWithProviders(
      <UpgradeSection archive={buildArchive({ id: "arch_sky1mag3ry" })} />,
      { services },
    );
    expect(await screen.findByText("v0.0.9")).toBeInTheDocument();
    expect(
      await screen.findByText("Update available → v0.0.11"),
    ).toBeInTheDocument();
  });

  it("shows up-to-date when the pin is the newest supported version", async () => {
    const services = makeTestServices();
    renderWithProviders(
      <UpgradeSection
        archive={buildArchive({
          id: "arch_sky1mag3ry",
          osaVersionPin: "v0.0.11",
        })}
      />,
      { services },
    );
    expect(await screen.findByText("Up to date")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Upgrade…" })).toBeDisabled();
  });

  it("blocks the button with a reason while a deployment is in flight", async () => {
    const services = makeTestServices();
    renderWithProviders(
      <UpgradeSection
        archive={buildArchive({
          id: "arch_d0pp13rswp",
          status: { kind: "deploying" },
        })}
      />,
      { services },
    );
    const button = await screen.findByRole("button", { name: "Upgrade…" });
    await waitFor(() => expect(button).toBeDisabled());
    expect(
      screen.getByText("A deployment is already in progress."),
    ).toBeInTheDocument();
  });

  it("shows the registry as unavailable, not up to date, when it errors", async () => {
    const services = makeTestServices();
    vi.spyOn(services.amacrin, "listOsaVersions").mockRejectedValue(
      new Error("registry down"),
    );
    renderWithProviders(
      <UpgradeSection archive={buildArchive({ id: "arch_sky1mag3ry" })} />,
      { services },
    );
    expect(
      await screen.findByText("Version check unavailable"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Up to date")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Upgrade…" })).toBeDisabled();
  });

  it("warns when the pinned version is deprecated", async () => {
    const services = makeTestServices();
    renderWithProviders(
      <UpgradeSection
        archive={buildArchive({
          id: "arch_sky1mag3ry",
          osaVersionPin: "v0.0.8",
        })}
      />,
      { services },
    );
    expect(
      await screen.findByText(/is deprecated — upgrading is recommended/),
    ).toBeInTheDocument();
  });

  it("opens the dialog and performs an upgrade end to end", async () => {
    const services = makeTestServices();
    const upgradeSpy = vi.spyOn(services.amacrin, "upgradeArchive");
    const user = userEvent.setup();
    renderWithProviders(
      <UpgradeSection archive={buildArchive({ id: "arch_sky1mag3ry" })} />,
      { services },
    );

    await user.click(await screen.findByRole("button", { name: "Upgrade…" }));
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toBeInTheDocument();

    // The confirm button is gated on the forward-only acknowledgment.
    const confirm = screen.getByRole("button", { name: "Upgrade to v0.0.11" });
    expect(confirm).toBeDisabled();
    await user.click(
      screen.getByLabelText(
        "I understand upgrades are forward-only and can't be rolled back.",
      ),
    );
    expect(confirm).toBeEnabled();

    await user.click(confirm);
    await waitFor(() =>
      expect(upgradeSpy).toHaveBeenCalledWith("arch_sky1mag3ry", "v0.0.11"),
    );
    await waitFor(() =>
      expect(mockRouter.push).toHaveBeenCalledWith(
        "/deploying/arch_sky1mag3ry",
      ),
    );
  });

  it("lets the user pick an intermediate version when several are eligible", async () => {
    const services = makeTestServices();
    const upgradeSpy = vi.spyOn(services.amacrin, "upgradeArchive");
    const user = userEvent.setup();
    renderWithProviders(
      <UpgradeSection archive={buildArchive({ id: "arch_sky1mag3ry" })} />,
      { services },
    );

    await user.click(await screen.findByRole("button", { name: "Upgrade…" }));
    await user.selectOptions(
      await screen.findByLabelText("Upgrade to"),
      "v0.0.10",
    );
    await user.click(
      screen.getByLabelText(
        "I understand upgrades are forward-only and can't be rolled back.",
      ),
    );
    await user.click(screen.getByRole("button", { name: "Upgrade to v0.0.10" }));
    await waitFor(() =>
      expect(upgradeSpy).toHaveBeenCalledWith("arch_sky1mag3ry", "v0.0.10"),
    );
  });

  it("surfaces an in-flight conflict as a friendly message", async () => {
    const services = makeTestServices();
    // Seeded deploying archive: the mock rejects with invalid_state.
    const user = userEvent.setup();
    renderWithProviders(
      <UpgradeSection
        archive={buildArchive({
          // Running per the props (so the button is enabled), but the mock's
          // stored state is deploying — mirrors a stale client racing the
          // cloud, which answers 422 invalid_state.
          id: "arch_d0pp13rswp",
          status: { kind: "running" },
        })}
      />,
      { services },
    );

    await user.click(await screen.findByRole("button", { name: "Upgrade…" }));
    await user.click(
      screen.getByLabelText(
        "I understand upgrades are forward-only and can't be rolled back.",
      ),
    );
    await user.click(screen.getByRole("button", { name: "Upgrade to v0.0.11" }));
    expect(
      await screen.findByText("A deployment is already in progress."),
    ).toBeInTheDocument();
  });
});
